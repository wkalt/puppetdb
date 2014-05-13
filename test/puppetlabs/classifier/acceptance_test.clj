(ns puppetlabs.classifier.acceptance-test
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh] :rename {sh blocking-sh}]
            [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [me.raynes.conch.low-level :refer [destroy proc stream-to] :rename {proc sh}]
            [schema.core :as sc]
            [schema.test]
            [puppetlabs.kitchensink.core :refer [ini-to-map mapkeys mapvals spit-ini]]
            [puppetlabs.classifier.http :refer [convert-uuids]]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [ClientNode Group]]
            [puppetlabs.classifier.storage.postgres :refer [root-group-uuid]]
            [puppetlabs.classifier.util :refer [->uuid merge-and-clean]])
  (:import [java.util.concurrent TimeoutException TimeUnit]
           java.util.regex.Pattern
           java.util.UUID))

(def test-config
  "Classifier base configuration used for tests in this namespace"
  {:webserver {:host "0.0.0.0"
               :port 1261}
   :classifier {:url-prefix "/classifier"
                :puppet-master "https://localhost:8140"}})

(defn- origin-url
  [app-config]
  (let [{{:keys [host port]} :webserver} app-config]
    (str "http://" (if (= host "0.0.0.0") "localhost" host) ":" port)))

(defn- base-url
  [app-config]
  (let [{{:keys [url-prefix]} :webserver} app-config]
    (str (origin-url app-config)
         (get-in app-config [:classifier :url-prefix]))))

(defn- block-until-ready
  [server-process]
  (let [out-lines (-> (:out server-process) io/reader line-seq)]
    (dorun (take-while #(not (re-find #"Started o.e.j.s.h.ContextHandler" %))
                         out-lines))))

(defn start!
  [config-path]
  "Initialize the database and then start a classifier server using the given
  config file, returning a conch process map describing the server instance
  process."
  (let [test-db {:subprotocol "postgresql"
                 :dbname (or (System/getenv "CLASSIFIER_DBNAME")
                              "classifier_test")
                 :user (or (System/getenv "CLASSIFIER_DBUSER")
                           "classifier_test")
                 :password (or (System/getenv "CLASSIFIER_DBPASS")
                               "classifier_test")}
        config-with-db (assoc test-config :database test-db)
        test-config-file (java.io.File/createTempFile "classifier-test-" ".conf")
        test-config-path (.getAbsolutePath test-config-file)
        _ (spit test-config-file (json/encode config-with-db))
        {initdb-stat :exit
         initdb-err :err
         initdb-out :out} (blocking-sh "lein" "run"
                                       "-b" "resources/initdb.cfg"
                                       "-c" test-config-path)]
    (when-not (= initdb-stat 0)
      (binding [*out* *err*]
        (println "Could not initialize database! initdb service says:"
                 (if-not (empty? initdb-err) initdb-err initdb-out))
        (System/exit 1)))
    (let [server-proc (sh "lein" "trampoline" "run"
                          "-b" "resources/bootstrap.cfg"
                          "-c" test-config-path)
          timeout-ms 30000
          server-blocker (future (block-until-ready server-proc))]
      ;; Block on the server starting for up to thirty seconds.
      ;; If it doesn't start within that time, exit nonzero.
      (try
        (.get server-blocker timeout-ms TimeUnit/MILLISECONDS)
        (future (stream-to server-proc :out System/err))
        (future (stream-to server-proc :err System/err))
        (catch TimeoutException e
          (future-cancel server-blocker)
          (destroy server-proc)
          (binding [*out* *err*]
            (println "Server did not start within the allotted time"
                     (str "(" timeout-ms " ms)")))
          (System/exit 2)))
      ;; The config file has already been read by the test instance, so we can
      ;; delete it.
      (.delete test-config-file)
      server-proc)))

(defn stop!
  [process-map]
  "Terminate the given process, returning its exit status."
  (let [{proc :process} process-map]
    (.destroy proc)
    (.waitFor proc)
    (.exitValue proc)))

(defn with-classifier-instance
  "Fixture that re-initializes the database using the initdb service, then spins
  up a classifier server instance in a subprocess (killing it afterwards)."
  [f]
  (let [app-process (start! test-config)]
    (f)
    (stop! app-process)))

(use-fixtures :once with-classifier-instance schema.test/validate-schemas)

(deftest ^:acceptance smoke
  (let [base-url (base-url test-config)]
    (testing "can create a new environment"
      (let [resp (http/put (str base-url "/v1/environments/test-env"))]
        (is (= 201 (:status resp)))
        (is (= {"name" "test-env"} (json/parse-string (:body resp))))))
    (testing "can delete an environment"
      (let [resp (http/delete (str base-url "/v1/environments/test-env"))]
        (is (= 204 (:status resp)))))))

(defn valid-400-resp-body?
  [body]
  (let [{:keys [kind msg details]} (json/decode body true)]
    (and kind msg details
         (= #{:submitted :schema :error}
            (-> details keys set)))))

(deftest ^:acceptance object-validation
  (let [base-url (base-url test-config)
        quiet-put #(http/put % {:throw-exceptions false})]
    (testing "schema-noncompliant objects in requests elicit a 400 response."
      (let [resp (quiet-put (str base-url "/v1/environments/production/classes/foo"))]
        (is (= 400 (:status resp)))
        (is (valid-400-resp-body? (:body resp))))
      (let [resp (quiet-put (str base-url "/v1/groups/" (UUID/randomUUID)))]
        (is (= 400 (:status resp)))
        (is (valid-400-resp-body? (:body resp)))))))

(deftest ^:acceptance group-api
  (let [base-url (base-url test-config)]

    (testing "can create a group by POSTing to the group collection endpoint"
      (let [group {:name "bargroup"
                   :environment "test"
                   :description "this group is for bars only! no foos allowed."
                   :rule ["=" "bar" "bar"]
                   :parent root-group-uuid
                   :classes {}
                   :variables {}}
            {:keys [headers status body]} (http/post (str base-url "/v1/groups")
                                                     {:follow-redirects false
                                                      :content-type :json
                                                      :body (json/encode group)})]
        (testing "and get a 303 back with the group's location"
          (is (= 303 status))
          (is (contains? headers "location")))
        (testing "and retrieve the created group from the received location"
          (let [location (get headers "location" "")
                id (re-find #"[^-]{8}-[^-]{4}-[^-]{4}-[^-]{4}-[^-]{12}" location)
                group-with-id (assoc group :id (UUID/fromString id))
                origin (origin-url test-config)
                {:keys [body status]} (http/get (str origin location))]
            (is (= 200 status))
            (is (= group-with-id (-> body (json/decode true) convert-uuids)))))))

    (let [class {:name "fooclass", :environment "test", :parameters {}}
          group {:name "foogroup"
                 :id (UUID/randomUUID)
                 :environment "test"
                 :rule ["=" "foo" "foo"]
                 :parent root-group-uuid
                 :classes {}
                 :variables {}}
          group-uri (str base-url "/v1/groups/" (:id group))]

      (testing "can create a group by PUTting to its URI"
        (http/put (str base-url "/v1/environments/" (:environment class) "/classes/" (:name class))
                  {:content-type :json, :body (json/encode class)})
        (http/put group-uri {:content-type :json, :body (json/encode group)})
        (testing "and retrieve it from the same place"
          (let [{:keys [body status]} (http/get group-uri)]
            (is (= 200 status))
            (is (= group (convert-uuids (json/decode body true)))))))

      (testing "when trying to create a group that would violate a uniqueness constraint"
        (let [conflicting {:name "foogroup"
                           :id (UUID/randomUUID)
                           :environment "test"
                           :rule ["=" "foo" "foo"]
                           :parent root-group-uuid
                           :classes {}}
              {:keys [body status]} (http/put (str base-url "/v1/groups/" (:id conflicting))
                                              {:content-type :json
                                               :body (json/encode conflicting)
                                               :throw-exceptions false})]
          (testing "a 422 Unprocessable Entity response is returned"
            (is (= status 422))
            (testing "that has an understandable error"
              (let [{:keys [kind msg details] :as error} (json/decode body true)]
                (is (= #{:kind :msg :details} (-> error keys set)))
                (is (= "uniqueness-violation" kind))
                (is (re-find #"violates a group uniqueness constraint" msg))
                (is (re-find #"A group with name = foogroup, environment_name = test" msg))
                (is (= #{:constraintName :conflict} (-> details keys set))))))))

      (testing "groups without rules"
        (let [no-rules {:name "chaos"
                        :id (UUID/randomUUID)
                        :environment "anarchy"
                        :parent root-group-uuid
                        :classes {}
                        :variables {}}
              no-rules-path (str "/v1/groups/" (:id no-rules))]

          (testing "can be created"
            (let [{:keys [body status]} (http/put (str base-url no-rules-path)
                                                  {:content-type :json
                                                   :throw-entire-message? true
                                                   :body (json/encode no-rules)})]
              (is (= 201 status))
              (is (= no-rules (-> body (json/decode true) convert-uuids)))))

          (testing "can be retrieved"
            (let [{:keys [body status]} (http/put (str base-url no-rules-path)
                                                  {:content-type :json
                                                   :body (json/encode no-rules)})]
              (is (= 200 status))
              (is (= no-rules (-> body (json/decode true) convert-uuids)))))

          (testing "can be updated to have a rule"
            (let [add-rule-delta {:id (:id no-rules)
                                  :rule ["=" "fun" "0"]}
                  with-rules (merge-and-clean no-rules add-rule-delta)
                  {:keys [body status]} (http/post (str base-url no-rules-path)
                                                   {:content-type :json
                                                    :body (json/encode add-rule-delta)})]
              (is (= 200 status))
              (is (= with-rules (-> body (json/decode true) convert-uuids)))))

          (testing "can be updated to not have a rule"
            (let [rm-rule-delta {:id (:id no-rules)
                                 :rule nil}
                  {:keys [body status]} (http/post (str base-url no-rules-path)
                                                   {:content-type :json
                                                    :throw-entire-message? true
                                                    :body (json/encode rm-rule-delta)})]
              (is (= 200 status))
              (is (= no-rules (-> body (json/decode true) convert-uuids)))))))

      (testing "can update a group through its UUID URI"
        (let [delta {:variables {:spirit_animal "turtle"}, :classes {:fooclass {}}}
              {:keys [body status]} (http/post group-uri
                                               {:content-type :json, :body (json/encode delta)})]
          (is (= 200 status))
          (is (= (merge-and-clean group delta) (convert-uuids (json/decode body true))))))

      (testing "attempting to update a group that doesn't exist produces a 404"
        (let [delta {:variables {:exists true}}
              {:keys [body status]} (http/post (str base-url "/v1/groups/" (UUID/randomUUID))
                                               {:throw-exceptions false
                                                :content-type :json, :body (json/encode delta)})
              error (json/decode body true)]
          (is (= 404 status))
          (is (= #{:kind :msg :details} (-> error keys set)))
          (is (= "not-found" (:kind error)))))

      (testing "can delete a group through its UUID URI"
        (let [{:keys [body status]} (http/delete group-uri)]
          (is (= 204 status))
          (is (empty? body)))))))

(deftest ^:acceptance composite-parameter-and-variable-values
  (let [base-url (base-url test-config)
        ringed {:name "ringed-planet", :environment "space"
                :parameters {:rings ["the-rings"]
                             :moons 0
                             :density "1.5 g/cm^3"}}
        saturn {:name "saturn"
                :id (UUID/randomUUID)
                :environment "space"
                :rule ["=" "foo" "foo"]
                :parent root-group-uuid
                :classes {:ringed-planet {:rings ["d" "c" "b" "a" "f" "g" "methone-arc"
                                                  "anth-arc" "pallene" "e" "phoebe"]
                                          :moons 53
                                          :density "0.687 g/cm^3"}}
                :variables {:semi-major-axis "9.582 AU"
                            :eccentricity "0.055"
                            :orbital-period "10759.22 days"}}]

    (testing "composite class parameter defaults round-trip through the API"
      (let [ringed-path (str "/v1/environments/" (:environment ringed) "/classes/" (:name ringed))
            {created-body :body} (http/put (str base-url ringed-path)
                                           {:content-type :json, :body (json/encode ringed)})
            {retrieved-body :body} (http/get (str base-url ringed-path))]
        (is (= ringed (json/decode created-body true)))
        (is (= ringed (json/decode retrieved-body true)))))

    (testing "composite group class parameters and variables round-trip through the API"
      (let [saturn-path (str "/v1/groups/" (:id saturn))
            {created-body :body} (http/put (str base-url saturn-path)
                                           {:content-type :json
                                            :body (json/encode saturn)
                                            :throw-entire-message? true})
            {retrieved-body :body} (http/get (str base-url saturn-path))]
        (is (= saturn (-> created-body (json/decode true) convert-uuids)))
        (is (= saturn (-> retrieved-body (json/decode true) convert-uuids)))))))

(deftest ^:acceptance listing-and-deleting
  (let [base-url (base-url test-config)
        env-url #(str base-url "/v1/environments/" %)
        env-names ["dev" "tropical" "desert" "space"]
        envs (for [en env-names] {:name en})
        group {:name "bazgroup", :id (UUID/randomUUID), :environment "production",
               :parent root-group-uuid, :rule ["=" "foo" "foo"], :classes {}}]

    (doseq [en env-names] (http/put (env-url en)))

    (testing "lists all resource instances"
      (let [{body :body, :as resp} (http/get (str base-url "/v1/environments"))]
        (is (= 200 (:status resp)))
        (is (= (set env-names) (-> body
                                 (json/decode true)
                                 (->> (map :name))
                                 set
                                 (disj "production")
                                 (disj "staging"))))))

    (http/put (str base-url "/v1/groups/" (:id group))
              {:content-type :json, :body (json/encode group)})

    (testing "deletes resource instances"
      (doseq [en env-names]
        (is (= 204 (:status (http/delete (env-url en))))))
      (let [{body :body} (http/get (str base-url "/v1/environments"))]
        (is (empty? (-> body
                      (json/decode true)
                      (->> (map :name))
                      set
                      (disj "production")
                      (disj "staging")))))
      (is (= 204 (:status (http/delete (str base-url "/v1/groups/" (:id group))))))
      (let [{body :body} (http/get (str base-url "/v1/groups"))
            group-names (-> body
                          (json/decode true)
                          (->> (map :name))
                          set)]
        (is (not (contains? group-names (:name group))))))))

(deftest ^:acceptance hierarchy-validation
  (let [base-url (base-url test-config)
        id (UUID/randomUUID)
        with-missing-class {:name "with-missing"
                            :id id
                            :parent root-group-uuid
                            :rule ["=" "foo" "foo"]
                            :classes {:missing {}}}]

    (testing "validates group references on creation"
      (let [{:keys [body status]} (http/put (str base-url "/v1/groups/" id)
                                            {:content-type :json
                                             :body (json/encode with-missing-class)
                                             :throw-exceptions false})
            {:keys [details kind msg]} (json/decode body true)]
        (is (= 422 status))
        (is (= kind "missing-referents"))
        (is (re-find #"exist in the group's environment" body))
        (is (= (count details) 1))
        (is (= (first details)
               {:kind "missing-class", :group "with-missing", :defined-by "with-missing"
                :missing "missing", :environment "production"}))))

    (testing "group validation endpoint also validates references"
      (let [{:keys [body status]} (http/post (str base-url "/v1/validate/group")
                                             {:content-type :json
                                              :body (json/encode with-missing-class)
                                              :throw-exceptions false})
            {:keys [kind]} (json/decode body true)]
        (is (= 422 status))
        (is (= kind "missing-referents"))))

    (let [high-class {:name "high"
                      :environment "production"
                      :parameters {:refined "surely"}}
          top-group {:name "top"
                     :id (UUID/randomUUID)
                     :parent root-group-uuid
                     :environment "production",
                     :classes {:high {:refined "most"}}
                     :rule ["=" "foo" "foo"]}
          side-group {:name "side"
                      :id (UUID/randomUUID)
                      :parent root-group-uuid
                      :environment "production"
                      :classes {}
                      :rule ["=" "foo" "foo"]}
          bottom-group {:name "bottom"
                        :id (UUID/randomUUID)
                        :parent (:id side-group)
                        :environment "staging"
                        :classes {}
                        :rule ["=" "foo" "foo"]}]

      (http/put (str base-url
                     "/v1/environments/" (:environment high-class)
                     "/classes/" (:name high-class))
                {:content-type :json, :body (json/encode high-class)})
      (http/put (str base-url "/v1/groups/" (:id top-group))
                {:content-type :json, :body (json/encode top-group)})
      (http/put (str base-url "/v1/groups/" (:id side-group))
                {:content-type :json, :body (json/encode side-group)})
      (http/put (str base-url "/v1/groups/" (:id bottom-group))
                {:content-type :json, :body (json/encode bottom-group)})

      (testing "validates children when changing an ancestor's parent link"
        (let [{:keys [body status]} (http/post (str base-url "/v1/groups/" (:id side-group))
                                               {:content-type :json,
                                                :body (json/encode {:parent (:id top-group)})
                                                :throw-exceptions false})
              {:keys [details kind msg]} (json/decode body true)]
          (is (= 422 status))
          (is (= kind "missing-referents"))
          (is (= 1 (count details)))
          (is (= (first details)
                 {:kind "missing-class", :group "bottom", :missing "high"
                  :environment "staging", :defined-by "top"}))))

      (testing "group validation endpoint also does hierarchy validation"
        (let [side-group' (assoc side-group :parent (:id top-group))
              {:keys [body status]} (http/post (str base-url "/v1/validate/group")
                                               {:content-type :json
                                                :body (json/encode side-group')
                                                :throw-exceptions false})
              {:keys [kind]} (json/decode body true)]
          (is (= 422 status))
          (is (= kind "missing-referents")))))))

(deftest ^:acceptance simple-classification
  (let [base-url (base-url test-config)]
    (testing "classify a static group with one class"
      (let [class-resp (http/put (str base-url "/v1/environments/staging/classes/noisyclass")
                                 {:content-type :json
                                  :body (json/generate-string {:parameters {:verbose "true"}
                                                               :environment "staging"})})
            group {:name "test-group"
                   :id (UUID/randomUUID)
                   :environment "staging"
                   :parent root-group-uuid
                   :rule ["=" "name" "thenode"]
                   :classes {:noisyclass {:verbose "false"}}
                   :variables {:dothings "yes"}}
            group-resp (http/put (str base-url "/v1/groups/" (:id group))
                                 {:content-type :json, :body (json/generate-string group)})
            classification (-> (http/get (str base-url "/v1/classified/nodes/thenode")
                                         {:accept :json})
                             :body
                             (json/parse-string true))]
        (is (= 201 (:status class-resp)))
        (is (= 201 (:status group-resp)))
        (is (contains? (set (:groups classification)) (str (:id group))))
        (is (= {:noisyclass {:verbose "false"}}
               (:classes classification)))
        (is (= {:dothings "yes"} (:parameters classification)))))))

(deftest ^:acceptance multiple-classifications
  (let [base-url (base-url test-config)]
    (testing "classifying a node into multiple groups"
      (let [red-class {:name "redclass", :parameters {:red "the blood of angry men"
                                                     :black "the dark of ages past"}}
            blue-class {:name "blueclass", :parameters {:blue "dabudi dabudai"}}
            left-child {:name "left-child", :id (UUID/randomUUID)
                        :parent root-group-uuid
                        :rule ["=" "name" "multinode"]
                        :classes {:redclass {:red "a world about to dawn"}
                                  :blueclass {:blue "since my baby left me"}}
                        :variables {:snowflake "identical"}}
            right-child {:name "right-child", :id (UUID/randomUUID)
                         :parent root-group-uuid
                         :rule ["=" "name" "multinode"]
                         :classes {:redclass {:black "the night that ends at last"}}
                         :variables {:snowflake "identical"}}
            grandchild {:name "grandchild", :id (UUID/randomUUID)
                        :parent (:id right-child)
                        :rule ["=" "name" "multinode"]
                        :classes {:blueclass {:blue "since my baby left me"}}
                        :variables {:snowflake "identical"}}
            groups [left-child right-child grandchild]]
        (doseq [c [red-class blue-class], env ["production" "staging"]]
          (http/put (str base-url "/v1/environments/" env "/classes/" (:name c))
                    {:content-type :json, :body (json/encode c)}))
        (doseq [g groups]
          (http/put (str base-url "/v1/groups/" (:id g))
                    {:content-type :json, :body (json/encode g)}))

        (testing "merges the classifications if they are disjoint"
          (let [{:keys [status body]} (http/get (str base-url "/v1/classified/nodes/multinode"))]
            (is (= 200 status))
            (is (= {:name "multinode"
                    :environment "production"
                    :groups (-> (->> groups (map :id) set)
                              (conj root-group-uuid))
                    :classes {:blueclass {:blue "since my baby left me"}
                              :redclass {:red "a world about to dawn"
                                         :black "the night that ends at last"}}
                    :parameters {:snowflake "identical"}}
                   (-> body
                     (json/decode true)
                     (update-in [:groups] (comp set (partial map #(UUID/fromString %)))))))))

        (testing "when the classifications are not disjoint"
          (doseq [[id delta] {(:id left-child) {:environment "staging"}
                              (:id right-child) {:classes {:blueclass {:blue "suede shoes"}}}
                              (:id grandchild) {:classes {:blueclass nil}
                                                :variables {:snowflake "unique"}}}]
            (http/post (str base-url "/v1/groups/" id)
                       {:content-type :json, :body (json/encode delta)}))
          (let [groups' (doall (for [id (map :id [left-child right-child grandchild])]
                                 (-> (http/get (str base-url "/v1/groups/" id))
                                   :body
                                   (json/decode true)
                                   convert-uuids)))
                [left-child' right-child' grandchild'] groups'
                {:keys [status body]} (http/get (str base-url "/v1/classified/nodes/multinode")
                                                {:throw-exceptions false})
                error (json/decode body true)
                convert-uuids-if-group #(if (map? %) (convert-uuids %) %)]

            (testing "throws a 500 error"
              (is (= 500 status))
              (testing "that has kind 'classification-conflict'"
                (is (= "classification-conflict" (:kind error))))

              (testing "that contains details for"
                (testing "environment conflicts"
                  (let [environment-conflicts (->> (get-in error [:details :environment])
                                                (map (partial mapvals convert-uuids-if-group)))]
                    (is (= #{{:value "staging", :from left-child', :defined-by left-child'}
                             {:value "production", :from grandchild', :defined-by grandchild'}}
                           (set environment-conflicts)))))

                (testing "variable conflicts"
                  (let [variable-conflicts (get-in error [:details :variables])
                        snowflake-conflicts (->> (:snowflake variable-conflicts)
                                              (map (partial mapvals convert-uuids-if-group)))]
                    (is (= [:snowflake] (keys variable-conflicts)))
                    (is (= #{{:value "unique", :from grandchild', :defined-by grandchild'}
                             {:value "identical", :from left-child', :defined-by left-child'}}))))

                (testing "class parameter conflicts"
                  (let [class-conflicts (get-in error [:details :classes])
                        blueclass-conflicts (:blueclass class-conflicts)
                        blue-param-conflicts (->> (:blue blueclass-conflicts)
                                               (map (partial mapvals convert-uuids-if-group)))]
                    (is (= [:blueclass] (keys class-conflicts)))
                    (is (= [:blue] (keys blueclass-conflicts)))
                    (is (= #{{:value "suede shoes", :from grandchild', :defined-by right-child'}
                             {:value "since my baby left me"
                              :from grandchild'
                              :defined-by grandchild'}}))))))))))))

(deftest ^:acceptance fact-classification
  (let [base-url (base-url test-config)]
    (testing "classify a static group with one class"
      (let [class-resp (http/put (str base-url "/v1/environments/staging/classes/riscybusiness")
                                 {:content-type :json
                                  :body (json/generate-string {:parameters {}
                                                               :environment "staging"})})
            group {:name "risc-group"
                   :id (UUID/randomUUID)
                   :classes {:riscybusiness {}}
                   :environment "staging"
                   :parent root-group-uuid
                   :rule ["=" ["facts" "architecture"] "alpha"]
                   :variables {:riscisgood "yes"}}
            group-resp (http/put (str base-url "/v1/groups/" (:id group))
                                 {:content-type :json
                                  :body (json/generate-string group)
                                  :throw-entire-message? true})
            classification (-> (http/post (str base-url "/v1/classified/nodes/factnode")
                                          {:accept :json
                                           :body (json/generate-string {:facts {:values {:architecture "alpha"}}})})
                             :body
                             (json/parse-string true))]
        (is (= 201 (:status class-resp)))
        (is (= 201 (:status group-resp)))
        (is (contains? (set (:groups classification)) (str (:id group))))
        (is (= {:riscybusiness {}}
               (:classes classification)))
        (is (= {:riscisgood "yes"} (:parameters classification)))))))

(deftest ^:acceptance update-resources
  (let [base-url (base-url test-config)
        aclass {:name "aclass"
                :environment "production"
                :parameters {:log "warn", :verbose "false", :loglocation "/var/log"}}
        bclass {:name "bclass", :environment "production", :parameters {}}
        group {:name "agroup"
               :id (UUID/randomUUID)
               :parent root-group-uuid
               :environment "production"
               :rule ["=" "name" "gary"]
               :classes {:aclass {:verbose "true" :log "info"}
                         :bclass {}}
               :variables {:dns "8.8.8.8"
                           :dev_mode false}}
        new-env "spaaaace"]

    ;; insert pre-reqs
    (doseq [env ["production" new-env]
            class [aclass bclass]
            :let [class-with-env (assoc class :environment env)]]
      (http/put (str base-url "/v1/environments/" env "/classes/" (:name class-with-env))
                {:content-type :json, :body (json/encode class-with-env)}))
    (http/put (str base-url (str "/v1/groups/" (:id group)))
              {:content-type :json, :body (json/encode group)})

    (testing "can update group's name, description, rule, classes, class parameters, variables, and environment."
      (let [group-delta {:id (:id group)
                         :name "zgroup"
                         :environment new-env
                         :description "the omega of groups"
                         :rule ["=" "name" "jerry"]
                         :classes {:aclass {:log "fatal"
                                            :verbose nil
                                            :loglocation "/dev/null"}}
                         :variables {:dns nil
                                     :dev_mode true
                                     :ntp_servers ["0.us.pool.ntp.org"]}}
            group' (merge-and-clean group group-delta)
            update-resp (http/post
                          (str base-url (str "/v1/groups/" (:id group)))
                          {:content-type :json, :body (json/encode group-delta)})
            {:keys [body status]} update-resp]
        (is (= 200 status))
        (is (= group' (-> body (json/decode true) convert-uuids)))))

    (testing "when trying to update a group's environment fails, a useful error message is produced"
      (let [new-env "dne"
            group-env-delta {:id (:id group), :environment new-env}
            {:keys [body status]} (http/post (str base-url "/v1/groups/" (:id group))
                                             {:content-type :json
                                              :body (json/encode group-env-delta)
                                              :throw-exceptions false})
            {:keys [details kind msg]} (json/decode body true)]
        (is (= 422 status))
        (is (re-find #"not exist in the group's environment" msg))
        (is (every? #(and (= (:kind %) "missing-class")
                          (= (:environment %) "dne")
                          (= (:defined-by %) "zgroup"))
                    details))))))

(deftest update-root-group
  (let [base-url (base-url test-config)
        root-group (-> (http/get (str base-url "/v1/groups/" root-group-uuid))
                     :body, (json/decode true), convert-uuids)]

    (testing "can update the root group"
      (let [root-delta {:id root-group-uuid
                        :variables {:classified true}}
            {:keys [body status]} (http/post (str base-url "/v1/groups/" root-group-uuid)
                                             {:content-type :json, :body (json/encode root-delta)})]
        (is (= 200 status))
        (is (= (merge-and-clean root-group root-delta) (-> body (json/decode true) convert-uuids)))
        (let [revert-delta {:id root-group-uuid
                            :variables {:classified nil}}
              {:keys [body status]} (http/post (str base-url "/v1/groups/" root-group-uuid)
                                               {:content-type :json
                                                :body (json/encode revert-delta)})]
          (is (= 200 status))
          (is (= root-group) (-> body (json/decode true) convert-uuids)))))

    (testing "can't update the root group's rule"
      (let [root-rule-delta {:id root-group-uuid
                             :rule ["=" "name" "betty"]
                             :variables {:classified true}}
            {:keys [body status]} (http/post (str base-url "/v1/groups/" root-group-uuid)
                                             {:content-type :json
                                              :body (json/encode root-rule-delta)
                                              :throw-exceptions false})
            {:keys [details kind msg]} (json/decode body true)]
        (is (= 422 status))
        (is (= kind "root-rule-edit"))
        (is (re-find #"Changing the root group's rule " msg))
        (is (re-find #"None of your other edits were applied" msg))
        ;; TODO: once bugfix for deltas getting environment set is merged in,
        ;; remove the dissoc here
        (is (= root-rule-delta (-> details (dissoc :environment) convert-uuids)))
        (is (= root-group (-> (http/get (str base-url "/v1/groups/" root-group-uuid))
                            :body
                            (json/decode true)
                            convert-uuids)))))))

(deftest ^:acceptance put-to-existing
  (let [base-url (base-url test-config)]
    (testing "can PUT to an existing crd-resource and get a 200 back with the resource"
      (http/put (str base-url "/v1/environments/space")
                {:throw-exceptions false})
      (let [{:keys [status body]} (http/put (str base-url "/v1/environments/space"))]
        (is (= 200 status))
        (is (= {:name "space"} (json/decode body true)))))

    (let [group {:name "groucho", :id (UUID/randomUUID)
                 :environment "space", :parent root-group-uuid
                 :rule ["=" "x" "y"], :classes {}, :variables {}}
          group-url (str base-url "/v1/groups/" (:id group))
          put-opts {:content-type :json, :body (json/encode group)}]
      (http/put group-url put-opts)

      (testing "can PUT to an existing group and get a 200 back with the group"
        (let [{:keys [body status]} (http/put group-url (assoc put-opts :throw-exceptions false))]
          (is (= 200 status))
          (is (= group (-> body (json/decode true) convert-uuids)))))

      (testing "a PUT that overwrites an existing group then \"creates\" the new one"
        (let [diff-group (assoc group :environment "spaaaaace")
              {:keys [body status]} (http/put group-url (assoc put-opts
                                                               :body (json/encode diff-group)
                                                               :throw-exceptions false))]
          (is (= 201 status))
          (is (= diff-group (-> body (json/decode true) convert-uuids))))))))

(deftest ^:acceptance group-cycles
  (let [base-url (base-url test-config)
        group-id (UUID/randomUUID)
        group {:name "badgroupnono", :environment "production"
               :id group-id, :parent group-id
               :rule ["=" "a" "b"], :classes {}, :variables {}}
        group-url (str base-url "/v1/groups/" (:id group))]

    (testing "can't create a group parent set to itself"
      (let [{:keys [body status]} (http/put group-url
                                            {:content-type :json
                                             :body (json/encode group)
                                             :throw-exceptions false})]
        (is (= 422 status)))))

  (let [base-url (base-url test-config)
        group-url (str base-url "/v1/groups/")
        enos {:name "enos", :id (UUID/randomUUID), :parent root-group-uuid
              :rule ["=" "1" "2"], :classes {}, :variables {}, :environment "production"}
        yancy {:name "yancy", :id (UUID/randomUUID), :parent (:id enos)
              :rule ["=" "3" "4"], :classes {}, :variables {}, :environment "production"}
        philip {:name "philip", :id (UUID/randomUUID), :parent (:id yancy)
              :rule ["=" "5" "6"], :classes {}, :variables {}, :environment "production"}
        delta {:parent (:id philip)}]

    (http/put (str group-url (:id enos)) {:content-type :json, :body (json/encode enos)})
    (http/put (str group-url (:id yancy)) {:content-type :json, :body (json/encode yancy)})
    (http/put (str group-url (:id philip)) {:content-type :json, :body (json/encode philip)})

    (testing "can't change a parent to create a cycle"
      (let [{:keys [body status]} (http/post (str group-url (:id yancy))
                                             {:content-type :json
                                              :body (json/encode delta)
                                              :throw-exceptions false})
            {:keys [details kind msg]} (json/decode body true)
            new-yancy (assoc yancy :parent (:id philip))]
        (is (= 422 status))
        (is (= kind "inheritance-cycle"))
        (is (= (map convert-uuids details) [new-yancy philip]))
        (is (= msg
               (str "Detected group inheritance cycle: yancy -> philip -> yancy."
                    " See the `details` key for the full groups of the cycle.")))))))

(deftest ^:acceptance no-parent
  (let [base-url (base-url test-config)
        orphan {:name "orphan", :id (UUID/randomUUID)
                :parent (UUID/randomUUID), :rule ["=" "a" "a"]
                :classes {}, :variables {}, :environment "production"}]
    (testing "referring to a nonexistent parent generates an understandable error"
      (let [{:keys [body status]} (http/put (str base-url "/v1/groups/" (:id orphan))
                                            {:content-type :json
                                             :body (json/encode orphan)
                                             :throw-exceptions false})
            {:keys [details kind msg]} (json/decode body true)]
        (is (= 422 status))
        (is (= kind "missing-parent"))
        (is (= msg (str "The parent group " (:parent orphan) " does not exist.")))
        (is (= (convert-uuids details) orphan))))))

(deftest ^:acceptance classification-history
  (let [base-url (base-url test-config)
        root (-> (http/get (str base-url "/v1/groups/" root-group-uuid))
               :body
               (json/decode true)
               (update-in [:id] ->uuid))
        spaceships {:name "spaceships"
                    :rule ["and" [">=" ["facts" "pressure hulls"] "1"]
                                 [">=" ["facts" "warp cores"] "1"]]
                    :environment "deep space", :id (UUID/randomUUID), :parent root-group-uuid
                    :classes {}, :variables {}}
        spacestations {:name "spacestations"
                       :rule ["and" [">=" ["facts" "pressure hulls"] "1"]
                                    ["=" ["facts" "warp cores"] "0"]
                                    [">" ["facts" "docking pylons"] "0"]]
                       :environment "space", :id (->uuid "6dba6085-b4c4-40ef-a63b-6acd30a63acd")
                       :parent root-group-uuid
                       :classes {}, :variables {}}
        fun-spacestations {:name "spacestations to have a good time at"
                           :rule ["and" [">=" ["facts" "pressure hulls"] "1"]
                                        ["=" ["facts" "warp cores"] "0"]
                                        [">" ["facts" "docking pylons"] "0"]
                                        [">=" ["facts" "bars"] "1"]]
                           :environment "space", :id (UUID/randomUUID), :parent root-group-uuid
                           :classes {}, :variables {}}
        ds9-node {:name "Deep Space 9"
                  :facts {"pressure hulls" "3"
                          "docking ports" "18"
                          "docking pylons" "3"
                          "warp cores" "0"
                          "bars" "1"}}
        ncc1701d-node {:name "USS Enterprise"
                       :facts {"registry" "NCC-1701-D"
                               "warp cores" "1"
                               "pressure hulls" "2"
                               "docking ports" "3"}}
        ds9-explanation (apply merge
                               (for [group [spacestations fun-spacestations root]
                                     :let [{:keys [id rule]} group]]
                                 {id (rules/explain-rule {:when rule, :group-id id} ds9-node)}))
        ncc1701d-explanation (apply merge
                                    (for [group [spaceships root]]
                                      (let [{:keys [id rule]} group
                                            full-rule {:when rule, :group-id id}]
                                        {id (rules/explain-rule full-rule ncc1701d-node)})))]

    ;; create groups
    (doseq [g [spaceships spacestations fun-spacestations]]
      (http/put (str base-url "/v1/groups/" (:id g))
                {:content-type :json, :body (json/generate-string g)}))

    ;; populate classification history
    (doseq [{facts :facts, :as node} [ds9-node ncc1701d-node]]
      (http/post (str base-url "/v1/classified/nodes/" (:name node))
                 {:content-type :json
                  :body (json/generate-string (-> node
                                                (dissoc :facts)
                                                (assoc :facts {:values facts})))}))

    (testing "can retrieve classification history"
      (testing "for a single node"
        (let [{:keys [status body]} (http/get (str base-url "/v1/nodes/" (:name ds9-node)))
              node (sc/validate ClientNode (json/decode body true))
              expected-node {:name (:name ds9-node)
                             :check_ins [{:explanation ds9-explanation}]}]
          (is (= 200 status))
          (is (= expected-node (-> node
                                 (update-in [:check_ins 0] dissoc :time)
                                 (update-in [:check_ins 0 :explanation] #(mapkeys ->uuid %)))))))

      (testing "for all nodes"
        (let [{:keys [status body]} (http/get (str base-url "/v1/nodes"))
              node-names (set (map :name [ds9-node ncc1701d-node]))
              nodes (-> body
                      (json/decode true)
                      (->>
                        (sc/validate [ClientNode])
                        (filter #(contains? node-names (:name %)))))
              expected-nodes [{:name (:name ds9-node)
                               :check_ins [{:explanation ds9-explanation}]}
                              {:name (:name ncc1701d-node)
                               :check_ins [{:explanation ncc1701d-explanation}]}]]
          (is (= 200 status))
          (is (= expected-nodes
                 (->> nodes
                   (map #(update-in % [:check_ins 0] dissoc :time))
                   (map #(update-in % [:check_ins 0 :explanation] (partial mapkeys ->uuid)))))))))))
