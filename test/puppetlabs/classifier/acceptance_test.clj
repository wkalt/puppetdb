(ns puppetlabs.classifier.acceptance-test
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh] :rename {sh blocking-sh}]
            [clojure.test :refer :all]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [me.raynes.conch.low-level :refer [destroy proc stream-to-out] :rename {proc sh}]
            [schema.test]
            [puppetlabs.classifier.util :refer [merge-and-clean]]
            [puppetlabs.kitchensink.core :refer [ini-to-map spit-ini]])
  (:import [java.util.concurrent TimeoutException TimeUnit]
           java.util.regex.Pattern
           java.util.UUID))

(def test-config
  "Path from the root of the repo to the configuration file to use for the tests
  in this namespace."
  "ext/classifier.ini")

(defn- base-url
  [config-path]
  (let [app-config (ini-to-map config-path)
        host (get-in app-config [:webserver :host])]
    (str "http://" (if (= host "0.0.0.0") "localhost" host)
               ":" (get-in app-config [:webserver :port])
                   (get-in app-config [:clasifier :url-prefix]))))

(defn- block-until-ready
  [server-process]
  (let [out-lines (-> (:out server-process) io/reader line-seq)]
    (dorun (take-while #(not (re-find #"started o.e.j.s.h.ContextHandler" %))
                         out-lines))))

(defn start!
  [config-path]
  "Initialize the database and then start a classifier server using the given
  config file, returning a conch process map describing the server instance
  process."
  (let [base-config (ini-to-map config-path)
        test-db {:subprotocol "postgresql"
                 :subname (or (System/getenv "CLASSIFIER_DBNAME")
                              "classifier_test")
                 :user (or (System/getenv "CLASSIFIER_DBUSER")
                           "classifier_test")
                 :password (or (System/getenv "CLASSIFIER_DBPASS")
                               "classifier_test")}
        config-with-db (assoc base-config :database test-db)
        test-config-file (java.io.File/createTempFile "classifier-test-" ".ini")
        test-config-path (.getAbsolutePath test-config-file)
        _ (spit-ini test-config-file config-with-db)
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
        (future (stream-to-out server-proc :out))
        (future (stream-to-out server-proc :err))
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
    (testing "can create a new node"
      (let [resp (http/put (str base-url "/v1/nodes/test-node"))]
        (is (= 201 (:status resp)))
        (is (= {"name" "test-node"} (json/parse-string (:body resp))))))
    (testing "can delete a node"
      (let [resp (http/delete (str base-url "/v1/nodes/test-node"))]
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
      (let [resp (quiet-put (str base-url "/v1/groups/test-group"))]
        (is (= 400 (:status resp)))
        (is (valid-400-resp-body? (:body resp)))))))

(deftest ^:acceptance groups-and-uuids
  (let [base-url (base-url test-config)
        group {:name "foogroup"
               :id (UUID/randomUUID)
               :environment "production"
               :rule {:when ["=" "foo" "foo"]}
               :parent "default"
               :classes {}}]
    (testing "trying to create a group by specifying id in URI causes a 405"
      (let [{:keys [status]} (http/put (str base-url "/v1/groups/" (:id group))
                                       {:throw-exceptions false
                                        :content-type :json
                                        :body (json/encode group)})]
        (is (= 405 status))))
    (let [stored-group (-> (http/put (str base-url "/v1/groups/" (:name group))
                                     {:content-type :json, :body (json/encode (dissoc group :id))})
                         :body
                         (json/decode true))
          group-uuid-url (str base-url "/v1/groups/" (:id stored-group))]
      (testing "can retrieve a group by giving its UUID"
        (let [{:keys [body status]} (http/get group-uuid-url)]
          (is (= 200 status))
          (is (= stored-group (json/decode body true)))))
      (testing "can update a group through its UUID URI"
        (let [delta {:variables {:spirit_animal "turtle"}}
              {:keys [body status]} (http/post group-uuid-url
                                               {:content-type :json, :body (json/encode delta)})]
          (is (= 200 status))
          (is (= (merge stored-group delta) (json/decode body true)))))
      (testing "can delete a group through its UUID URI"
        (let [{:keys [body status]} (http/delete group-uuid-url)]
          (is (= 204 status))
          (is (empty? body)))))))

(deftest ^:acceptance listing-and-deleting
  (let [base-url (base-url test-config)
        node-url #(str base-url "/v1/nodes/" %)
        node-names ["seven-of-nine" "two-of-three" "locutus-of-borg"]
        nodes (for [nn node-names] {:name nn})
        group {:name "bargroup", :environment "production", :parent "default"
               :rule {:when ["=" "foo" "foo"]}, :classes {}}]
    (testing "lists all resource instances"
      (http/put (str base-url "/v1/groups/" (:name group))
                {:content-type :json, :body (json/encode group)})
      (doseq [nn node-names] (http/put (node-url nn)))
      (let [{body :body, :as resp} (http/get (str base-url "/v1/nodes"))]
        (is (= 200 (:status resp)))
        (is (= (set nodes) (set (json/decode body true))))))
    (testing "deletes resource instances"
      (doseq [nn node-names]
        (is (= 204 (:status (http/delete (node-url nn))))))
      (let [{body :body} (http/get (str base-url "/v1/nodes"))]
        (is (empty? (json/decode body))))
      (is (= 204 (:status (http/delete (str base-url "/v1/groups/" (:name group))))))
      (let [{body :body} (http/get (str base-url "/v1/groups"))
            group-names (-> body
                          (json/decode true)
                          (->> (map :name))
                          set)]
        (is (not (contains? group-names (:name group))))))))

(deftest ^:acceptance hierarchy-validation
  (let [base-url (base-url test-config)]
    (testing "validates group when creating"
      (let [group-with-missing-class {:parent "default", :rule {:when ["=" "foo" "foo"]}
                                      :classes {:missing {}}}
            {:keys [body status]} (http/put (str base-url "/v1/groups/with-missing")
                                            {:content-type :json
                                             :body (json/encode group-with-missing-class)
                                             :throw-exceptions false})
            {:keys [details kind msg]} (json/decode body true)]
        (is (= 412 status))
        (is (= kind "missing-referents"))
        (is (re-find #"exist in the group's environment" body))
        (is (= (count details) 1))
        (is (= (first details)
               {:kind "missing-class", :group "with-missing", :defined-by "with-missing"
                :missing "missing", :environment "production"}))))

    (testing "validates children when changing an ancestor's parent link"
      (let [high-class {:name "high", :parameters {:refined "surely"}}
            top-group {:name "top", :environment "production", :parent "default"
                       :classes {:high {:refined "most"}}, :rule {:when ["=" "foo" "foo"]}}
            side-group {:name "side", :environment "production", :parent "default"
                        :classes {}, :rule {:when ["=" "foo" "foo"]}}
            bottom-group {:name "bottom", :environment "staging", :parent "side"
                          :classes {}, :rule {:when ["=" "foo" "foo"]}}]
        (http/put (str base-url "/v1/environments/production/classes/" (:name high-class))
                  {:content-type :json, :body (json/encode high-class)})
        (http/put (str base-url "/v1/groups/" (:name top-group))
                  {:content-type :json, :body (json/encode top-group)})
        (http/put (str base-url "/v1/groups/" (:name side-group))
                  {:content-type :json, :body (json/encode side-group)})
        (http/put (str base-url "/v1/groups/" (:name bottom-group))
                  {:content-type :json, :body (json/encode bottom-group)})
        (let [{:keys [body status]} (http/post (str base-url "/v1/groups/" (:name side-group))
                                               {:content-type :json,
                                                :body (json/encode {:parent "top"})
                                                :throw-exceptions false})
              {:keys [details kind msg]} (json/decode body true)]
          (is (= 409 status))
          (is (= kind "missing-referents"))
          (is (= 1 (count details)))
          (is (= (first details)
                 {:kind "missing-class", :group "bottom", :missing "high"
                  :environment "staging", :defined-by "top"})))))))

(deftest ^:acceptance simple-classification
  (let [base-url (base-url test-config)]
    (testing "classify a static group with one class"
      (let [class-resp (http/put (str base-url "/v1/environments/staging/classes/noisyclass")
                                 {:content-type :json
                                  :body (json/generate-string {:parameters {:verbose "true"}
                                                               :environment "staging"})})
            group-resp (http/put (str base-url "/v1/groups/test-group")
                                 {:content-type :json
                                  :body (json/generate-string
                                          {:classes {:noisyclass {:verbose "false"}}
                                           :environment "staging"
                                           :parent "default"
                                           :rule {:when ["=" "name" "thenode"]}
                                           :variables {:dothings "yes"}})})
            classification (-> (http/get (str base-url "/v1/classified/nodes/thenode")
                                         {:accept :json})
                             :body
                             (json/parse-string true))]
        (is (= 201 (:status class-resp)))
        (is (= 201 (:status group-resp)))
        (is (= ["test-group"] (:groups classification)))
        (is (= ["noisyclass"] (:classes classification)))
        (is (= {:noisyclass {:verbose "false"}}
               (:parameters classification)))
        (is (= {:dothings "yes"} (:variables classification)))))))

(deftest ^:acceptance update-resources
  (let [base-url (base-url test-config)
        aclass {:name "aclass"
                :environment "production"
                :parameters {:log "warn", :verbose "false", :loglocation "/var/log"}}
        bclass {:name "bclass", :environment "production", :parameters {}}
        group {:name "agroup"
               :parent "default"
               :environment "production"
               :rule {:when ["=" "name" "gary"]}
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
    (http/put (str base-url "/v1/groups/agroup")
              {:content-type :json, :body (json/encode group)})

    (testing "can update group rule, classes, class parameters, variables, and environment."
      (let [group-delta {:environment new-env
                         :rule {:when ["=" "name" "jerry"]}
                         :classes {:aclass {:log "fatal"
                                            :verbose nil
                                            :loglocation "/dev/null"}}
                         :variables {:dns nil
                                     :dev_mode true
                                     :ntp_servers ["0.us.pool.ntp.org"]}}
            group' (merge-and-clean group group-delta)
            update-resp (http/post
                          (str base-url "/v1/groups/agroup")
                          {:content-type :json, :body (json/encode group-delta)})
            {:keys [body status]} update-resp]
        (is (= 200 status))
        (is (= group' (-> body (json/decode true) (dissoc :id))))))

    (testing "when trying to update a group's environment fails, a useful error message is produced"
      (let [new-env "dne"
            group-env-delta {:environment new-env}
            update-resp (http/post (str base-url "/v1/groups/agroup")
                                   {:content-type :json
                                    :body (json/encode group-env-delta)
                                    :throw-exceptions false})
            {:keys [body status]} update-resp
            {:keys [details kind msg]} (json/decode body true)]
        (is (= 412 status))
        (is (re-find #"not exist in the group's environment" msg))
        (is (every? #(and (= (:kind %) "missing-class")
                          (= (:environment %) "dne")
                          (= (:defined-by %) "agroup"))
                    details))))))

(deftest ^:acceptance put-to-existing
  (let [base-url (base-url test-config)]
    (testing "can PUT to an existing crd-resource and get a 200 back with the resource"
      (http/put (str base-url "/v1/environments/space"))
      (let [{:keys [status body]} (http/put (str base-url "/v1/environments/space"))]
        (is (= 200 status))
        (is (= {:name "space"} (json/decode body true)))))
      (let [group {:name "groucho", :environment "space"
                   :parent "default", :rule {:when ["=" "x" "y"]}
                   :classes {}, :variables {}}
            group-url (str base-url "/v1/groups/" (:name group))
            put-opts {:content-type :json, :body (json/encode group)}]
        (http/put group-url put-opts)
        (testing "can PUT to an existing group and get a 200 back with the group"
          (let [{:keys [body status]} (http/put group-url put-opts)]
            (is (= 200 status))
            (is (= group (-> body
                           (json/decode true)
                           (dissoc :id))))))
        (testing "a PUT that overwrites an existing group that \"creates\" the new one"
          (let [diff-group (assoc group :environment "spaaaaace")
                {:keys [body status]} (http/put group-url (assoc put-opts
                                                                 :body (json/encode diff-group)
                                                                 :throw-exceptions false))]
            (is (= 201 status))
            (is (= diff-group (-> body
                                (json/decode true)
                                (dissoc :id)))))))))
