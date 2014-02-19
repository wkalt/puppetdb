(ns puppetlabs.classifier.acceptance-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh] :rename {sh blocking-sh}]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [schema.test]
            [me.raynes.conch.low-level :refer [proc stream-to-out] :rename {proc sh}]
            [puppetlabs.classifier.util :as util])
  (:import [java.util.concurrent TimeoutException TimeUnit]))

(def test-config
  "Path from the root of the repo to the configuration file to use for the tests
  in this namespace."
  "ext/classifier.conf")

(defn- base-url
  [config-path]
  (let [app-config (util/ini->map config-path)
        host (get-in app-config [:webserver :host])]
    (str "http://" (if (= host "0.0.0.0") "localhost" host)
               ":" (get-in app-config [:webserver :port])
                   (get-in app-config [:clasifier :url-prefix]))))

(defn- block-until-ready
  [server-process]
  (let [err-lines (-> (:err server-process) io/reader line-seq)]
    (dorun (take-while #(not (re-find #"ContextHandler:started" %))
                         err-lines))))

(defn start!
  [config-path]
  "Initialize the database and then start a classifier server using the given
  config file, returning a conch process map describing the server instance
  process."
  (let [base-config (util/ini->map config-path)
        test-db {:subprotocol "postgresql"
                 :subname (or (System/getenv "CLASSIFIER_DBNAME")
                              "classifier_test")
                 :user (or (System/getenv "CLASSIFIER_DBUSER")
                           "classifier_test")
                 :password (or (System/getenv "CLASSIFIER_DBPASS")
                               "classifier_test")}
        config-with-db (assoc base-config :database test-db)
        test-config-file (java.io.File/createTempFile "classifier-test-" "conf")
        test-config-path (.getAbsolutePath test-config-file)
        _ (util/write-map-as-ini! config-with-db test-config-path)
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

(deftest ^:acceptance object-validation
  (let [base-url (base-url test-config)
        quiet-put #(http/put % {:throw-exceptions false})
        valid-400-resp-body? (fn [body]
                               (= #{:submitted :schema :error}
                                  (-> body (json/decode true) keys set)))]
    (testing "schema-noncompliant objects in requests elicit a 400 response."
      (let [resp (quiet-put (str base-url "/v1/classes/foo"))]
        (is (= 400 (:status resp)))
        (is (valid-400-resp-body? (:body resp))))
      (let [resp (quiet-put (str base-url "/v1/groups/test-group"))]
        (is (= 400 (:status resp)))
        (is (valid-400-resp-body? (:body resp))))
      (let [resp (http/post (str base-url "/v1/rules")
                            {:throw-exceptions false})]
        (is (= 400 (:status resp)))
        (is (valid-400-resp-body? (:body resp)))))))

(deftest ^:acceptance listing-and-deleting
  (let [base-url (base-url test-config)
        node-url #(str base-url "/v1/nodes/" %)
        node-names ["seven-of-nine" "two-of-three" "locutus-of-borg"]
        nodes (for [nn node-names] {:name nn})
        group {:name "bargroup" :environment "production", :classes {}}]
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

(deftest ^:acceptance missing-referents-explanation
  (let [base-url (base-url test-config)
        group-with-missing-class {:classes {:missing {}}}
        {:keys [body status], :as resp} (http/put (str base-url "/v1/groups/with-missing")
                                                  {:content-type :json
                                                   :body (json/encode group-with-missing-class)
                                                   :throw-exceptions false})]
    (is (= 412 status))
    (is (re-find #"The group you tried to create" body))
    (is (re-find #"refers to a class" body))
    (is (re-find #"no such class could be found" body))))

(deftest ^:acceptance simple-classification
  (let [base-url (base-url test-config)]
    (testing "classify a static group with one class"
      (let [env-resp   (http/put (str base-url "/v1/environments/staging"))
            class-resp (http/put (str base-url "/v1/classes/noisyclass")
                                 {:content-type :json
                                  :body (json/generate-string {:parameters {:verbose "true"}
                                                               :environment "staging"})})
            group-resp (http/put (str base-url "/v1/groups/test-group")
                                 {:content-type :json
                                  :body (json/generate-string
                                          {:classes {:noisyclass {:verbose "false"}}
                                           :environment "staging"
                                           :variables {:dothings "yes"}})})
            rule-resp  (http/post (str base-url "/v1/rules")
                                  {:content-type :json
                                   :body (json/generate-string {:when ["=" "name" "thenode"]
                                                                :groups ["test-group"]})})
            classification (-> (http/get (str base-url "/v1/classified/nodes/thenode")
                                         {:accept :json})
                             :body
                             (json/parse-string true))]
        (is (= 201 (:status class-resp)))
        (is (= 201 (:status group-resp)))
        (is (= 201 (:status rule-resp)))
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
               :environment "production"
               :classes {:aclass {:verbose "true" :log "info"}
                         :bclass {}}
               :variables {:dns "8.8.8.8"}}
        new-env "spaaaace"]

    ;; insert pre-reqs
    (doseq [env ["production" new-env]
            class [aclass bclass]
            :let [class-with-env (assoc class :environment env)]]
      (http/put (str base-url "/v1/classes/" (:name class-with-env))
                {:content-type :json, :body (json/encode class-with-env)}))
    (http/put (str base-url "/v1/groups/agroup")
              {:content-type :json, :body (json/encode group)})

    (testing "can update group classes, class parameters, variables, and environment."
      (let [group-delta {:environment new-env
                         :classes {:aclass {:log "fatal"
                                            :verbose nil
                                            :loglocation "/dev/null"}}
                         :variables {:dns nil}}
            group' (util/merge-and-clean group group-delta)
            update-resp (http/post
                          (str base-url "/v1/groups/agroup")
                          {:content-type :json, :body (json/encode group-delta)})
            {:keys [body status]} update-resp]
        (is (= 200 status))
        (is (= group' (json/decode body true)))))

    (testing "when trying to update a group's environment fails, a useful error message is produced"
      (let [new-env "dne"
            group-env-delta {:environment new-env}
            update-resp (http/post (str base-url "/v1/groups/agroup")
                                   {:content-type :json
                                    :body (json/encode group-env-delta)
                                    :throw-exceptions false})
            {:keys [body status]} update-resp]
        (is (= 412 status))
        (is (re-find #"class with primary key values \(aclass, dne\)" body))))))
