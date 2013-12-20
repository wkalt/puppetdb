(ns puppetlabs.classifier.acceptance-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh] :rename {sh blocking-sh}]
            [cheshire.core :as json]
            [me.raynes.conch.low-level :refer [proc] :rename {proc sh}]
            puppetlabs.trapperkeeper.core
            [clj-http.client :as http]))

(def test-config
  "Path from the root of the repo to the configuration file to use for the tests
  in this namespace."
  "ext/classifier.conf")

(defn- base-url
  [config-path]
  (let [app-config (#'puppetlabs.trapperkeeper.core/parse-config-file config-path)]
    (str "http://" (get-in app-config [:webserver :host])
               ":" (get-in app-config [:webserver :port])
                   (get-in app-config [:clasifier :url-prefix]))))

(defn start!
  [config-path]
  "Initialize the database and then start a classifier server using the given
  config file, returning a conch process map describing the server instance
  process."
  (let [{initdb-stat :exit
         initdb-err :err
         initdb-out :out} (blocking-sh "lein" "run"
                                       "-b" "resources/initdb.cfg"
                                       "-c" config-path)]
    (when-not (= initdb-stat 0)
      (throw (new RuntimeException
                  (str "Could not initialize database! initdb service says: "
                       (if-not (empty? initdb-err) initdb-err initdb-out)))))
    (let [proc-map (sh "lein" "trampoline" "run"
                       "-b" "resources/bootstrap.cfg"
                       "-c" config-path)
          server-err-lines (-> (:err proc-map) io/reader line-seq)]
      ;; block on the server until it starts
      (dorun (take-while #(not (re-find #"AbstractConnector:Started" %))
                         server-err-lines))
      proc-map)))

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

(use-fixtures :once with-classifier-instance)

(deftest ^{:database true, :acceptance true} smoke
  (let [base-url (base-url test-config)]
    (testing "can create a new node"
      (let [resp (http/put (str base-url "/v1/nodes/test-node"))]
        (is (= 201 (:status resp)))
        (is (= {"name" "test-node"} (json/parse-string (:body resp))))))))
