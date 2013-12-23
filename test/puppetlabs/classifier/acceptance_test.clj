(ns puppetlabs.classifier.acceptance-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh] :rename {sh blocking-sh}]
            [cheshire.core :as json]
            [me.raynes.conch.low-level :refer [proc] :rename {proc sh}]
            puppetlabs.trapperkeeper.core
            [clj-http.client :as http])
  (:import [java.util.concurrent TimeoutException TimeUnit]))

(def test-config
  "Path from the root of the repo to the configuration file to use for the tests
  in this namespace."
  "ext/classifier.conf")

(defn- base-url
  [config-path]
  (let [app-config (#'puppetlabs.trapperkeeper.core/parse-config-file config-path)
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
  (let [{initdb-stat :exit
         initdb-err :err
         initdb-out :out} (blocking-sh "lein" "run"
                                       "-b" "resources/initdb.cfg"
                                       "-c" config-path)]
    (when-not (= initdb-stat 0)
      (throw (new RuntimeException
                  (str "Could not initialize database! initdb service says: "
                       (if-not (empty? initdb-err) initdb-err initdb-out)))))
    (let [server-proc (sh "lein" "trampoline" "run"
                          "-b" "resources/bootstrap.cfg"
                          "-c" config-path)
          timeout-ms 30000
          server-blocker (future (block-until-ready server-proc))]
      ;; Block on the server starting for up to thirty seconds.
      ;; If it doesn't start within that time, exit nonzero.
      (try
        (.get server-blocker timeout-ms TimeUnit/MILLISECONDS)
        (catch TimeoutException e
          (future-cancel server-blocker)
          (binding [*out* *err*]
            (println "Server did not start within the allotted time"
                     (str "(" timeout-ms " ms)")))
          (System/exit 1)))
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

(use-fixtures :once with-classifier-instance)

(deftest ^{:database true, :acceptance true} smoke
  (let [base-url (base-url test-config)]
    (testing "can create a new node"
      (let [resp (http/put (str base-url "/v1/nodes/test-node"))]
        (is (= 201 (:status resp)))
        (is (= {"name" "test-node"} (json/parse-string (:body resp))))))))
