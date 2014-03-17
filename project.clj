(require '[clojure.string :as string])
(require '[clojure.java.io :refer [file]])
(require '[clojure.java.shell :refer [sh]])

(def version-string
  (memoize
  (fn []
    "Determine the version number using 'rake version -s'"
    (if (.exists (file "version"))
      (string/trim (slurp "version"))
      (let [command ["rake" "package:version" "-s"]
            {:keys [exit out err]} (apply sh command)]
        (if (zero? exit)
          (string/trim out)
          "0.0-dev-build"))))))

(defproject classifier (version-string)
  :description "Node classifier"
  :pedantic? :abort
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/java.jdbc "0.3.2"]
                 [cheshire "5.2.0"]
                 [clj-http "0.7.8"]
                 [compojure "1.1.6" :exclusions [[clj-time] [org.clojure/tools.macro]]]
                 [java-jdbc/dsl "0.1.0"]
                 [liberator "0.10.0"]
                 [migratus "0.7.0"]
                 [org.postgresql/postgresql "9.3-1100-jdbc4"]
                 [prismatic/schema "0.2.0"]
                 [slingshot "0.10.3"]
                 [puppetlabs/http-client "0.1.0"]
                 [puppetlabs/kitchensink "0.5.3"]
                 [puppetlabs/trapperkeeper "0.3.4"]
                 [puppetlabs/trapperkeeper-webserver-jetty7 "0.3.3"]]
  :profiles {:dev {:dependencies [[me.raynes/conch "0.5.0" :exclusions [org.clojure/tools.macro]]
                                  [ring-mock "0.1.5"]
                                  [spyscope "0.1.3" :exclusions [[clj-time]]]]
                   :injections [(require 'spyscope.core)]}}
  :repositories [["releases" "http://nexus.delivery.puppetlabs.net/content/repositories/releases/"]
                 ["snapshots" "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/"]]
  :test-selectors {:default (complement :database)
                   :database :database
                   :acceptance :acceptance
                   :all (constantly true)}
  :aliases {"initdb" ["run" "--bootstrap-config" "resources/initdb.cfg"]}
  :uberjar-name "classifier.jar"
  :main puppetlabs.trapperkeeper.main)
