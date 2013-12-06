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
                 [compojure "1.1.6" :exclusions [[clj-time]]]
                 ;; Logging
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/java.jdbc "0.3.0-beta1"]
                 [org.postgresql/postgresql "9.3-1100-jdbc4"]
                 [liberator "0.10.0"]
                 [cheshire "5.2.0"]
                 [puppetlabs/trapperkeeper "0.1.0-20131205.214040-1"]]
  :profiles {:dev {:dependencies [[ring-mock "0.1.5"]
                                  [spyscope "0.1.3" :exclusions [[clj-time]]]]
                   :injections [(require 'spyscope.core)]}}
  :repositories [["releases" "http://nexus.delivery.puppetlabs.net/content/repositories/releases/"]
                 ["snapshots" "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/"]]
  :test-selectors {:default (complement :database)
                   :database :database
                   :all (constantly true)}
  :main puppetlabs.trapperkeeper.main)
