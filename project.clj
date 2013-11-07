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
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ring/ring-jetty-adapter "1.2.0"]
                 [compojure "1.1.5"]
                 [org.clojure/java.jdbc "0.3.0-beta1"]
                 [org.postgresql/postgresql "9.3-1100-jdbc4"]
                 [cheshire "5.2.0"]]
  :profiles {:dev {:dependencies [[ring-mock "0.1.5"]]}}
  :aot [puppetlabs.classifier.core]
  :main puppetlabs.classifier.core)
