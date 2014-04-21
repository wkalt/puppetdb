(require '[clojure.string :as string])
(require '[clojure.java.io :refer [file]])
(require '[clojure.java.shell :refer [sh]])

(def tk-version "0.3.4")

(defn deploy-info
  [url]
  {:url url
   :username :env/nexus_jenkins_username
   :password :env/nexus_jenkins_password
   :sign-releases false})

(defproject puppetlabs/classifier "0.2.2-SNAPSHOT"
  :description "Node classifier"
  :pedantic? :abort
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/java.jdbc "0.3.2"]
                 [cheshire "5.2.0"]
                 [clj-http "0.7.8"]
                 [clj-stacktrace "0.2.7"]
                 [compojure "1.1.6" :exclusions [[clj-time] [org.clojure/tools.macro]]]
                 [java-jdbc/dsl "0.1.0"]
                 [liberator "0.10.0"]
                 [migratus "0.7.0"]
                 [org.postgresql/postgresql "9.3-1100-jdbc4"]
                 [prismatic/schema "0.2.0"]
                 [slingshot "0.10.3"]
                 [puppetlabs/http-client "0.1.4"]
                 [puppetlabs/kitchensink "0.5.3"]
                 [puppetlabs/trapperkeeper ~tk-version]
                 [puppetlabs/trapperkeeper-webserver-jetty9 "0.3.4"]]
  :profiles {:dev {:dependencies [[me.raynes/conch "0.5.0" :exclusions [org.clojure/tools.macro]]
                                  [ring-mock "0.1.5"]
                                  [spyscope "0.1.3" :exclusions [[clj-time]]]
                                  [puppetlabs/trapperkeeper ~tk-version :classifier "test"]]
                   :injections [(require 'spyscope.core)]}}
  :repositories [["releases" "http://nexus.delivery.puppetlabs.net/content/repositories/releases/"]
                 ["snapshots" "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/"]]
  :test-selectors {:default (complement :database)
                   :database :database
                   :acceptance :acceptance
                   :all (constantly true)}
  :aliases {"initdb" ["run" "--bootstrap-config" "resources/initdb.cfg"]}
  :uberjar-name "classifier.jar"
  :main ^:skip-aot puppetlabs.trapperkeeper.main
  ;; For release
  :plugins [[lein-release "1.0.5"]]
  :lein-release {:scm :git, :deploy-via :lein-deploy}
  :deploy-repositories [["releases" ~(deploy-info "http://nexus.delivery.puppetlabs.net/content/repositories/releases/")]
                        ["snapshots" ~(deploy-info "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/")]]
  ;; Put our puppet plugin code in the jar because that's where packaging
  ;; expects it
  :resource-paths ["puppet/lib"])
