(require '[clojure.string :as string])
(require '[clojure.java.io :refer [file]])
(require '[clojure.java.shell :refer [sh]])

(def tk-version "0.5.1")
(def ks-version "0.7.2")
(def rbac-svc-version "0.1.13")

(defn deploy-info
  [url]
  {:url url
   :username :env/nexus_jenkins_username
   :password :env/nexus_jenkins_password
   :sign-releases false})

(defproject puppetlabs/classifier "0.7.4"
  :description "Node classifier"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/java.jdbc "0.3.2"]
                 [cheshire "5.3.1"]
                 [clj-stacktrace "0.2.7"]
                 [clj-time "0.6.0"]
                 [compojure "1.1.6" :exclusions [[clj-time] [org.clojure/tools.macro]]]
                 [fast-zip-visit "1.0.2"]
                 [java-jdbc/dsl "0.1.0"]
                 [liberator "0.11.0"]
                 [migratus "0.7.0"]
                 [overtone/at-at "1.2.0"]
                 [org.postgresql/postgresql "9.3-1100-jdbc41"]
                 [prismatic/schema "0.2.2"]
                 [slingshot "0.10.3"]
                 [puppetlabs/http-client "0.2.6" :exclusions [commons-io]]
                 [puppetlabs/certificate-authority "0.5.0"]
                 [puppetlabs/kitchensink ~ks-version]
                 [puppetlabs/liberator-util "0.2.1" :exclusions [liberator]]
                 [puppetlabs/schema-tools "0.1.0" :exclusions [prismatic/schema]]
                 [puppetlabs/trapperkeeper ~tk-version]]
  :profiles {:dev {:dependencies [[clj-http "0.7.9" :exclusions [org.apache.httpcomponents/httpclient]]
                                  [me.raynes/conch "0.5.0" :exclusions [org.clojure/tools.macro]]
                                  [ring-mock "0.1.5"]
                                  [spyscope "0.1.3" :exclusions [[clj-time]]]
                                  [puppetlabs/kitchensink ~ks-version :classifier "test"]
                                  [puppetlabs/pe-rbac-service ~rbac-svc-version]
                                  [puppetlabs/pe-rbac-service ~rbac-svc-version :classifier "test"]
                                  [puppetlabs/pe-trapperkeeper-ldap-apacheds "0.2.9"
                                   :exclusions [bouncycastle/bcprov-jdk15 org.slf4j/slf4j-log4j12]]
                                  [puppetlabs/trapperkeeper ~tk-version :classifier "test"]
                                  [puppetlabs/trapperkeeper-webserver-jetty9 "0.7.2"]]
                   :injections [(require 'spyscope.core)]}}
  :repositories [["releases" "http://nexus.delivery.puppetlabs.net/content/repositories/releases/"]
                 ["snapshots" "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/"]]
  :test-selectors {:default (complement :database)
                   :database :database
                   :acceptance :acceptance
                   :all (constantly true)}
  :aliases {"initdb" ["run" "--bootstrap-config" "resources/puppetlabs/classifier/initdb.cfg"]}
  :uberjar-name "classifier.jar"
  :main ^:skip-aot puppetlabs.trapperkeeper.main
  ;; For release
  :plugins [[lein-release "1.0.5"]]
  :jar-exclusions [#"\.sw[a-z]$" #"~$" #"logback\.xml$" #"log4j\.properties$"]
  :lein-release {:scm :git, :deploy-via :lein-deploy}
  :deploy-repositories [["releases" ~(deploy-info "http://nexus.delivery.puppetlabs.net/content/repositories/releases/")]
                        ["snapshots" ~(deploy-info "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/")]]
  ;; Put our puppet plugin code in the jar because that's where packaging
  ;; expects it
  :resource-paths ["resources" "puppet/lib" "resources/puppetlabs/classifier" "resources/ext/docs"])
