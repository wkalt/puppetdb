(ns puppetlabs.classifier.application
  (:require [compojure.core :refer [context]]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage.postgres :as postgres]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [clojure.tools.logging :as log]))

(def db-spec {:subprotocol "postgresql"
              :subname "classifier"
              :user "classifier"
              :password "classifier"})

(defn on-shutdown
  []
  (log/info "Classifier service shutting down."))

(defn- add-url-prefix
  "Puts the application in an optional url prefix. If none is given just
  returns the original handler."
  [prefix app]
  (if (seq prefix)
    (context prefix [] app)
    app))

(defservice classifier-service
   {:depends [[:config-service get-config]
              [:webserver-service add-ring-handler]]
    :provides [shutdown]}
   (let [database     (postgres/new-db db-spec)
         context-path (get-in (get-config) [:classifier :url-prefix] "")
         app          (add-url-prefix context-path (http/app database))]
     (add-ring-handler app context-path))
   {:shutdown on-shutdown})

(defservice initdb
  {:depends [[:config-service get-config]
             [:shutdown-service request-shutdown]]}
  (postgres/drop-public-tables db-spec)
  (postgres/init-schema db-spec)
  (request-shutdown)
  {})
