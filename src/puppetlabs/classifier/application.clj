(ns puppetlabs.classifier.application
  (:require [compojure.core :refer [context]]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage.postgres :as postgres]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [clojure.tools.logging :as log]))

(def fallback-db-spec {:subprotocol "postgresql"
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

(defprotocol ClassifierService
  (shutdown [this] "Shut down the classifier."))

(defservice classifier-service
  ClassifierService

  [[:ConfigService get-config]
   [:WebserverService add-ring-handler]]

  (start [_ context]
         (let [config (get-config)
               db-spec (get config :database fallback-db-spec)
               api-prefix (get-in config [:classifier :url-prefix] "")
               app-config {:db (postgres/new-db db-spec)
                           :puppet-master (get-in config [:classifier :puppet-master])}
               app (add-url-prefix api-prefix (http/app app-config))]
           (postgres/migrate db-spec)
           (add-ring-handler app api-prefix)
           context))

  (shutdown [_] (on-shutdown)))

(defservice initdb
  [[:ConfigService get-config]
   [:ShutdownService request-shutdown]]
  (start [_ context]
         (let [config (get-config)
               db-spec (get config :database fallback-db-spec)]
           (postgres/drop-public-tables db-spec)
           (postgres/migrate db-spec)
           (request-shutdown)
           context)))
