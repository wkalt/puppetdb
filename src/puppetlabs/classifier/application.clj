(ns puppetlabs.classifier.application
  (:require [clojure.set :refer [rename-keys]]
            [clojure.tools.logging :as log]
            [compojure.core :refer [context]]
            [puppetlabs.kitchensink.ssl :as ssl]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage.postgres :as postgres]))

(def fallback-db-spec {:subprotocol "postgresql"
                       :dbname "classifier"
                       :user "classifier"
                       :password "classifier"})

(defn- config->db-spec
  [{:keys [database]}]
  (-> (merge fallback-db-spec database)
    (rename-keys {:dbname :subname})))

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

(defn- init-ssl-context
  [{:keys [ssl-cert ssl-key ssl-ca-cert]}]
    (if (and ssl-cert ssl-key ssl-ca-cert)
      (ssl/pems->ssl-context ssl-cert ssl-key ssl-ca-cert)))

(defservice classifier-service
  ClassifierService

  [[:ConfigService get-config]
   [:WebserverService add-ring-handler]]

  (start [_ context]
         (let [config (get-config)
               db-spec (config->db-spec config)
               api-prefix (get-in config [:classifier :url-prefix] "")
               app-config {:db (postgres/new-db db-spec)
                           :puppet-master (get-in config [:classifier :puppet-master])
                           :ssl-files (select-keys (:webserver config) [:ssl-cert :ssl-key :ssl-ca-cert])
                           :ssl-context (init-ssl-context (:webserver config))}
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
               db-spec (config->db-spec config)]
           (postgres/drop-public-tables db-spec)
           (postgres/migrate db-spec)
           (request-shutdown)
           context)))
