(ns puppetlabs.classifier.main
  (:require [clojure.set :refer [rename-keys]]
            [clojure.tools.logging :as log]
            [compojure.core :refer [context]]
            [overtone.at-at :as at-at]
            [ring.util.servlet :refer [servlet]]
            [puppetlabs.certificate-authority.core :as ssl]
            [puppetlabs.kitchensink.json :refer [add-common-json-encoders!]]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [puppetlabs.trapperkeeper.services :refer [get-service service-context]]
            [puppetlabs.classifier.application.default :refer [default-application]]
            [puppetlabs.classifier.application.permissioned :refer [app-with-permissions]]
            [puppetlabs.classifier.application.permissioned.rbac :refer [rbac-service-permissions]]
            [puppetlabs.classifier.class-updater :refer [update-classes-and-log-errors!]]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.http.middleware :refer [wrap-authn-errors]]
            [puppetlabs.classifier.storage.postgres :as postgres]))

(def default-db-spec {:subprotocol "postgresql"
                       :subname "classifier"
                       :user "classifier"
                       :password "classifier"})

(def default-sync-period (* 15 60))

(defn- config->db-spec
  [{{:keys [database]} :classifier}]
  (merge default-db-spec database))

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

(defn- init-ssl-context
  [{:keys [ssl-cert ssl-key ssl-ca-cert]}]
    (if (and ssl-cert ssl-key ssl-ca-cert)
      (ssl/pems->ssl-context ssl-cert ssl-key ssl-ca-cert)))

(defservice classifier-service
  [[:ConfigService get-config]
   RbacAuthzService
   [:RbacAuthnMiddleware wrap-authenticated]
   [:WebroutingService add-ring-handler add-servlet-handler get-route]]
  (start [this context]
         (let [config (get-config)
               db-spec (config->db-spec config)
               db (postgres/new-db db-spec)
               api-prefix (get-route this)
               app-config {:db db
                           :api-prefix api-prefix
                           :puppet-master (get-in config [:classifier :puppet-master])
                           :client-ssl-context (or (init-ssl-context (get config :classifier))
                                                   (init-ssl-context
                                                     (get-in config [:webserver :classifier])))}
               default-app (default-application app-config)
               sync-period (get-in config [:classifier :synchronization-period] default-sync-period)
               job-pool (at-at/mk-pool)]
           (postgres/migrate db-spec)
           (add-common-json-encoders!)
           (when (pos? sync-period)
             (at-at/every (* sync-period 1000)
                          #(update-classes-and-log-errors! app-config default-app)
                          job-pool))
           (if (= (get-in config [:classifier :access-control]) false)
             (let [handler (add-url-prefix api-prefix (http/api-handler default-app))]
               (add-ring-handler this handler)
               (log/warn "Access-control explicitly disabled in configuration, running without it"))
             (let [authz-service (get-service this :RbacAuthzService)
                   perm-fns (rbac-service-permissions authz-service)
                   permd-app (app-with-permissions default-app perm-fns)
                   permd-handler (->> (http/api-handler permd-app)
                                   (add-url-prefix api-prefix)
                                   wrap-authenticated
                                   wrap-authn-errors)]
               (add-servlet-handler this (servlet permd-handler))
               (log/info "Access-control enabled")))
           (assoc context :job-pool job-pool)))

  (stop [this _]
        (let [{:keys [job-pool]} (service-context this)]
          (when job-pool
            (at-at/stop-and-reset-pool! job-pool)))))

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
