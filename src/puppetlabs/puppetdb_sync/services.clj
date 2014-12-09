(ns puppetlabs.puppetdb-sync.services
  (:require [clojure.tools.logging :as log]
            [puppetlabs.trapperkeeper.services :refer [get-services]]
            [compojure.core :as compojure]
            [ring.util.request :as request]
            [puppetlabs.puppetdb-sync.command :as command]
            [puppetlabs.puppetdb.cheshire :as json]  
            [puppetlabs.trapperkeeper.core :refer [defservice main]]))

(defn app
  [req]
  (let [request-string (slurp (:body req))
        request-map (json/parse-string request-string true)]
    (command/puppetdb-sync* (:payload request-map))
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (format "hello worlds")}))

(defservice puppetdb-sync-service
  [[:ConfigService get-in-config]
   [:WebroutingService add-ring-handler]]
  (start [this context]
         (let [port (get-in-config [ :webserver :default :port])]
         (log/info "Initializing hello webservice")
         (add-ring-handler this app)
         context)))

(defn -main
  "Calls the trapperkeeper main argument to initialize tk."
  [& args]
  (apply main args))
