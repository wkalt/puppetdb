(ns puppetlabs.puppetdb-sync.services
  (:require [clojure.tools.logging :as log]
            [puppetlabs.trapperkeeper.services :refer [get-services]]
            [compojure.core :as compojure]
            [puppetlabs.trapperkeeper.core :refer [defservice main]]))

(defn app
  [req]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body "hello world"})   

(defservice puppetdb-sync-service
  [[:ConfigService get-in-config]
   [:WebroutingService add-ring-handler]]
  (start [this context]
     (log/info "Initializing hello webservice")
     (add-ring-handler this app)
     context))

(defn -main
  "Calls the trapperkeeper main argument to initialize tk."
  [& args]
  (apply main args))
