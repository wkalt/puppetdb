(ns puppetdb-sync.services
  (:require [clojure.tools.logging :as log]
            [puppetlabs.trapperkeeper.core :refer [defservice main]]
            [puppetlabs.trapperkeeper.services :refer [get-services]]
            [puppetdb-sync.command :as command]
            [robert.hooke :as rh]
            [compojure.core :as compojure]
            [compojure.route :as route]))

(defservice puppetdb-sync-service
  [[:ConfigService get-in-config]
   [:WebroutingService add-ring-handler]]
  (init [this context]
    (log/info "Initializing hello webservice")
      ; Since we're using the -to versions of the below functions and are specifying
      ; server-id :foo, these will be added to the :foo server specified in the
      ; config file.
      (add-ring-handler
        (fn [req]
          {:status 200
           :headers {"Content-Type" "text/plain"}
           :body "hello world"}))))

(defn -main
  "Calls the trapperkeeper main argument to initialize tk.

   For configuration customization, we intercept the call to parse-config-data
   within TK."
  [& args]
  (apply main args))
