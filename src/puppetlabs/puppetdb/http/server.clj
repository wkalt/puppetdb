(ns puppetlabs.puppetdb.http.server
  "REST server

   Consolidates our disparate REST endpoints into a single Ring
   application."
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.middleware :refer [wrap-with-globals
                                                    wrap-with-metrics
                                                    wrap-with-illegal-argument-catch
                                                    verify-accepts-json
                                                    make-pdb-handler]]
            [ring.util.response :as rr]
            [puppetlabs.comidi :as cmdi]
            [puppetlabs.puppetdb.http.handlers :as handlers]))

(defn- refuse-retired-api
  [version]
  (constantly
   (http/error-response
    (format "The %s API has been retired; please use v4" version)
    404)))

(defn retired-api-route
  [version]
  (cmdi/context (str "/" version)
                (cmdi/routes
                 [true (refuse-retired-api version)])))

(def routes
  (cmdi/routes
   (retired-api-route "v1")
   (retired-api-route "v2")
   (retired-api-route "v3")
   (apply cmdi/context "/v4"
          (map (fn [[route-str handler]]
                 (cmdi/context route-str (handler :v4)))
               {"" handlers/experimental-root-routes
                "/facts" handlers/facts-routes
                "/edges" handlers/edge-routes
                "/factsets" handlers/factset-routes
                "/fact-names" handlers/fact-names-routes
                "/fact-contents" handlers/fact-contents-routes
                "/fact-paths" handlers/fact-path-routes
                "/nodes" handlers/node-routes
                "/environments" handlers/environments-routes
                "/resources" handlers/resources-routes
                "/catalogs" handlers/catalog-routes
                "/events" handlers/events-routes
                "/event-counts" handlers/event-counts-routes
                "/aggregate-event-counts" handlers/agg-event-counts-routes
                "/reports" handlers/reports-routes}))))

(defn server-routes
  [get-shared-globals]
  (-> routes
      (cmdi/wrap-routes (fn [x]
                          (-> x
                              wrap-with-illegal-argument-catch
                              verify-accepts-json
                              (wrap-with-metrics (atom {}) http/leading-uris)
                              (wrap-with-globals get-shared-globals))))))
