(ns puppetlabs.puppetdb.http.catalogs
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.catalogs :as catalogs]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.query-eng :as eng :refer [produce-streaming-body]]
            [puppetlabs.puppetdb.middleware :as middleware]
            [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.http.edges :as edges]
            [puppetlabs.puppetdb.http.resources :as resources]
            [schema.core :as s]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.middleware :refer [verify-accepts-json validate-query-params
                                                    wrap-with-paging-options wrap-with-parent-check]]
            [puppetlabs.puppetdb.jdbc :refer [with-transacted-connection]]
            [net.cgrand.moustache :refer [app]]))

(defn catalog-status
  "Produce a response body for a request to retrieve the catalog for `node`."
  [api-version node db url-prefix]
  (let [catalog (first
                 (eng/stream-query-result :catalogs
                                          api-version
                                          ["=" "certname" node]
                                          {}
                                          db
                                          url-prefix))]
    (if catalog
      (http/json-response (s/validate catalogs/catalog-query-schema catalog))
      (http/status-not-found-response "catalog" node))))

(defn routes
  [version optional-handlers]
  (let [handlers (or optional-handlers [identity])
        query-route #(apply (partial http-q/query-route :catalogs version) %)]
  (app
    []
    (query-route handlers)

    [node]
    (fn [{:keys [globals]}]
      (catalog-status version node (:scf-read-db globals)
                      (str (:url-prefix globals))))

    [node "edges" &]
    (-> (edges/edges-app version false (partial http-q/restrict-query-to-node node))
        (wrap-with-parent-check version :catalog node))

    [node "resources" &]
    (-> (resources/resources-app version false (partial http-q/restrict-query-to-node node))
        (wrap-with-parent-check version :catalog node)))))

(defn catalog-app
  [version & optional-handlers]
  (-> (routes version optional-handlers)
      verify-accepts-json
      (validate-query-params
        {:optional (cons "query" paging/query-params)})
      wrap-with-paging-options))
