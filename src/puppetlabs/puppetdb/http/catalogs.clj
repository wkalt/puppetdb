(ns puppetlabs.puppetdb.http.catalogs
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.query.catalogs :as c]
            [puppetlabs.puppetdb.catalogs :as cats]
            [puppetlabs.puppetdb.query-eng :refer [produce-streaming-body]]
            [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.middleware :as middleware]
            [schema.core :as s]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.middleware :refer [verify-accepts-json validate-query-params
                                                    wrap-with-paging-options]]
            [puppetlabs.puppetdb.jdbc :refer [with-transacted-connection]]
            [net.cgrand.moustache :refer [app]]))

(defn catalog-status
  "Produce a response body for a request to retrieve the catalog for `node`."
  [api-version node db]
  (if-let [catalog (with-transacted-connection db
                     (c/status api-version node))]
    (http/json-response (s/validate (c/catalog-response-schema api-version) catalog))
    (http/json-response {:error (str "Could not find catalog for " node)} http/status-not-found)))

(defn build-catalog-app
  [version entity]
  (fn [{:keys [params globals paging-options]}]
              (produce-streaming-body
                entity
                version
                (params "query")
                paging-options
                (:scf-read-db globals))))

(defn routes
  [version]
  (case version
    :v3
    (app
      [node]
      (fn [{:keys [globals]}]
        (catalog-status version node (:scf-read-db globals))))
    (app
      [""]
      {:get (build-catalog-app version :catalogs)}

      [node]
      (fn [{:keys [globals]}]
        (catalog-status version node (:scf-read-db globals))))))

(defn catalog-app
  [version]
  (case version
    :v3
  (-> (routes version)
      middleware/verify-accepts-json
      (middleware/validate-no-query-params))
  (-> (routes version)
      verify-accepts-json
      (validate-query-params
       {:optional (cons "query" paging/query-params)})
      wrap-with-paging-options)))
