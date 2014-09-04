(ns com.puppetlabs.puppetdb.http.fact-paths
  (:require [com.puppetlabs.puppetdb.query-eng :as qe]
            [com.puppetlabs.puppetdb.query.paging :as paging]
            [net.cgrand.moustache :refer [app]]
            [com.puppetlabs.middleware :refer [verify-accepts-json
                                               validate-query-params
                                               wrap-with-paging-options]]
            [com.puppetlabs.puppetdb.facts :refer [string-to-factpath]]))

(defn query-app
  [version]
  (app
    [&]
    {:get (comp (fn [{:keys [params globals paging-options] :as request}]
                  (qe/produce-streaming-body
                    :fact-paths
                    version
                    (params "query")
                    {:paging-options paging-options}
                    (:scf-read-db globals))))}))

(defn routes
  [query-app]
  (app
    []
    (verify-accepts-json query-app)))

(defn fact-paths-app
  [version]
  (routes
    (-> (query-app version)
        verify-accepts-json
        (validate-query-params {:optional (cons "query" paging/query-params)})
        wrap-with-paging-options)))
