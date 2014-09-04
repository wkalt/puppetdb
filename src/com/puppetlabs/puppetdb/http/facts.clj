(ns com.puppetlabs.puppetdb.http.facts
  (:require [com.puppetlabs.puppetdb.http.query :as http-q]
            [com.puppetlabs.puppetdb.query.paging :as paging]
            [com.puppetlabs.http :as pl-http]
            [com.puppetlabs.puppetdb.query.facts :as facts]
            [com.puppetlabs.cheshire :as json]
            [com.puppetlabs.puppetdb.query :as query]
            [com.puppetlabs.puppetdb.query-eng :as qe]
            [net.cgrand.moustache :refer [app]]
            [com.puppetlabs.middleware :refer [verify-accepts-json validate-query-params
                                               wrap-with-paging-options]]
            [com.puppetlabs.jdbc :as jdbc]
            [com.puppetlabs.puppetdb.http :as http]))

(defn query-app
  [version]
  (app
    [&]
    {:get (comp (fn [{:keys [params globals paging-options] :as request}]
                  (qe/produce-streaming-body
                    :facts
                    version
                    (params "query")
                    {:paging-options paging-options}
                    (:scf-read-db globals)))
                http-q/restrict-query-to-active-nodes)}))

(defn build-facts-app
  [query-app]
  (app
    []
    (verify-accepts-json query-app)

    [fact value &]
    (comp query-app
          (partial http-q/restrict-fact-query-to-name fact)
          (partial http-q/restrict-fact-query-to-value value))

    [fact &]
    (comp query-app
          (partial http-q/restrict-fact-query-to-name fact))))

(defn facts-app
  [version]
  (case version
    :v1 (throw (IllegalArgumentException. "No support for v1 for facts end-point"))
    :v2 (build-facts-app
         (-> (query-app version)
             (validate-query-params
               {:optional ["query"]})))
    (build-facts-app
     (-> (query-app version)
         (validate-query-params
           {:optional (cons "query" paging/query-params)})
         wrap-with-paging-options))))
