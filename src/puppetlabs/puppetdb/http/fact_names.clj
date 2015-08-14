(ns puppetlabs.puppetdb.http.fact-names
  (:require [puppetlabs.puppetdb.query.facts :as f]
            [puppetlabs.puppetdb.query-eng :refer [produce-streaming-body]]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.jdbc :refer [with-transacted-connection]]
            [net.cgrand.moustache :refer [app]]
            [puppetlabs.puppetdb.middleware :refer [verify-accepts-json validate-query-params wrap-with-paging-options validate-no-query-params]]
            [puppetlabs.puppetdb.http :refer [query-result-response]]))

(defn query-app
  ([version]
   (app
     [&]
     {:get (fn [{:keys [params globals paging-options]}]
             (produce-streaming-body
               :fact-names
               version
               (params "query")
               paging-options
               (:scf-read-db globals)
               (:url-prefix globals)))})))

(defn routes
  [query-app]
  (app
    []
    (verify-accepts-json query-app)))

(defn fact-names-app
  [version]
  (routes
    (-> (query-app version)
        verify-accepts-json
        (validate-query-params {:optional (cons "query" paging/query-params)})
        wrap-with-paging-options)))
