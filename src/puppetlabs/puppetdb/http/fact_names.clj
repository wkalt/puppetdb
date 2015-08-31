(ns puppetlabs.puppetdb.http.fact-names
  (:require [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.http.query :as http-q]
            [net.cgrand.moustache :refer [app]]
            [puppetlabs.puppetdb.middleware :refer [verify-accepts-json
                                                    validate-query-params
                                                    wrap-with-paging-options]]))

(defn routes
  [version]
  (app
    []
    (http-q/query-route :fact-names version identity)))

(defn fact-names-app
  [version]
  (-> (routes version)
      verify-accepts-json
      (validate-query-params {:optional (cons "query" paging/query-params)})
      wrap-with-paging-options))
