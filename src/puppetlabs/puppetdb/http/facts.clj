(ns puppetlabs.puppetdb.http.facts
  (:require [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.query-eng :refer [produce-streaming-body]]
            [net.cgrand.moustache :refer [app]]
            [puppetlabs.puppetdb.middleware :refer [verify-accepts-json validate-query-params
                                                    wrap-with-paging-options]]))

(defn routes
  [version restrict-to-active-nodes]
  (let [handler (if restrict-to-active-nodes
                  http-q/restrict-query-to-active-nodes'
                  identity)]
  (app
    []
    (http-q/query-route :facts version handler)

    [fact value &]
    (http-q/query-route :facts version
                        (comp handler
                              (partial http-q/restrict-fact-query-to-name' fact)
                              (partial http-q/restrict-fact-query-to-value' value)))

    [fact &]
    (http-q/query-route :facts version
                        (comp handler
                              (partial http-q/restrict-fact-query-to-name' fact))))))

(defn facts-app
  ([version] (facts-app version true))
  ([version restrict-to-active-nodes]
   (-> (routes version restrict-to-active-nodes)
       (validate-query-params
         {:optional (cons "query" paging/query-params)})
       wrap-with-paging-options)))
