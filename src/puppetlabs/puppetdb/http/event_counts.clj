(ns puppetlabs.puppetdb.http.event-counts
  (:require [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.http.events :as events-http]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.query-eng :refer [produce-streaming-body]]
            [puppetlabs.puppetdb.http.query :as http-q]
            [puppetlabs.puppetdb.middleware :refer [verify-accepts-json validate-query-params
                                                    wrap-with-paging-options]]
            [clojure.tools.logging :as log]
            [net.cgrand.moustache :refer [app]]))

(defn routes
  [version]
  (app
   [""]
   {:get (fn [{:keys [params globals paging-options]}]
           (when (= "puppetdb" (:product-name globals))
             (log/warn "The event-counts endpoint is experimental"
                       " and may be altered or removed in the future."))
           (let [{:strs [query summarize_by counts_filter count_by] :as query-params} params
                 query-options (merge {:counts_filter (if counts_filter (json/parse-strict-string counts_filter true))
                                       :count_by count_by
                                       :summarize_by summarize_by}
                                      (events-http/validate-distinct-options! query-params)
                                      paging-options)]
             (produce-streaming-body
              :event-counts
              version
              query
              query-options
              (:scf-read-db globals)
              (:url-prefix globals))))}))

(defn routes
  [version optional-handlers]
  (let [handlers (or optional-handlers [identity])
        query-route #(apply (partial http-q/query-route :event-counts version) %)]
    (app
      []
      (query-route handlers))))

(defn event-counts-app
  "Ring app for querying for summary information about resource events."
  [version & optional-handlers]
  (-> (routes version optional-handlers)
      verify-accepts-json
      (validate-query-params {:optional (concat ["query"
                                                 "summarize_by"
                                                 "counts_filter" "count_by"
                                                 "distinct_resources" "distinct_start_time"
                                                 "distinct_end_time"]
                                                paging/query-params)})
      wrap-with-paging-options))
