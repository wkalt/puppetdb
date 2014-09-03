(ns com.puppetlabs.puppetdb.query.reports
  (:require [puppetlabs.kitchensink.core :as kitchensink]
            [clojure.string :as string]
            [com.puppetlabs.puppetdb.http :refer [remove-status v4?]]
            [clojure.core.match :refer [match]]
            [com.puppetlabs.jdbc :as jdbc]
            [com.puppetlabs.puppetdb.query :as query]
            [com.puppetlabs.puppetdb.query.paging :as paging]))

(def report-columns
  [:hash
   :certname
   :puppet-version
   :report-format
   :configuration-version
   :start-time
   :end-time
   :receive-time
   :transaction-uuid
   :environment
   :status])

(defn query->sql
  "Converts a vector-structured `query` to a corresponding SQL query which will
  return nodes matching the `query`."
  ([version query]
     (query->sql version query {}))
  ([version query paging-options]
     {:pre  [((some-fn nil? sequential?) query)]
      :post [(map? %)
             (jdbc/valid-jdbc-query? (:results-query %))
             (or
              (not (:count? paging-options))
              (jdbc/valid-jdbc-query? (:count-query %)))]}
       (let [operators (query/report-ops version)
             [sql & params] (query/report-query->sql version operators query)
             paged-select (jdbc/paged-sql sql paging-options)
             result {:results-query (apply vector paged-select params)}]
         (if (:count? paging-options)
           (assoc result :count-query (apply vector (jdbc/count-sql sql) params))
           result))))

(defn munge-result-rows
  "Munge the result rows so that they will be compatible with the version
  specified API specification"
  [version]
  (fn [rows] (map (comp #(kitchensink/mapkeys jdbc/underscores->dashes %)
                       #(query/remove-environment % version)
                       #(remove-status % version))
                 rows)))

(defn query-reports
  "Queries reports and unstreams, used mainly for testing.

  This wraps the existing streaming query code but returns results
  and count (if supplied)."
  [version query-sql]
  {:pre [(map? query-sql)]}
  (let [{[sql & params] :results-query
         count-query    :count-query} query-sql
         result {:result (query/streamed-query-result
                          version sql params
                          ;; The doall simply forces the seq to be traversed
                          ;; fully.
                          (comp doall (munge-result-rows version)))}]
    (if count-query
      (assoc result :count (jdbc/get-result-count count-query))
      result)))


(defn is-latest-report?
  "Given a node and a report hash, return `true` if the report is the most recent one for the node,
  and `false` otherwise."
  [node report-hash]
  {:pre  [(string? node)
          (string? report-hash)]
   :post [(kitchensink/boolean? %)]}
  (= 1 (count (jdbc/query-to-vec
                ["SELECT report FROM latest_reports
                    WHERE certname = ? AND report = ?"
                  node report-hash]))))
