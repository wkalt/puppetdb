(ns puppetlabs.puppetdb.query.events
  "SQL/query-related functions for events"
  (:require [clojure.string :as string]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.query :as query]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.query-eng.engine :as qe]
            [puppetlabs.puppetdb.schema :as pls]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [schema.core :as s]))

;; MUNGE

(pls/defn-validated munge-result-rows
  "Returns a function that munges the resulting rows ready for final
   presentation."
  [_ _]
  (fn [rows]
    (map (comp #(utils/update-when % [:old_value] json/parse-string)
               #(utils/update-when % [:new_value] json/parse-string))
         rows)))

;; QUERY

(defn default-select
  "Build the default SELECT statement that we use in the common case.  Returns
  a two-item vector whose first value is the SQL string and whose second value
  is a list of parameters for the SQL query."
  [select-fields where params]
  {:pre [(string? select-fields)
         (string? where)
         ((some-fn nil? sequential?) params)]
   :post [(vector? %)
          (= 2 (count %))
          (string? (first %))
          ((some-fn nil? sequential?) (second %))]}
  [(format
    "SELECT %s
         FROM resource_events
         JOIN reports ON resource_events.report_id = reports.id
         LEFT OUTER JOIN environments on reports.environment_id = environments.id
         WHERE %s"
    select-fields
    where)
   params])

;;;(defn with-latest-events
;;;  "CTE to wrap unioned queries when distinct_resources is used"
;;;  [query]
;;;  (format
;;;    "WITH latest_events AS
;;;           (SELECT certnames.certname,
;;;                   resource_events.resource_type,
;;;                   resource_events.resource_title,
;;;                   resource_events.property,
;;;                   MAX(resource_events.timestamp) AS timestamp
;;;                   FROM resource_events
;;;                   JOIN certnames ON resource_events.certname_id = certnames.id
;;;                   WHERE resource_events.timestamp >= ?
;;;                   AND resource_events.timestamp <= ?
;;;                   GROUP BY certname, resource_type, resource_title, property)
;;;           %s" query))

(defn with-latest-events
  "CTE to wrap unioned queries when distinct_resources is used"
  [query]
  (format
    "WITH latest_events AS
           (SELECT certname_id,
                   resource_type collate \"C\" as resource_type,
                   resource_title collate \"C\" as resource_title,
                   property collate \"C\" as property,
                   MAX(resource_events.timestamp) AS timestamp
                   FROM resource_events
                   WHERE resource_events.timestamp >= ?
                   AND resource_events.timestamp <= ?
                   GROUP BY certname_id, resource_type collate \"C\", resource_title collate \"C\", property collate \"C\")
           %s" query))


;;;(defn with-latest-events
;;;  "CTE to wrap unioned queries when distinct_resources is used"
;;;  [query]
;;;  (format
;;;    "WITH latest_events AS
;;;     (with t as
;;;     (select certname,resource_type,resource_title,property,timestamp,row_number() over
;;;     (partition by (certname_id,resource_type,resource_title,property) order by timestamp desc) as rn
;;;     from resource_events
;;;     inner join certnames on resource_events.certname_id=certnames.id
;;;     where timestamp >= ?
;;;     and timestamp <= ?)
;;;     select certname,resource_type,resource_title,property,timestamp
;;;     from t where rn=1)
;;;     %s" query))

(defn distinct-select
  "Build the SELECT statement that we use in the `distinct-resources` case (where
  we are filtering out multiple events on the same resource on the same node).
  Returns a two-item vector whose first value is the SQL string and whose second value
  is a list of parameters for the SQL query."
  [select-fields where params distinct-start-time distinct-end-time]
  {:pre [(string? select-fields)
         (string? where)
         ((some-fn nil? sequential?) params)]
   :post [(vector? %)
          (= 2 (count %))
          (string? (first %))
          ((some-fn nil? sequential?) (second %))]}
  [(format
    "SELECT %s
         FROM resource_events
         JOIN reports ON resource_events.report_id = reports.id
         LEFT OUTER JOIN environments ON reports.environment_id = environments.id
         JOIN latest_events
              ON reports.certname_id = latest_events.certname_id
               AND resource_events.resource_type = latest_events.resource_type
               AND resource_events.resource_title = latest_events.resource_title
               AND ((resource_events.property = latest_events.property) OR
                    (resource_events.property IS NULL AND latest_events.property IS NULL))
               AND resource_events.timestamp = latest_events.timestamp
         WHERE %s"
    select-fields
    where)
   (concat [distinct-start-time distinct-end-time] params)])

(defn legacy-query->sql
  "Compile a resource event `query` into an SQL expression."
  [will-union? version query-options query paging-options]
  {:pre  [(or (sequential? query) (nil? query))
          (let [distinct-options [:distinct_resources? :distinct_start_time :distinct_end_time]]
            (or (not-any? #(contains? query-options %) distinct-options)
                (every? #(contains? query-options %) distinct-options)))]
   :post [(map? %)
          (jdbc/valid-jdbc-query? (:results-query %))
          (or
           (not (:count? paging-options))
           (jdbc/valid-jdbc-query? (:count-query %)))]}
  (let [{:keys [where params]}  (query/compile-term (query/resource-event-ops version) query)
        select-fields           (string/join ", "
                                             (map
                                              (fn [[column [table alias]]]
                                                (str (if (= column "hash")
                                                       (sutils/sql-hash-as-str (str table "." column))
                                                       (str table "." column))
                                                     (if alias (format " AS %s" alias) "")))
                                              query/event-columns))
        [sql params]            (if (:distinct_resources? query-options)
                                  (distinct-select select-fields where params
                                                   (:distinct_start_time query-options)
                                                   (:distinct_end_time query-options))
                                  (default-select select-fields where params))
        paged-select (cond-> (jdbc/paged-sql sql paging-options)
                       ;; if the caller is aggregate-event-counts, this is one
                       ;; of potentially three unioned queries, and
                       ;; with-latest-events must be applied higher up
                       (not will-union?) with-latest-events)
        result {:results-query (apply vector paged-select params)}]
    (if (:count? paging-options)
      (let [count-sql (jdbc/count-sql (with-latest-events sql))]
        (assoc result :count-query (apply vector count-sql params)))
      result)))

(defn query->sql
  "Compile a resource event `query` into an SQL expression."
  ([version query [query-options paging-options]]
   (query->sql false version query [query-options paging-options]))
  ([will-union? version query [query-options paging-options]]
   {:pre  [(or (sequential? query) (nil? query))
           (let [distinct-options [:distinct_resources? :distinct_start_time :distinct_end_time]]
             (or (not-any? #(contains? query-options %) distinct-options)
                 (every? #(contains? query-options %) distinct-options)))]
    :post [(map? %)
           (jdbc/valid-jdbc-query? (:results-query %))
           (or
             (not (:count? paging-options))
             (jdbc/valid-jdbc-query? (:count-query %)))]}
   (paging/validate-order-by! (map keyword (keys query/event-columns)) paging-options)
   (if (:distinct_resources? query-options)
     ;; The new query engine does not support distinct-resources yet, so we
     ;; fall back to the old
     (legacy-query->sql will-union? version query-options query paging-options)
     (qe/compile-user-query->sql qe/report-events-query query paging-options))))
