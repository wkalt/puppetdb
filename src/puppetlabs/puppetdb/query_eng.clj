(ns puppetlabs.puppetdb.query-eng
  (:import [org.postgresql.util PGobject])
  (:require [clojure.core.match :as cm]
            [clojure.java.jdbc :as sql]
            [clojure.tools.logging :as log]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.query.paging :as paging]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.query.aggregate-event-counts :as aggregate-event-counts]
            [puppetlabs.puppetdb.query.edges :as edges]
            [puppetlabs.puppetdb.query.events :as events]
            [puppetlabs.puppetdb.query.event-counts :as event-counts]
            [puppetlabs.puppetdb.query.facts :as facts]
            [puppetlabs.puppetdb.query.fact-contents :as fact-contents]
            [puppetlabs.puppetdb.query.resources :as resources]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.schema :as pls]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.query-eng.engine :as eng]
            [schema.core :as s]))

(def entity-fn-idx
  (atom
    {:aggregate-event-counts {:munge aggregate-event-counts/munge-result-rows
                              :rec nil}
     :event-counts {:munge event-counts/munge-result-rows
                    :rec nil}
     :facts {:munge facts/munge-result-rows
             :rec eng/facts-query}
     :fact-contents {:munge fact-contents/munge-result-rows
                     :rec eng/fact-contents-query}
     :fact-paths {:munge facts/munge-path-result-rows
                  :rec eng/fact-paths-query}
     :fact-names {:munge facts/munge-name-result-rows
                  :rec eng/fact-names-query}
     :factsets {:munge (constantly identity)
                :rec eng/factsets-query}
     :catalogs {:munge (constantly identity)
                :rec eng/catalog-query}
     :nodes {:munge (constantly identity)
             :rec eng/nodes-query}
     :environments {:munge (constantly identity)
                    :rec eng/environments-query}
     :events {:munge events/munge-result-rows
              :rec eng/report-events-query}
     :edges {:munge edges/munge-result-rows
             :rec eng/edges-query}
     :reports {:munge (constantly identity)
               :rec eng/reports-query}
     :report-metrics {:munge (constantly (comp :metrics first))
                      :rec eng/report-metrics-query}
     :report-logs {:munge (constantly (comp :logs first))
                   :rec eng/report-logs-query}
     :resources {:munge resources/munge-result-rows
                 :rec eng/resources-query}}))

(defn get-munge
  [entity]
  (if-let [munge-result (get-in @entity-fn-idx [entity :munge])]
    munge-result
    (throw (IllegalArgumentException.
             (format "Invalid entity '%s' in query"
                     (utils/dashes->underscores (name entity)))))))

(defn orderable-columns
  [query-rec]
  (vec
   (for [[projection-key projection-value] (:projections query-rec)
         :when (and (not= projection-key "value")
                    (:queryable? projection-value))]
     (keyword projection-key))))

(defn query->sql
  "Converts a vector-structured `query` to a corresponding SQL query which will
   return nodes matching the `query`."
  [query entity version paging-options]
  {:pre  [((some-fn nil? sequential?) query)]
   :post [(map? %)
          (jdbc/valid-jdbc-query? (:results-query %))
          (or (not (:include_total paging-options))
              (jdbc/valid-jdbc-query? (:count-query %)))]}

  (cond
    (= :aggregate-event-counts entity)
    (aggregate-event-counts/query->sql version query paging-options)

    (= :event-counts entity)
    (event-counts/query->sql version query paging-options)

    (and (= :events entity) (:distinct_resources paging-options))
    (events/legacy-query->sql false version query paging-options)

    :else
    (let [query-rec (get-in @entity-fn-idx [entity :rec])
          columns (orderable-columns query-rec)]
      (paging/validate-order-by! columns paging-options)
      (eng/compile-user-query->sql query-rec query paging-options))))

(defn get-munge-fn
  [entity version paging-options url-prefix]
  (let [munge-fn (get-munge entity)]
    (case entity
      :event-counts
      (munge-fn (:summarize_by paging-options) version url-prefix)

      (munge-fn version url-prefix))))

(def query-options-schema
  {:scf-read-db s/Any
   :url-prefix String
   (s/optional-key :warn-experimental) Boolean
   (s/optional-key :pretty-print) Boolean})

(pls/defn-validated stream-query-result
  "Given a query, and database connection, return a Ring response with the query
   results."
  ([version query paging-options options]
   ;; We default to doall because tests need this for the most part
   (stream-query-result version query paging-options options doall))
  ([version :- s/Keyword
    query
    paging-options
    options :- query-options-schema
    row-fn]
   (let [{:keys [scf-read-db url-prefix warn-experimental pretty-print]
          :or {warn-experimental true
               pretty-print false}} options
         {:keys [remaining-query entity]} (eng/parse-query-context version query warn-experimental)
         munge-fn (get-munge-fn entity version paging-options url-prefix)]
     (jdbc/with-transacted-connection scf-read-db
       (let [{:keys [results-query]}
             (query->sql remaining-query entity version paging-options)]
         (jdbc/with-query-results-cursor results-query (comp row-fn munge-fn)))))))

(pls/defn-validated produce-streaming-body
  "Given a query, and database connection, return a Ring response with
   the query results. query-map is a clojure map of the form
   {:query ['=','certname','foo'] :order_by [{'field':'foo'}]...}
   If the query can't be parsed, a 400 is returned."
  [version :- s/Keyword
   query-map
   options :- query-options-schema]
  (let [{:keys [scf-read-db url-prefix warn-experimental pretty-print]
         :or {warn-experimental true
              pretty-print false}} options
        query (:query query-map)
        {:keys [remaining-query entity paging-clauses]} (eng/parse-query-context
                                                          version query warn-experimental)
        query-options (merge (dissoc query-map :query) paging-clauses)]
    (try
      (jdbc/with-transacted-connection scf-read-db
        (let [munge-fn (get-munge-fn entity version query-options url-prefix)
              {:keys [results-query count-query]} (-> remaining-query
                                                      json/coerce-from-json
                                                      (query->sql entity version query-options))
              query-error (promise)
              resp (http/streamed-response
                    buffer
                    (try (jdbc/with-transacted-connection scf-read-db
                           (jdbc/with-query-results-cursor
                             results-query (comp #(http/stream-json % buffer pretty-print)
                                                 #(do (when-not (instance? PGobject %)
                                                        (first %)) (deliver query-error nil) %)
                                                 munge-fn)))
                         (catch java.sql.SQLException e
                           (deliver query-error e))))]
          (if @query-error
            (throw @query-error)
            (cond-> (http/json-response* resp)
                    count-query (http/add-headers {:count (jdbc/get-result-count count-query)})))))
      (catch com.fasterxml.jackson.core.JsonParseException e
        (log/errorf e (str "Error executing query '%s'"
                           "with query-options '%s'. Returning a 400 error code.")
                    (name entity) query query-options)
        (http/error-response e))
      (catch IllegalArgumentException e
        (log/errorf e (str "Error executing query '%s'"
                           "with query-options '%s'. Returning a 400 error code.")
                    (name entity) query query-options)
        (http/error-response e))
      (catch org.postgresql.util.PSQLException e
        (if (= (.getSQLState e) "2201B")
          (do (log/debug e "Caught PSQL processing exception")
              (http/error-response (.getMessage e)))
          (throw e))))))

(pls/defn-validated object-exists? :- s/Bool
  "Returns true if an object exists."
  [type :- s/Keyword
   id :- s/Str]
  (let [check-sql (case type
                    :catalog "SELECT 1
                              FROM certnames
                              INNER JOIN catalogs
                                ON catalogs.certname = certnames.certname
                              WHERE certnames.certname=?"
                    :node "SELECT 1
                           FROM certnames
                           WHERE certname=? "
                    :report (str "SELECT 1
                                  FROM reports
                                  WHERE " (sutils/sql-hash-as-str "hash") "=?
                                  LIMIT 1")
                    :environment "SELECT 1
                                  FROM environments
                                  WHERE environment=?"
                    :factset "SELECT 1
                              FROM certnames
                              INNER JOIN factsets
                              ON factsets.certname = certnames.certname
                              WHERE certnames.certname=?")]
    (jdbc/query-with-resultset [check-sql id]
                               (comp boolean seq sql/result-set-seq))))
