(ns puppetlabs.puppetdb.query.catalogs
  "Catalog retrieval

   Returns a catalog in the PuppetDB JSON wire format.  For more info, see
   `documentation/api/wire_format/catalog_format.markdown`."
  (:require [puppetlabs.puppetdb.query.resources :as resources]
            [puppetlabs.puppetdb.schema :as pls]
            [puppetlabs.puppetdb.catalogs :as cats]
            [schema.core :as s]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.query :as query]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.query.paging :as paging]
            [clj-time.core :refer [now]]
            [puppetlabs.puppetdb.query-eng.engine :as qe]
            [puppetlabs.kitchensink.core :as kitchensink]))

;; v4+ functions

(def catalog-columns
  [:name
   :version
   :transaction-uuid
   :producer-timestamp
   :environment
   :hash
   :edges
   :resources])

(def row-schema
  {:version (s/maybe String)
   :hash (s/maybe String)
   :transaction_uuid (s/maybe String)
   :environment (s/maybe String)
   :name String
   :producer_timestamp (s/maybe pls/Timestamp)
   :resource (s/maybe String)
   :type (s/maybe String)
   :title (s/maybe String)
   :tags (s/maybe [String])
   :exported (s/maybe s/Bool)
   :file (s/maybe String)
   :line (s/maybe s/Int)
   :parameters (s/maybe String)
   :source_type (s/maybe String)
   :source_title (s/maybe String)
   :target_type (s/maybe String)
   :target_title (s/maybe String)
   :relationship (s/maybe String)})

(defn catalog-response-schema
  "Returns the correct schema for the `version`, use :all for the full-catalog (superset)"
  [api-version]
  (case api-version
    :v4 (assoc (cats/catalog-wireformat :v5) :hash String)
    (cats/catalog-wireformat api-version)))

(defn create-catalog-pred
  [rows]
  (let [catalog-hash (:hash (first rows))]
    (fn [row]
      (= catalog-hash (:hash row)))))

(defn collapse-resources
  [acc row]
  (let [{:keys [tags type title line parameters exported file]} row
        resource {:tags tags :type type :title title :line line
                  :parameters (json/parse-strict-string parameters true)
                  :exported exported :file file}]
    (into acc
            (-> resource
                (kitchensink/dissoc-if-nil :line :file)
                vector))))

(defn collapse-edges
  [acc row]
  (let [{:keys [source_type target_type source_title target_title relationship]} row
        edge {:source {:type source_type :title source_title}
              :target {:type target_type :title target_title}
              :relationship relationship}]
    (into acc [edge])))

(pls/defn-validated collapse-catalog :- (catalog-response-schema :v4)
  [version :- s/Keyword
   catalog-rows :- [row-schema]]
  (let [first-row (kitchensink/mapkeys jdbc/underscores->dashes (first catalog-rows))
        resources (->> catalog-rows
                       (filter #(not (nil? (:resource %))))
                       (reduce collapse-resources #{})
                       (into []))
        edges (->> catalog-rows
                   (filter #(not (nil? (:source_type %))))
                   (reduce collapse-edges #{})
                   (into []))]
    (assoc (select-keys first-row [:name :version :environment :hash
                                   :transaction-uuid :producer-timestamp])
           :edges edges :resources resources)))

(pls/defn-validated structured-data-seq
  "Produce a lazy seq of catalogs from a list of rows ordered by catalog hash"
  [version :- s/Keyword
   rows]
  (when (seq rows)
    (let [[catalog-rows more-rows] (split-with (create-catalog-pred rows) rows)]
      (cons (collapse-catalog version catalog-rows)
            (lazy-seq (structured-data-seq version more-rows))))))

(defn catalogs-sql
  "Return a vector with the catalogs SQL query string as the first element,
  parameters needed for that query as the rest."
  [operators query]
  (if query
    (let [[subselect & params] (query/catalog-query->sql operators query)
          sql (format "SELECT catalogs.version,
                       catalogs.transaction_uuid,
                       catalogs.environment,
                       catalogs.name,
                       catalogs.hash,
                       catalogs.producer_timestamp,
                       catalogs.resource,
                       catalogs.type,
                       catalogs.title,
                       catalogs.tags,
                       catalogs.exported,
                       catalogs.file,
                       catalogs.line,
                       catalogs.parameters,
                       catalogs.source_type,
                       catalogs.source_title,
                       catalogs.target_type,
                       catalogs.target_title,
                       catalogs.relationship
                       FROM (%s) catalogs" subselect)]
      (apply vector sql params))
    ["select c.catalog_version as version,
      c.certname,
      c.hash,
      transaction_uuid,
      e.name as environment,
      c.certname as name,
      c.producer_timestamp,
      cr.resource,
      cr.type,
      cr.title,
      cr.tags,
      cr.exported,
      cr.file,
      cr.line,
      cr.parameters,
      null as source_type,
      null as source_title,
      null as target_type,
      null as target_title,
      null as relationship
      from catalogs c
      left outer join environments e on c.environment_id = e.id
      left outer join catalog_resources cr ON c.id=cr.catalog_id
      inner join resource_params_cache rpc on rpc.resource=cr.resource

      UNION ALL

      select c.catalog_version as version,
      c.certname,
      c.hash,
      transaction_uuid,
      e.name as environment,
      c.certname as name,
      c.producer_timestamp,
      null as resource,
      null as type,
      null as title,
      null as tags,
      null as exported,
      null as file,
      null as line,
      null as parameters,
      sources.type as source_type,
      sources.title as source_title,
      targets.type as target_type,
      targets.title as target_title,
      edges.type as relationship
      FROM catalogs c
      left outer join environments e on c.environment_id = e.id
      INNER JOIN edges ON c.certname = edges.certname
      INNER JOIN catalog_resources sources
      ON edges.source = sources.resource AND sources.catalog_id=c.id
      INNER JOIN catalog_resources targets
      ON edges.target = targets.resource AND targets.catalog_id=c.id
      order by certname" ]))

(defn query->sql
  "Converts a vector-structured `query` to a corresponding SQL query which will
  return nodes matching the `query`."
  ([version query]
   (query->sql version query {}))
  ([version query paging-options]
   {:pre  [((some-fn nil? sequential?) query)]
    :post [(map? %)
           (jdbc/valid-jdbc-query? (:results-query %))
           (or (not (:count? paging-options))
               (jdbc/valid-jdbc-query? (:count-query %)))]}
   (paging/validate-order-by! catalog-columns paging-options)
   (case version
     :v3
     (let [operators (query/catalog-operators version)
           [sql & params] (catalogs-sql operators query)]
       (conj {:results-query (apply vector (jdbc/paged-sql sql paging-options) params)}
             (when (:count? paging-options)
               [:count-query (apply vector (jdbc/paged-sql sql paging-options) params)])))
     (qe/compile-user-query->sql
       qe/catalog-query query paging-options))))

(defn wrap-catalog-for-v3
  [catalog]
  (-> catalog
      (dissoc :environment :producer-timestamp :hash)
      cats/old-wire-format-schema))

(pls/defn-validated munge-result-rows
  "Reassemble rows from the database into the final expected format."
  [version :- s/Keyword
   projections]
  (let [post-extract (case version
                       :v3 (comp wrap-catalog-for-v3
                                 #(utils/assoc-when % :api_version 1))
                       (qe/basic-project projections))]
    (fn [rows]
      (if (empty? rows)
        []
        (map post-extract (structured-data-seq version rows))))))

(defn query-catalogs
  "Search for catalogs satisfying the given SQL filter."
  [version query-sql]
  {:pre  [(map? query-sql)
          (jdbc/valid-jdbc-query? (:results-query query-sql))]
   :post [(map? %)
          (sequential? (:result %))]}
  (let [{[sql & params] :results-query
         count-query    :count-query
         projections    :projections} query-sql
         result {:result (query/streamed-query-result
                          version sql params
                          (comp doall (munge-result-rows version projections))
                          false)}]
    (if count-query
      (assoc result :count (jdbc/get-result-count count-query))
      result)))

(defn status
  [version node]
  {:pre [string? node]}
  (let [sql (query->sql version ["=" "name" node])
        results (:result (query-catalogs version sql))]
    (first results)))
