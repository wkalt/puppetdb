(ns puppetlabs.puppetdb.query.catalogs
  "Catalog retrieval

   Returns a catalog in the PuppetDB JSON wire format.  For more info, see
   `documentation/api/wire_format/catalog_format.markdown`."
  (:require [puppetlabs.puppetdb.query.resources :as resources]
            [puppetlabs.puppetdb.schema :as pls]
            [puppetlabs.puppetdb.catalogs :as cats]
            [schema.core :as s]
            [puppetlabs.puppetdb.http :as http]
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
  {:version String
   :hash String
   :transaction_uuid (s/maybe String)
   :environment (s/maybe String)
   :name String
   :producer_timestamp (s/maybe pls/Timestamp)
   :resource String
   :type String
   :title String
   :tags [String]
   :exported s/Bool
   :file (s/maybe String)
   :line (s/maybe s/Int)
   :parameters String
   :source_type String
   :source_title String
   :target_type String
   :target_title String
   :relationship String})

(def resource-schema
  {:tags [String]
   :type String
   :title String
   (s/optional-key :line) (s/maybe s/Int)
   (s/optional-key :file) (s/maybe String)
   :parameters {s/Any s/Any}
   :exported s/Bool})

(def edge-schema
  {:source {:type String :title String}
   :target {:type String :title String}
   :relationship String})

(def catalog-schema
  {:name String
   :hash String
   :version String
   :environment (s/maybe String)
   :transaction-uuid (s/maybe String)
   :producer-timestamp (s/maybe pls/Timestamp)
   :resources [resource-schema]
   :edges [edge-schema]})

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

(pls/defn-validated collapse-resources :- #{resource-schema}
  [acc row]
  (let [{:keys [tags type title line parameters exported file]} row
        resource {:tags tags :type type :title title
                  :line line :parameters (json/parse-strict-string parameters true)
                  :exported exported :file file}]
    (into acc
            (-> resource
                (kitchensink/dissoc-if-nil :line :file)
                vector))))

(pls/defn-validated collapse-edges :- #{edge-schema}
  [acc row]
  (let [{:keys [source_type target_type source_title target_title relationship]} row
        edge {:source {:type source_type :title source_title}
              :target {:type target_type :title target_title}
              :relationship relationship}]
    (into acc [edge])))

(pls/defn-validated collapse-catalog :- catalog-schema
  [version :- s/Keyword
   catalog-rows :- [row-schema]]
  (let [first-row (kitchensink/mapkeys jdbc/underscores->dashes (first catalog-rows))
        resources (into [] (reduce collapse-resources #{} catalog-rows))
        edges (into [] (reduce collapse-edges #{} catalog-rows))]
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

(pls/defn-validated munge-result-rows
  "Reassemble rows from the database into the final expected format."
  [version :- s/Keyword
   projections]
  (fn [rows]
    (if (empty? rows)
      []
      (map (qe/basic-project projections) (structured-data-seq version rows)))))

(defn munge-result-rows-for-node
  "Reassemble rows from the database into the final expected format."
  [node]
  (fn [version projections]
    (fn [rows]
      (if (empty? rows)
        {:error (str "Could not find catalog for " node)}
        (first (map (qe/basic-project projections) (structured-data-seq version rows)))))))

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
   (paging/validate-order-by! catalog-columns paging-options)
   (qe/compile-user-query->sql
     qe/catalog-query query paging-options)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; v2-v3 catalog query functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-catalog-info
  "Given a node name, return a map of Puppet catalog information
  for the most recent catalog that we've seen for that node.
  Returns `nil` if no catalogs are found for the node.
  The map contains the following data:
    - `:catalog-version`
    - `:transaction-uuid`, which may be nil"
  [node]
  {:pre  [(string? node)]
   :post [((some-fn nil? map?) %)]}
  (let [query (str "SELECT catalog_version as version, transaction_uuid as \"transaction-uuid\", "
                   "e.name as environment, COALESCE(c.api_version, 1) as api_version, hash,"
                   "producer_timestamp as \"producer-timestamp\""
                   "FROM catalogs c left outer join environments e on c.environment_id = e.id "
                   "WHERE certname = ?")]
    (first (jdbc/query-to-vec query node))))

(defn resource-to-wire-format
  "Given a resource as returned by our resource database query functions,
  munges the resource into a map that complies with our wire format.  This
  basically involves removing extraneous fields (`certname`, the puppetdb resource
  hash), and removing the `file` and `line` fields if they are `nil`."
  [resource]
  {:pre  [(map? resource)]
   :post [(map? %)
          (empty? (select-keys % [:certname :resource]))]}
  (-> resource
      (dissoc :certname :resource :environment)
      ;; All of the sample JSON catalogs I've seen do not include the `file`/`line`
      ;; fields if we don't have actual values for them.
      (kitchensink/dissoc-if-nil :line :file)))

(defn get-resources
  "Given a node name, return a sequence of resources (as maps, conforming to
  our catalog wire format) that appear in the node's latest catalog."
  [version node]
  {:pre  [(string? node)]
   :post [(seq? %)
          (every? map? %)]}
  (map resource-to-wire-format
       (->> (resources/query->sql version ["=" "certname" node])
            (resources/query-resources version)
            (:result))))

(defn get-edges
  "Fetch the edges for the current catalog for the given `node`.  Edges are returned
  as maps, conforming to the catalog wire format."
  [node]
  {:pre  [(string? node)]
   :post [(seq? %)
          (every? map? %)]}
  (let [query (str "SELECT sources.type AS source_type, "
                   "sources.title AS source_title, "
                   "targets.type AS target_type, "
                   "targets.title AS target_title, "
                   "e.type AS relationship "
                   "FROM edges e "
                   "INNER JOIN catalog_resources sources "
                   "ON e.source = sources.resource "
                   "INNER JOIN catalog_resources targets "
                   "ON e.target = targets.resource "
                   "INNER JOIN catalogs c "
                   "ON sources.catalog_id = c.id "
                   "AND targets.catalog_id = c.id "
                   "AND e.certname = c.certname "
                   "WHERE e.certname = ?")]
    (for [{:keys [source_type
                  source_title
                  target_type
                  target_title
                  relationship]} (jdbc/query-to-vec query node)]
      {:source       {:type source_type :title source_title}
       :target       {:type target_type :title target_title}
       :relationship relationship})))

(defn get-full-catalog [catalog-version node]
  (let [{:keys [hash version transaction-uuid environment api_version producer-timestamp] :as catalog}
        (get-catalog-info node)]
    (when (and catalog-version catalog)
      {:name             node
       :edges            (get-edges node)
       :resources        (get-resources version node)
       :version          version
       :transaction-uuid transaction-uuid
       :environment environment
       :hash hash
       :api_version api_version
       :producer-timestamp producer-timestamp})))

(pls/defn-validated validate-api-catalog
  "Converts `catalog` to `version` in the canonical format, adding
   and removing keys as needed"
  [api-version catalog]
  (let [target-schema (catalog-response-schema api-version)
        strip-keys #(pls/strip-unknown-keys target-schema %)]
    (s/validate target-schema
                (case api-version
                  :v1 (-> catalog
                          (dissoc :transaction-uuid :environment :producer-timestamp :hash)
                          cats/old-wire-format-schema)
                  :v2 (-> catalog
                          (dissoc :transaction-uuid :environment :producer-timestamp :hash)
                          cats/old-wire-format-schema
                          strip-keys)
                  :v3 (-> catalog
                          (dissoc :environment :producer-timestamp :hash)
                          cats/old-wire-format-schema
                          strip-keys)
                  :v4 (strip-keys (dissoc catalog :api_version))
                  (strip-keys (dissoc catalog :api_version))))))

(pls/defn-validated catalog-for-node
  "Retrieve the catalog for `node`."
  [version node]
  {:pre  [(string? node)]}
  (when-let [catalog (get-full-catalog version node)]
    (validate-api-catalog version catalog)))
