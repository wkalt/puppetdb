;; ## Catalog persistence
;;
;; Catalogs are persisted in a relational database. Roughly speaking,
;; the schema looks like this:
;;
;; * resource_parameters are associated 0 to N catalog_resources (they are
;; deduped across catalogs). It's possible for a resource_param to exist in the
;; database, yet not be associated with a catalog. This is done as a
;; performance optimization.
;;
;; * edges are associated with a single catalog
;;
;; * catalogs are associated with a single certname
;;
;; * facts are associated with a single certname
;;
;; The standard set of operations on information in the database will
;; likely result in dangling resources, catalogs, paths, and values;
;; to clean these up, it's important to run `garbage-collect!`.

(ns com.puppetlabs.puppetdb.scf.storage
  (:require [com.puppetlabs.puppetdb.catalogs :as cat]
            [com.puppetlabs.puppetdb.reports :as report]
            [com.puppetlabs.puppetdb.facts :as facts :refer [facts-schema]]
            [puppetlabs.kitchensink.core :as kitchensink]
            [com.puppetlabs.jdbc :as jdbc]
            [clojure.java.jdbc :as sql]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.puppetlabs.cheshire :as json]
            [clojure.data :as data]
            [com.puppetlabs.puppetdb.scf.hash :as shash]
            [com.puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [com.puppetlabs.puppetdb.scf.hash-debug :as hashdbg]
            [schema.core :as s]
            [com.puppetlabs.puppetdb.schema :as pls :refer [defn-validated]]
            [com.puppetlabs.puppetdb.utils :as utils]
            [clj-time.coerce :refer [to-timestamp]]
            [clj-time.core :refer [ago secs now before?]]
            [puppetlabs.kitchensink.core :refer [select-values]]
            [metrics.meters :refer [meter mark!]]
            [metrics.counters :refer [counter inc! value]]
            [metrics.gauges :refer [gauge]]
            [metrics.histograms :refer [histogram update!]]
            [metrics.timers :refer [timer time!]]
            [com.puppetlabs.jdbc :refer [query-to-vec dashes->underscores]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(def resource-ref-schema
  {:type String
   :title String})

(def json-primitive-schema (s/either String Number Boolean))

;; the maximum number of parameters pl-jdbc will admit in a prepared statement
;; is 32767. delete-pending-value-id-orphans will create a prepared statement
;; with 5 times the number of invalidated values, so 6000 here keeps us under
;; that and leaves some room.
(def gc-chunksize 6000)

(def resource-schema
  (merge resource-ref-schema
         {(s/optional-key :exported) Boolean
          (s/optional-key :file) String
          (s/optional-key :line) s/Int
          (s/optional-key :tags) #{String}
          (s/optional-key :aliases)#{String}
          (s/optional-key :parameters) {s/Any s/Any}}))

(def resource-ref->resource-schema
  {resource-ref-schema resource-schema})

(def edge-relationship-schema (s/enum :contains :before :required-by :notifies :subscription-of))

(def edge-schema
  {:source resource-ref-schema
   :target resource-ref-schema
   :relationship edge-relationship-schema})

(def catalog-schema
  "This is a bit of a hack to make a more restrictive schema in the storage layer.
   Moving the more restrictive resource/edge schemas into puppetdb.catalogs is TODO. Upstream
   code needs to assume a map of resources (not a vector) and tests need to be update to adhere
   to the new format."
  (assoc (cat/catalog-schema :all)
    :resources resource-ref->resource-schema
    :edges #{edge-schema}))

(def environments-schema
  {:id s/Int
   :name s/Str})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas - Internal

(def resource-ref->hash {resource-ref-schema String})

(def edge-db-schema
  #{[(s/one String "source hash")
     (s/one String "target hash")
     (s/one String "relationship type")]})

(declare add-certname!)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Metrics

(def ns-str (str *ns*))

;; ## Performance metrics
;;
;; ### Timers for catalog storage
;;
;; * `:replace-catalog`: the time it takes to replace the catalog for
;;   a host
;;
;; * `:new-catalog`: the time it takes to persist a catalog for a
;;    never before seen certname
;;
;; * `:catalog-hash-match`: the time it takes to persist the updates
;;    for a catalog with a matches the previously stored hash
;;
;; * `:catalog-hash-miss`: the time it takes to persist the
;;    differential updates of the previously stored catalog and this one
;;
;; * `:add-resources`: the time it takes to persist just a catalog's
;;   resources
;;
;; * `:add-edges`: the time it takes to persist just a catalog's edges
;;
;; * `:catalog-hash`: the time it takes to compute a catalog's
;;   similary hash
;;
;; ### Counters for catalog storage
;;
;; * `:updated-catalog`: how many brand new or updated (non-duplicate)
;;    catalogs we've received
;;
;; * `:duplicate-catalog`: how many duplicate catalogs we've received
;;
;; ### Gauges for catalog storage
;;
;; * `:duplicate-pct`: percentage of incoming catalogs determined to
;;   be duplicates
;;
;; ### Catalog Histograms
;;
;; * `:catalog-volatility`: number of inserts/updates/deletes required
;;   per hash miss
;;
;; ### Timers for garbage collection
;;
;; * `:gc`: the time it takes to collect all database garbage
;;
;; * `:gc-catalogs`: the time it takes to remove all unused catalogs
;;
;; * `:gc-params`: the time it takes to remove all unused resource params
;;
;; ### Timers for fact storage
;;
;; * `:replace-facts`: the time it takes to replace the facts for a
;;   host
;;
(def metrics
  {
   :add-resources      (timer [ns-str "default" "add-resources"])
   :add-edges          (timer [ns-str "default" "add-edges"])

   :resource-hashes    (timer [ns-str "default" "resource-hashes"])
   :catalog-hash       (timer [ns-str "default" "catalog-hash"])
   :add-new-catalog    (timer [ns-str "default" "new-catalog-time"])
   :catalog-hash-match (timer [ns-str "default" "catalog-hash-match-time"])
   :catalog-hash-miss  (timer [ns-str "default" "catalog-hash-miss-time"])
   :replace-catalog    (timer [ns-str "default" "replace-catalog-time"])

   :gc                 (timer [ns-str "default" "gc-time"])
   :gc-catalogs        (timer [ns-str "default" "gc-catalogs-time"])
   :gc-params          (timer [ns-str "default" "gc-params-time"])
   :gc-environments    (timer [ns-str "default" "gc-environments-time"])
   :gc-report-statuses (timer [ns-str "default" "gc-report-statuses"])
   :gc-fact-paths  (timer  [ns-str "default" "gc-fact-paths"])

   :updated-catalog    (counter [ns-str "default" "new-catalogs"])
   :duplicate-catalog  (counter [ns-str "default" "duplicate-catalogs"])
   :duplicate-pct      (gauge [ns-str "default" "duplicate-pct"]
                              (let [dupes (value (:duplicate-catalog metrics))
                                    new   (value (:updated-catalog metrics))]
                                (float (kitchensink/quotient dupes (+ dupes new)))))
   :catalog-volatility (histogram [ns-str "default" "catalog-volitilty"])

   :replace-facts     (timer [ns-str "default" "replace-facts-time"])

   :store-report      (timer [ns-str "default" "store-report-time"])
   })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Certname querying/deleting

(defn certname-exists?
  "Returns a boolean indicating whether or not the given certname exists in the db"
  [certname]
  {:pre [certname]}
  (sql/with-query-results result-set
    ["SELECT 1 FROM certnames WHERE name=? LIMIT 1" certname]
    (pos? (count result-set))))

(defn delete-certname!
  "Delete the given host from the db"
  [certname]
  {:pre [certname]}
  (sql/delete-rows :certnames ["name=?" certname]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Node activation/deactivation

(defn stale-nodes
  "Return a list of nodes that have seen no activity between
  (now-`time` and now)"
  [time]
  {:pre  [(kitchensink/datetime? time)]
   :post [(coll? %)]}
  (let [ts (to-timestamp time)]
    (map :name (jdbc/query-to-vec "SELECT c.name FROM certnames c
                                   LEFT OUTER JOIN catalogs clogs ON c.name=clogs.certname
                                   LEFT OUTER JOIN factsets fs ON c.name=fs.certname
                                   WHERE c.deactivated IS NULL
                                   AND (clogs.timestamp IS NULL OR clogs.timestamp < ?)
                                   AND (fs.timestamp IS NULL OR fs.timestamp < ?)"
                                  ts ts))))

(defn node-deactivated-time
  "Returns the time the node specified by `certname` was deactivated, or nil if
  the node is currently active."
  [certname]
  {:pre [(string? certname)]}
  (sql/with-query-results result-set
    ["SELECT deactivated FROM certnames WHERE name=?" certname]
    (:deactivated (first result-set))))

(defn purge-deactivated-nodes!
  "Delete nodes from the database which were deactivated before `time`."
  [time]
  {:pre [(kitchensink/datetime? time)]}
  (let [ts (to-timestamp time)]
    (sql/delete-rows :certnames ["deactivated < ?" ts])))

(defn activate-node!
  "Reactivate the given host.  Adds the host to the database if it was not
  already present."
  [certname]
  {:pre [(string? certname)]}
  (when-not (certname-exists? certname)
    (add-certname! certname))
  (sql/update-values :certnames
                     ["name=?" certname]
                     {:deactivated nil}))


(pls/defn-validated create-row :- s/Int
  "Creates a row using `row-map` for `table`, returning the PK that was created upon insert"
  [table :- s/Keyword
   row-map :- {s/Keyword s/Any}]
  (:id (first (sql/insert-records table row-map))))

(pls/defn-validated query-id :- (s/maybe s/Int)
  "Returns the id (primary key) from `table` that contain `row-map` values"
  [table :- s/Keyword
   row-map :- {s/Keyword s/Any}]
  (let [cols (keys row-map)
        where-clause (str "where " (str/join " " (map (fn [col] (str (name col) "=?") ) cols)))]
    (sql/with-query-results rs (apply vector (format "select id from %s %s" (name table) where-clause) (map row-map cols))
      (:id (first rs)))))

(pls/defn-validated ensure-row :- (s/maybe s/Int)
  "Check if the given row (defined by `row-map` exists in `table`, creates it if it does not. Always returns
   the id of the row (whether created or existing)"
  [table :- s/Keyword
   row-map :- {s/Keyword s/Any}]
  (when row-map
    (if-let [id (query-id table row-map)]
      id
      (create-row table row-map))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Environments querying/updating

(pls/defn-validated environment-id :- (s/maybe s/Int)
  "Returns the id (primary key) from the environments table for the given `env-name`"
  [env-name :- s/Str]
  (query-id :environments {:name env-name}))

(pls/defn-validated ensure-environment :- (s/maybe s/Int)
  "Check if the given `env-name` exists, creates it if it does not. Always returns
   the id of the `env-name` (whether created or existing)"
  [env-name :- (s/maybe s/Str)]
  (when env-name
    (ensure-row :environments {:name env-name})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Status querying/updating

(pls/defn-validated status-id :- (s/maybe s/Int)
  "Returns the id (primary key) from the result_statuses table for the given `status`"
  [status :- s/Str]
  (query-id :report_statuses {:status status}))

(pls/defn-validated ensure-status :- (s/maybe s/Int)
  "Check if the given `status` exists, creates it if it does not. Always returns
   the id of the `status` (whether created or existing)"
  [status :- (s/maybe s/Str)]
  (when status
    (ensure-row :report_statuses {:status status})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Catalog updates/changes

(defn catalog-metadata
  "Returns the id and hash of certname's catalog"
  [certname]
  {:pre [certname]}
  (sql/with-query-results result-set
    ["SELECT id, hash FROM catalogs WHERE certname=?" certname]
    (first result-set)))

(pls/defn-validated catalog-row-map
  "Creates a row map for the catalogs table, optionally adding envrionment when it was found"
  [hash
   {:keys [api_version version transaction-uuid environment producer-timestamp]} :- catalog-schema
   timestamp :- pls/Timestamp]
  {:hash hash
   :api_version api_version
   :catalog_version  version
   :transaction_uuid transaction-uuid
   :timestamp (to-timestamp timestamp)
   :environment_id (ensure-environment environment)
   :producer_timestamp (to-timestamp producer-timestamp)})

(pls/defn-validated update-catalog-metadata!
  "Given some catalog metadata, update the db"
  [id :- Number
   hash :- String
   catalog :- catalog-schema
   timestamp :- pls/Timestamp]
  (sql/update-values :catalogs
                     ["id=?" id]
                     (catalog-row-map hash catalog timestamp)))

(pls/defn-validated add-catalog-metadata!
  "Given some catalog metadata, persist it in the db. Returns a map of the
  inserted data including any autogenerated columns."
  [hash :- String
   {:keys [name] :as catalog} :- catalog-schema
   timestamp :- pls/Timestamp]
  {:post [(map? %)]}
  (first (sql/insert-records :catalogs
                             (assoc (catalog-row-map hash catalog timestamp)
                               :certname name))))

(pls/defn-validated resources-exist? :- #{String}
  "Given a collection of resource-hashes, return the subset that
  already exist in the database."
  [resource-hashes :- #{String}]
  {:pre  [(coll? resource-hashes)
          (every? string? resource-hashes)]
   :post [(set? %)]}
  (let [resource-array (sutils/array-to-param "text" String (vec resource-hashes))
        query "SELECT DISTINCT resource FROM resource_params_cache WHERE resource=ANY(?)"
        sql-params [query resource-array]]
    (sql/with-query-results result-set
      sql-params
      (set (map :resource result-set)))))

;;The schema definition of this function should be
;;resource-ref->resource-schema, but there are a lot of tests that
;;have incorrect data. When examples.clj and tests get fixed, this
;;should be changed to the correct schema
(pls/defn-validated catalog-resources
  "Returns the resource hashes keyed by resource reference"
  [catalog-id :- Number]
  (sql/with-query-results result-set
    ["SELECT type, title, tags, exported, file, line, resource
      FROM catalog_resources
      WHERE catalog_id = ?" catalog-id]
    (zipmap (map #(select-keys % [:type :title]) result-set)
            (jdbc/convert-result-arrays set result-set))))

(pls/defn-validated new-params-only
  "Returns a map of not persisted parameters, keyed by hash"
  [persisted-params :- #{String}
   refs-to-resources :- resource-ref->resource-schema
   refs-to-hashes :- resource-ref->hash]
  (reduce-kv (fn [acc resource-ref {:keys [parameters]}]
               (let [resource-hash (get refs-to-hashes resource-ref)]
                 (if (contains? persisted-params (refs-to-hashes resource-ref))
                   acc
                   (assoc acc resource-hash parameters))))
             {} refs-to-resources))

(pls/defn-validated insert-records*
  "Nil/empty safe insert-records, see java.jdbc's insert-records for more "
  [table :- s/Keyword
   record-coll :- [{s/Keyword s/Any}]]
  (when (seq record-coll)
    (apply sql/insert-records table record-coll)))

(pls/defn-validated add-params!
  "Persists the new parameters found in `refs-to-resources` and populates the
   resource_params_cache."
  [refs-to-resources :- resource-ref->resource-schema
   refs-to-hashes :- resource-ref->hash]
  (let [new-params (new-params-only (resources-exist? (kitchensink/valset refs-to-hashes))
                                    refs-to-resources
                                    refs-to-hashes)]

    (update! (:catalog-volatility metrics) (* 2 (count new-params)))

    (insert-records*
     :resource_params_cache
     (map (fn [[resource-hash params]]
            {:resource resource-hash :parameters (when params (sutils/db-serialize params))})
          new-params))

    (insert-records*
     :resource_params
     (for [[resource-hash params] new-params
           [k v] params]
       {:resource resource-hash :name (name k) :value (sutils/db-serialize v)}))))

(def resource-ref?
  "Returns true of the map is a resource reference"
  (every-pred :type :title))

(defn convert-tags-array
  "Converts the given tags (if present) to the format the database expects"
  [resource]
  (if (contains? resource :tags)
    (update-in resource [:tags] sutils/to-jdbc-varchar-array)
    resource))

(pls/defn-validated insert-catalog-resources!
  "Returns a function that accepts a seq of ref keys to insert"
  [catalog-id :- Number
   refs-to-hashes :- {resource-ref-schema String}
   refs-to-resources :- resource-ref->resource-schema]
  (fn [refs-to-insert]
    {:pre [(every? resource-ref? refs-to-insert)]}

    (update! (:catalog-volatility metrics) (count refs-to-insert))

    (insert-records*
     :catalog_resources
     (map (fn [resource-ref]
            (let [{:keys [type title exported parameters tags file line] :as resource} (get refs-to-resources resource-ref)]
              (convert-tags-array
               {:catalog_id catalog-id
                :resource (get refs-to-hashes resource-ref)
                :type type
                :title title
                :tags tags
                :exported exported
                :file file
                :line line})))
          refs-to-insert))))

(pls/defn-validated delete-catalog-resources!
  "Returns a function accepts old catalog resources that should be deleted."
  [catalog-id :- Number]
  (fn [refs-to-delete]
    {:pre [(every? resource-ref? refs-to-delete)]}

    (update! (:catalog-volatility metrics) (count refs-to-delete))

    (doseq [{:keys [type title]} refs-to-delete]
      (sql/delete-rows :catalog_resources ["catalog_id = ? and type = ? and title = ?" catalog-id type title]))))

(s/defn basic-diff
  "Basic diffing that returns only the keys/values of `right` whose values don't match those of `left`.
   This is different from clojure.data/diff in that it treats non-equal sets as completely different
   (rather than returning only the differing items of the set) and only returns differences from `right`."
  [left right]
  (reduce-kv (fn [acc k right-value]
               (let [left-value (get left k)]
                 (if (= left-value right-value)
                   acc
                   (assoc acc k right-value))))
             {} right))

(s/defn diff-resources-metadata
  "Return resource references with values that are only the key/values that from `right` that
   are different from those of the `left`. The keys/values here are suitable for issuing update
   statements that will update resources to the correct (new) values."
  [left right]
  (reduce-kv (fn [acc k right-values]
               (let [updated-resource-vals (basic-diff (get left k) right-values)]
                 (if (seq updated-resource-vals)
                   (assoc acc k updated-resource-vals)
                   acc))) {} right))

(defn merge-resource-hash
  "Assoc each hash from `refs-to-hashes` as :resource on `refs-to-resources`"
  [refs-to-hashes refs-to-resources]
  (reduce-kv (fn [acc k v]
               (assoc-in acc [k :resource] (get refs-to-hashes k)))
             refs-to-resources refs-to-resources))

(pls/defn-validated update-catalog-resources!
  "Returns a function accepting keys that were the same from the old resources and the new resources."
  [catalog-id :- Number
   refs-to-hashes :- {resource-ref-schema String}
   refs-to-resources
   old-resources]

  (fn [maybe-updated-refs]
    {:pre [(every? resource-ref? maybe-updated-refs)]}
    (let [new-resources-with-hash (merge-resource-hash refs-to-hashes (select-keys refs-to-resources maybe-updated-refs))
          updated-resources (diff-resources-metadata old-resources new-resources-with-hash)]

      (update! (:catalog-volatility metrics) (count updated-resources))

      (doseq [[{:keys [type title]} updated-cols] updated-resources]
        (sql/update-values :catalog_resources
                           ["catalog_id = ? and type = ? and title = ?" catalog-id type title]
                           (convert-tags-array updated-cols))))))

(defn strip-params
  "Remove params from the resource as it is stored (and hashed) separately
   from the resource metadata"
  [resource]
  (dissoc resource :parameters))

(pls/defn-validated add-resources!
  "Persist the given resource and associate it with the given catalog."
  [catalog-id :- Number
   refs-to-resources :- resource-ref->resource-schema
   refs-to-hashes :- {resource-ref-schema String}]
  (let [old-resources (catalog-resources catalog-id)
        diffable-resources (kitchensink/mapvals strip-params refs-to-resources)]
    (sql/transaction
     (add-params! refs-to-resources refs-to-hashes)
     (utils/diff-fn old-resources
                    diffable-resources
                    (delete-catalog-resources! catalog-id)
                    (insert-catalog-resources! catalog-id refs-to-hashes diffable-resources)
                    (update-catalog-resources! catalog-id refs-to-hashes diffable-resources old-resources)))))

(pls/defn-validated catalog-edges-map
  "Return all edges for a given catalog id as a map"
  [certname :- String]
  (sql/with-query-results result-set
    ["SELECT source, target, type FROM edges WHERE certname=?" certname]
    ;; Transform the result-set into a map with [source,target,type] as the key
    ;; and nil as always the value. This just feeds into clojure.data/diff
    ;; better this way.
    (zipmap (map vals result-set)
            (repeat nil))))

(pls/defn-validated delete-edges!
  "Delete edges for a given certname.

  Edges must be either nil or a collection of lists containing each element
  of an edge, eg:

    [[<source> <target> <type>] ...]"
  [certname :- String
   edges :- edge-db-schema]

  (update! (:catalog-volatility metrics) (count edges))

  (doseq [[source target type] edges]
    ;; This is relatively inefficient. If we have id's for edges, we could do
    ;; this in 1 statement.
    (sql/delete-rows :edges
                     ["certname=? and source=? and target=? and type=?" certname source target type])))

(pls/defn-validated insert-edges!
  "Insert edges for a given certname.

  Edges must be either nil or a collection of lists containing each element
  of an edge, eg:

    [[<source> <target> <type>] ...]"
  [certname :- String
   edges :- edge-db-schema]

  ;; Insert rows will not safely accept a nil, so abandon this operation
  ;; earlier.
  (when (seq edges)
    (let [rows (for [[source target type] edges]
                 [certname source target type])]

      (update! (:catalog-volatility metrics) (count rows))
      (apply sql/insert-rows :edges rows))))

(pls/defn-validated replace-edges!
  "Persist the given edges in the database

  Each edge is looked up in the supplied resources map to find a
  resource object that corresponds to the edge. We then use that
  resource's hash for persistence purposes.

  For example, if the source of an edge is {'type' 'Foo' 'title' 'bar'},
  then we'll lookup a resource with that key and use its hash."
  [certname :- String
   edges :- #{edge-schema}
   refs-to-hashes :- {resource-ref-schema String}]

  (let [new-edges (zipmap
                   (for [{:keys [source target relationship]} edges
                         :let [source-hash (refs-to-hashes source)
                               target-hash (refs-to-hashes target)
                               type        (name relationship)]]
                     [source-hash target-hash type])
                   (repeat nil))]
    (utils/diff-fn new-edges
                   (catalog-edges-map certname)
                   #(insert-edges! certname %)
                   #(delete-edges! certname %)
                   identity)))

(pls/defn-validated update-catalog-hash-match
  "When a new incoming catalog has the same hash as an existing catalog, update metrics
   and the transaction id for the new catalog"
  [catalog-id :- Number
   hash :- String
   catalog :- catalog-schema
   timestamp :- pls/Timestamp]
  (inc! (:duplicate-catalog metrics))
  (time! (:catalog-hash-match metrics)
         (update-catalog-metadata! catalog-id hash catalog timestamp)))

(pls/defn-validated update-catalog-associations!
  "Adds/updates/deletes the edges and resources for the given certname"
  [catalog-id :- Number
   {:keys [resources edges name]} :- catalog-schema
   refs-to-hashes :- {resource-ref-schema String}]
  (time! (:add-resources metrics)
         (add-resources! catalog-id resources refs-to-hashes))
  (time! (:add-edges metrics)
         (replace-edges! name edges refs-to-hashes)))

(pls/defn-validated update-catalog-hash-miss
  "New catalogs for a given certname needs to have their metadata, resources and edges updated.  This
   function also outputs debugging related information when `catalog-hash-debug-dir` is not nil"
  [catalog-id :- Number
   hash :- String
   catalog :- catalog-schema
   refs-to-hashes :- {resource-ref-schema String}
   catalog-hash-debug-dir :- (s/maybe s/Str)
   timestamp :- pls/Timestamp]

  (inc! (:updated-catalog metrics))

  (when catalog-hash-debug-dir
    (hashdbg/debug-catalog catalog-hash-debug-dir hash catalog))

  (time! (:catalog-hash-miss metrics)
         (update-catalog-metadata! catalog-id hash catalog timestamp)
         (update-catalog-associations! catalog-id catalog refs-to-hashes)))

(pls/defn-validated add-new-catalog
  "Creates new catalog metadata and adds the proper associations for the edges and resources"
  [hash :- String
   catalog :- catalog-schema
   refs-to-hashes :- {resource-ref-schema String}
   timestamp :- pls/Timestamp]
  (inc! (:updated-catalog metrics))
  (time! (:add-new-catalog metrics)
         (let [catalog-id (:id (add-catalog-metadata! hash catalog timestamp))]
           (update-catalog-associations! catalog-id catalog refs-to-hashes))))

(pls/defn-validated add-catalog!
  "Persist the supplied catalog in the database, returning its
   similarity hash. `catalog-hash-debug-dir` is an optional path that
   indicates where catalog debugging information should be stored."
  ([catalog :- catalog-schema]
     (add-catalog! catalog nil (now)))
  ([{:keys [api_version resources edges name] :as catalog} :- catalog-schema
    catalog-hash-debug-dir :- (s/maybe s/Str)
    timestamp :- pls/Timestamp]

     (let [refs-to-hashes (time! (:resource-hashes metrics)
                                 (reduce-kv (fn [acc k v]
                                              (assoc acc k (shash/resource-identity-hash v)))
                                            {} resources))
           hash           (time! (:catalog-hash metrics)
                                 (shash/catalog-similarity-hash catalog))
           {id :id
            stored-hash :hash} (catalog-metadata name)]

       (sql/transaction
        (cond
         (nil? id)
         (add-new-catalog hash catalog refs-to-hashes timestamp)

         (= stored-hash hash)
         (update-catalog-hash-match id hash catalog timestamp)

         :else
         (update-catalog-hash-miss id hash catalog refs-to-hashes catalog-hash-debug-dir timestamp)))

       hash)))

(defn delete-catalog!
  "Remove the catalog identified by the following hash"
  [catalog-hash]
  (sql/delete-rows :catalogs ["hash=?" catalog-hash]))

(defn catalog-hash-for-certname
  "Returns the hash for the `certname` catalog"
  [certname]
  (sql/with-query-results result-set
    ["SELECT hash as catalog FROM catalogs WHERE certname=?" certname]
    (:catalog (first result-set))))

;; ## Database compaction

(defn delete-unassociated-params!
  "Remove any resources that aren't associated with a catalog"
  []
  (time! (:gc-params metrics)
         (sql/delete-rows :resource_params_cache ["NOT EXISTS (SELECT * FROM catalog_resources cr WHERE cr.resource=resource_params_cache.resource)"])))


(defn delete-unassociated-environments!
  "Remove any environments that aren't associated with a catalog, report or factset"
  []
  (time! (:gc-environments metrics)
         (sql/delete-rows :environments
           ["ID NOT IN
              (SELECT environment_id FROM catalogs WHERE environment_id IS NOT NULL
               UNION
               SELECT environment_id FROM reports WHERE environment_id IS NOT NULL
               UNION
               SELECT environment_id FROM factsets WHERE environment_id IS NOT NULL)"])))

(defn delete-unassociated-statuses!
  "Remove any statuses that aren't associated with a report"
  []
  (time! (:gc-report-statuses metrics)
         (sql/delete-rows :report_statuses
           ["ID NOT IN (SELECT status_id FROM reports)"])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Facts

(defn-validated select-pid-vid-pairs-for-factset
  :- [(s/pair s/Int "path-id" s/Int "value-id")]
  "Return a collection of pairs of [path-id value-id] for the indicated factset."
  [factset-id :- s/Int]
  (for [{:keys [fact_path_id fact_value_id]}
        (query-to-vec "SELECT fact_path_id, fact_value_id FROM facts
                         WHERE factset_id = ?" factset-id)]
    [fact_path_id fact_value_id]))

(pls/defn-validated certname-to-factset-id :- s/Int
  "Given a certname, returns the factset id."
  [certname :- String]
  (sql/with-query-results result-set
    ["SELECT id from factsets WHERE certname = ?" certname]
    (:id (first result-set))))

(defn-validated delete-pending-path-id-orphans!
  "Delete paths in dropped-pids that are no longer mentioned
   in other factsets."
  [factset-id dropped-pids]
  (when-let [dropped-pids (seq dropped-pids)]
    (let [dropped-chunks (partition-all gc-chunksize dropped-pids)
          in-chunks (map jdbc/in-clause dropped-chunks)]
      (dorun
        (map (fn [in dropped]
               (sql/do-prepared
                 (format
                   "DELETE FROM fact_paths fp
                      WHERE fp.id %s
                        AND NOT EXISTS (SELECT 1 FROM facts f
                                        WHERE f.fact_path_id %s
                                        AND f.fact_path_id = fp.id
                                        AND f.factset_id <> ?)"
                   in in)
                 (concat dropped dropped [factset-id])))
             in-chunks
             dropped-chunks)))))

(defn-validated delete-pending-value-id-orphans!
  "Delete values in removed-pid-vid-pairs that are no longer mentioned
   in facts."
  [factset-id removed-pid-vid-pairs]
  (when-let [removed-pid-vid-pairs (seq removed-pid-vid-pairs)]
    (let [vid-chunks (partition-all gc-chunksize
                                    (map second removed-pid-vid-pairs))
          removed-fact-chunks (->> removed-pid-vid-pairs
                                   (map cons (repeat factset-id))
                                   (partition-all gc-chunksize))
          vid-in-chunks (map jdbc/in-clause vid-chunks)
          removed-facts-in-chunks (map jdbc/in-clause-multi
                                       removed-fact-chunks (repeat 3))]
      (dorun
        (map (fn [in-vids in-rm-facts vids rm-facts]
               (sql/do-prepared
                 (format
                   "DELETE FROM fact_values fv
                      WHERE fv.id %s
                        AND NOT EXISTS (SELECT 1 FROM facts f
                                        WHERE f.fact_value_id %s
                                        AND f.fact_value_id = fv.id
                                        AND (f.factset_id,
                                             f.fact_path_id,
                                             f.fact_value_id) NOT %s)"
                   in-vids in-vids in-rm-facts)
                 (flatten [vids vids rm-facts])))
             vid-in-chunks
             removed-facts-in-chunks
             vid-chunks
             removed-fact-chunks)))))

(defn-validated delete-orphaned-paths! :- s/Int
  "Deletes up to n paths that are no longer mentioned by any factsets,
  and returns the number actually deleted.  These orphans can be
  created by races between parallel updates since (for performance) we
  don't serialize those transactions.  Via repeatable read, an update
  transaction may decide not to delete paths that are only referred to
  by other facts that are being changed in parallel transactions to
  also not refer to the paths."
  [n :- (s/both s/Int (s/pred (complement neg?) 'nonnegative?))]
  (if (zero? n)
    0
    (first
     (sql/transaction
      (sql/do-prepared
       "DELETE FROM fact_paths
          WHERE id IN (SELECT fp.id
                         FROM fact_paths fp
                         WHERE NOT EXISTS (SELECT 1
                                             FROM facts f
                                             WHERE fp.id = f.fact_path_id)
                         LIMIT ?)"
       [n])))))

(defn-validated delete-orphaned-values! :- s/Int
  "Deletes up to n values that are no longer mentioned by any
  factsets, and returns the number actually deleted.  These orphans
  can be created by races between parallel updates since (for
  performance) we don't serialize those transactions.  Via repeatable
  read, an update transaction may decide not to delete values that are
  only referred to by other facts that are being changed in parallel
  transactions to also not refer to the values."
  [n :- (s/both s/Int (s/pred (complement neg?) 'nonnegative?))]
  (if (zero? n)
    0
    (first
     (sql/transaction
      (sql/do-prepared
       "DELETE FROM fact_values
          WHERE id in (SELECT fv.id
                         FROM fact_values fv
                         WHERE NOT EXISTS (SELECT 1
                                             FROM facts f
                                             WHERE fv.id = f.fact_value_id)
                         LIMIT ?)"
       [n])))))

;; NOTE: now only used in tests.
(defn-validated delete-certname-facts!
  "Delete all the facts for certname."
  [certname :- String]
  (sql/transaction
   (let [factset-id (certname-to-factset-id certname)
         dead-pairs (select-pid-vid-pairs-for-factset factset-id)]
     (sql/do-commands
      (format "DELETE FROM facts WHERE factset_id = %s" factset-id))
     (delete-pending-path-id-orphans! factset-id (set (map first dead-pairs)))
     (delete-pending-value-id-orphans! factset-id dead-pairs)
     (sql/delete-rows :factsets ["id=?" factset-id]))))

(defn-validated insert-facts-pv-pairs!
  [factset-id :- s/Int
   pairs :- (s/either [(s/pair s/Int "path-id" s/Int "value-id")]
                      #{(s/pair s/Int "path-id" s/Int "value-id")})]
  (apply sql/insert-records
         :facts
         (for [[pid vid] pairs]
           {:factset_id factset-id :fact_path_id pid :fact_value_id vid})))

(defn existing-row-ids
  "Returns a map from value to id for each value that's already in the
  named database column."
  [database column values]
  (let [k (keyword column)]
    (into {}
          (for [rec (apply query-to-vec
                           (format "SELECT %s, id FROM %s WHERE %s %s"
                                   column
                                   (if (keyword? database) (name database))
                                   column
                                   (jdbc/in-clause values))
                           values)]
            [(k rec) (:id rec)]))))

(defn realize-records!
  "Inserts the records (maps) into the named database and returns them
  with their new :id values."
  [database records]
  (map #(assoc %2 :id %1)
       (map :id (apply sql/insert-records database records))
       records))

(defn realize-paths!
  "Ensures that all paths exist in the database and returns a map of
  paths to ids."
  [pathstrs]
  (if-let [pathstrs (seq pathstrs)]
    (let [existing-path-ids (existing-row-ids :fact_paths "path" pathstrs)
          missing-db-paths (set/difference (set pathstrs)
                                           (set (keys existing-path-ids)))]
      (merge existing-path-ids
             (into {}
                   (map #(vector (:path %) (:id %))
                        (realize-records!
                         :fact_paths
                         (map (comp facts/path->pathmap facts/string-to-factpath)
                              missing-db-paths))))))
    {}))

(defn realize-values!
  "Ensures that all valuemaps exist in the database and returns a
  map of value hashes to ids."
  [valuemaps]
  (if-let [valuemaps (seq valuemaps)]
    (let [vhashes (map :value_hash valuemaps)
          existing-vhash-ids (existing-row-ids :fact_values "value_hash" vhashes)
          missing-vhashes (set/difference (set vhashes)
                                          (set (keys existing-vhash-ids)))]
      (merge existing-vhash-ids
             (into {}
                   (map #(vector (:value_hash %) (:id %))
                        (realize-records!
                         :fact_values
                         (set (filter #(missing-vhashes (:value_hash %))
                                      valuemaps)))))))
    {}))

(pls/defn-validated add-facts!
  "Given a certname and a map of fact names to values, store records for those
  facts associated with the certname."
  [{:keys [name values environment timestamp producer-timestamp] :as fact-data}
   :- facts-schema]
  (sql/transaction
   (sql/insert-record :factsets
                      {:certname name
                       :timestamp (to-timestamp timestamp)
                       :environment_id (ensure-environment environment)
                       :producer_timestamp (to-timestamp producer-timestamp)})
   ;; Ensure that all the required paths and values exist, and then
   ;; insert the new facts.
   (let [paths-and-valuemaps (facts/facts->paths-and-valuemaps values)
         pathstrs (map (comp facts/factpath-to-string first) paths-and-valuemaps)
         valuemaps (map second paths-and-valuemaps)
         vhashes (map :value_hash valuemaps)
         paths-to-ids (realize-paths! pathstrs)
         vhashes-to-ids (realize-values! valuemaps)]
     (insert-facts-pv-pairs! (certname-to-factset-id name)
                             (map #(vector (paths-to-ids %1)
                                           (vhashes-to-ids %2))
                                  pathstrs vhashes)))))

(defn-validated update-facts!
  "Given a certname, querys the DB for existing facts for that
   certname and will update, delete or insert the facts as necessary
   to match the facts argument. (cf. add-facts!)"
  [{:keys [name values environment timestamp producer-timestamp] :as fact-data}
   :- facts-schema]

  (sql/transaction
   (let [factset-id (certname-to-factset-id name)
         initial-factset-paths-vhashes
         (query-to-vec "SELECT fp.path, fv.value_hash FROM facts f
                          INNER JOIN fact_paths fp ON f.fact_path_id = fp.id
                          INNER JOIN fact_values fv ON f.fact_value_id = fv.id
                          WHERE factset_id = ?"
                       factset-id)
         ;; Ensure that all the required paths and values exist.
         paths-and-valuemaps (facts/facts->paths-and-valuemaps values)
         pathstrs (map (comp facts/factpath-to-string first) paths-and-valuemaps)
         valuemaps (map second paths-and-valuemaps)
         vhashes (map :value_hash valuemaps)
         paths-to-ids (realize-paths! pathstrs)
         vhashes-to-ids (realize-values! valuemaps)
         ;; Add new facts and remove obsolete facts.
         replacement-pv-pairs (set (map #(vector (paths-to-ids %1)
                                                 (vhashes-to-ids %2))
                                        pathstrs vhashes))
         current-pairs (set (select-pid-vid-pairs-for-factset factset-id))
         [new-pairs rm-pairs] (data/diff replacement-pv-pairs current-pairs)]

     ;; Paths are unique per factset so we can delete solely based on pid.
     (when rm-pairs
       (let [rm-pids (set (map first rm-pairs))]
         (sql/do-prepared
          (format "DELETE FROM facts WHERE factset_id = ? AND fact_path_id %s"
                  (jdbc/in-clause rm-pids))
          (cons factset-id rm-pids))))

     (insert-facts-pv-pairs! factset-id new-pairs)

     (when rm-pairs
       (delete-pending-path-id-orphans! factset-id
                                        (set/difference
                                         (set (map first rm-pairs))
                                         (set (map first new-pairs))))
       (delete-pending-value-id-orphans! factset-id rm-pairs))

     (sql/update-values :factsets ["id=?" factset-id]
                        {:timestamp (to-timestamp timestamp)
                         :environment_id (ensure-environment environment)
                         :producer_timestamp (to-timestamp producer-timestamp)}))))

(pls/defn-validated factset-timestamp :- (s/maybe pls/Timestamp)
  "Return the factset timestamp for the given certname, nil if not found"
  [certname :- String]
  (sql/with-query-results result-set
    ["SELECT timestamp FROM factsets WHERE certname=? ORDER BY timestamp DESC" certname]
    (:timestamp (first result-set))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Reports

(defn update-latest-report!
  "Given a node name, updates the `latest_reports` table to ensure that it indicates the
  most recent report for the node."
  [node]
  {:pre [(string? node)]}
  (let [latest-report (:hash (first (query-to-vec
                                     ["SELECT hash FROM reports
                                            WHERE certname = ?
                                            ORDER BY end_time DESC
                                            LIMIT 1" node])))]
    (sql/update-or-insert-values
     :latest_reports
     ["certname = ?" node]
     {:certname      node
      :report        latest-report})))

(defn find-containing-class
  "Given a containment path from Puppet, find the outermost 'class'."
  [containment-path]
  {:pre [(or
          (nil? containment-path)
          (and (coll? containment-path) (every? string? containment-path)))]
   :post [((some-fn nil? string?) %)]}
  (when-not ((some-fn nil? empty?) containment-path)
    ;; This is a little wonky.  Puppet only gives us an array of Strings
    ;; to represent the containment path.  Classes can be differentiated
    ;; from types because types have square brackets and a title; so, e.g.,
    ;; "Foo" is a class, but "Foo[Bar]" is a type with a title.
    (first
     (filter
      #(not (or (empty? %) (kitchensink/string-contains? "[" %)))
      (reverse containment-path)))))

(defn maybe-environment
  "This fn is most to help in testing, instead of persisting a value of
   nil, just omit it from the row map. For tests that are running older versions
   of migrations, this function prevents a failure"
  [row-map]
  (if (nil? (:environment_id row-map))
    (dissoc row-map :environment_id)
    row-map))

(defn normalize-resource-event
  "Prep `event` for comparison/computation of a hash"
  [event]
  (-> event
      (utils/update-when [:timestamp] to-timestamp)
      (utils/update-when [:old-value] sutils/db-serialize)
      (utils/update-when [:new-value] sutils/db-serialize)
      (assoc :containing-class (find-containing-class (:containment-path event)))))

(defn normalize-report
  "Prep the report for comparison/computation of a hash"
  [report]
  (-> report
      (update-in [:start-time] to-timestamp)
      (update-in [:end-time] to-timestamp)
      (update-in [:resource-events] #(map normalize-resource-event %))))

(defn convert-containment-path
  "Convert the contain path from a collection to the jdbc array type"
  [event]
  (utils/update-when event
                     [:containment-path]
                     (fn [cp]
                       (when cp
                         (sutils/to-jdbc-varchar-array cp)))))

(defn add-report!*
  "Helper function for adding a report.  Accepts an extra parameter, `update-latest-report?`, which
   is used to determine whether or not the `update-latest-report!` function will be called as part of
   the transaction.  This should always be set to `true`, except during some very specific testing
   scenarios."
  [orig-report
   timestamp
   update-latest-report?]
  {:pre [(map? orig-report)
         (kitchensink/datetime? timestamp)
         (kitchensink/boolean? update-latest-report?)]}
  (time! (:store-report metrics)
         (let [{:keys [puppet-version certname report-format configuration-version
                       start-time end-time resource-events transaction-uuid environment
                       status] :as report} (normalize-report orig-report)
               report-hash (shash/report-identity-hash report)
               containment-path-fn (fn [cp] (if-not (nil? cp) (sutils/to-jdbc-varchar-array cp)))]
           (sql/transaction
             (sql/insert-record :reports
                                (maybe-environment
                                  {:hash                   report-hash
                                   :puppet_version         puppet-version
                                   :certname               certname
                                   :report_format          report-format
                                   :configuration_version  configuration-version
                                   :start_time             start-time
                                   :end_time               end-time
                                   :receive_time           (to-timestamp timestamp)
                                   :transaction_uuid       transaction-uuid
                                   :environment_id         (ensure-environment environment)
                                   :status_id              (ensure-status status)}))

             (->> resource-events
                  (map (comp (partial kitchensink/mapkeys jdbc/dashes->underscores)
                             convert-containment-path #(assoc % :report report-hash)))
                  (apply sql/insert-records :resource_events))
             (when update-latest-report?
               (update-latest-report! certname))))))

(defn delete-reports-older-than!
  "Delete all reports in the database which have an `end-time` that is prior to
  the specified date/time."
  [time]
  {:pre [(kitchensink/datetime? time)]}
  (sql/delete-rows :reports ["end_time < ?" (to-timestamp time)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Database support/deprecation

(defn db-deprecated?
  "Returns a string with an deprecation message if the DB is deprecated,
   nil otherwise."
  [enterprise?]
  (when (and (sutils/postgres?)
             (sutils/db-version-newer-than? [8 3])
             (sutils/db-version-older-than? [9 4])
             (not (and enterprise?
                       (sutils/db-version? [9 2]))))
    "PostgreSQL DB versions 8.4 - 9.3 are deprecated and won't be supported in the future."))

(defn db-unsupported?
  "Returns a string with an unsupported message if the DB is not supported,
   nil otherwise."
  []
  (when (and (sutils/postgres?)
             (sutils/db-version-older-than? [8 4]))
    "PostgreSQL DB versions 8.3 and older are no longer supported. Please upgrade Postgres and restart PuppetDB."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(pls/defn-validated add-certname!
  "Add the given host to the db"
  [certname :- String]
  (sql/insert-record :certnames {:name certname}))

(pls/defn-validated maybe-activate-node!
  "Reactivate the given host, only if it was deactivated before `time`.
  Returns true if the node is activated, or if it was already active.

  Adds the host to the database if it was not already present."
  [certname :- String
   time :- pls/Timestamp]
  (when-not (certname-exists? certname)
    (add-certname! certname))
  (let [timestamp (to-timestamp time)
        replaced  (sql/update-values :certnames
                                     ["name=? AND deactivated<?" certname timestamp]
                                     {:deactivated nil})
        num-updated (first replaced)]
    (pos? num-updated)))

(pls/defn-validated deactivate-node!
  "Deactivate the given host, recording the current time. If the node is
  currently inactive, no change is made."
  [certname :- String]
  (sql/do-prepared "UPDATE certnames SET deactivated = ?
                    WHERE name=? AND deactivated IS NULL"
                   [(to-timestamp (now)) certname]))

(pls/defn-validated catalog-newer-than?
  "Returns true if the most current catalog for `certname` is more recent than
  `time`."
  [certname :- String
   time :- pls/Timestamp]
  (let [timestamp (to-timestamp time)]
    (sql/with-query-results result-set
      ["SELECT timestamp FROM catalogs WHERE certname=? ORDER BY timestamp DESC LIMIT 1" certname]
      (if-let [catalog-timestamp (:timestamp (first result-set))]
        (.after catalog-timestamp timestamp)
        false))))

(pls/defn-validated replace-catalog!
  "Given a catalog, replace the current catalog, if any, for its
  associated host with the supplied one. `catalog-hash-debug-dir`
  is an optional path that indicates where catalog debugging information
  should be stored."
  ([catalog :- catalog-schema
    timestamp :- pls/Timestamp]
     (replace-catalog! catalog timestamp nil))
  ([{:keys [name] :as catalog} :- catalog-schema
    timestamp :- pls/Timestamp
    catalog-hash-debug-dir :- (s/maybe s/Str)]
     (time! (:replace-catalog metrics)
            (sql/transaction
             (add-catalog! catalog catalog-hash-debug-dir timestamp)))))

(pls/defn-validated replace-facts!
  "Updates the facts of an existing node, if the facts are newer than the current set of facts.
   Adds all new facts if no existing facts are found. Invoking this function under the umbrella of
   a repeatable read or serializable transaction enforces only one update to the facts of a certname
   can happen at a time.  The first to start the transaction wins.  Subsequent transactions will fail
   as the factsets will have changed while the transaction was in-flight."
  [{:keys [name values environment timestamp producer-timestamp] :as fact-data} :- facts-schema]
  (time! (:replace-facts metrics)
         (if-let [factset-ts (factset-timestamp name)]
           (when (.before factset-ts (to-timestamp timestamp))
             (update-facts! fact-data))
           (add-facts! fact-data))))

(pls/defn-validated add-report!
  "Add a report and all of the associated events to the database."
  [report
   timestamp :- pls/Timestamp]
  (add-report!* report timestamp true))

(defn warn-on-db-deprecation
  "Log a warning message if the database is deprecated"
  [enterprise?]
  (when-let [deprecated-message (db-deprecated? enterprise?)]
    (log/warn deprecated-message)))

(defn fail-on-unsupported
  "Log an error message to the log and console if the currently
   configured database is unsupported, then call fail-fn  (probably to
   exit)."
  [fail-fn]
  (let [msg (db-unsupported?)]
    (when-let [attn-msg (and msg (utils/attention-msg msg))]
      (utils/println-err attn-msg)
      (log/error attn-msg)
      (fail-fn))))

(defn validate-database-version
  "Checks to ensure that the database is supported, fails if supported, logs
   if deprecated"
  [enterprise? action-for-unsupported-fn]
  (fail-on-unsupported action-for-unsupported-fn)
  (warn-on-db-deprecation enterprise?))

(def ^:dynamic *orphaned-path-gc-limit* 200)
(def ^:dynamic *orphaned-value-gc-limit* 200)

(defn garbage-collect!
  "Delete any lingering, unassociated data in the database"
  [db]
  (time!
   (:gc metrics)
   (jdbc/with-transacted-connection db
     (delete-unassociated-params!)
     (delete-unassociated-environments!)
     (delete-unassociated-statuses!))
   ;; These require serializable because they make the decision to
   ;; delete based on row counts in another table.
   (jdbc/with-transacted-connection' db :serializable
     (delete-orphaned-paths! *orphaned-path-gc-limit*))
   (jdbc/with-transacted-connection' db :serializable
     (delete-orphaned-values! *orphaned-value-gc-limit*))))
