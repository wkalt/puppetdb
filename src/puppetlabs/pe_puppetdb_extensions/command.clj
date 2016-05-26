(ns puppetlabs.pe-puppetdb-extensions.command
  (:require [puppetlabs.trapperkeeper.core :refer [defservice]]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.data :as data]
            [com.rpl.specter :as sp]
            [puppetlabs.puppetdb.command.constants :refer [command-names]]
            [puppetlabs.puppetdb.scf.hash :as shash]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.puppetdb.reports :as reports]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.time :refer [to-timestamp]]
            [clj-time.core :refer [now]]
            [schema.core :as s]
            [puppetlabs.puppetdb.schema :as pls :refer [defn-validated]]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.command :as cmd]
            [puppetlabs.puppetdb.scf.storage :as storage]))


(defn transform-tags
  "Turns a resource's list of tags into a set of strings."
  [{:keys [tags] :as o}]
  {:pre [tags
         (every? string? tags)]
   :post [(set? (:tags %))]}
  (update-in o [:tags] set))

(defn transform-resource*
  "Normalizes the structure of a single `resource`. Right now this just means
  setifying the tags."
  [resource]
  {:pre [(map? resource)]
   :post [(set? (:tags %))]}
  (transform-tags (-> resource
                      (dissoc :events)
                      (set/rename-keys {:resource_type :type :resource_title :title}))))

(defn transform-resources
  "Turns the list of resources into a mapping of
   `{resource-spec resource, ...}`, as well as transforming each resource."
  [{:keys [resources] :as report}]
  {:pre  [(coll? resources)
          (not (map? resources))]
   :post [(map? (:resources %))
          (= (count resources) (count (:resources %)))]}
  (let [new-resources (into {} (for [{:keys [resource_type resource_title] :as resource} resources
                                     :let [resource-spec    {:type resource_type :title resource_title}
                                           new-resource     (transform-resource* resource)]]
                                     [resource-spec new-resource]))
        result (assoc report :resources new-resources)]
    result))

(defn transform-edge*
  "Converts the `relationship` value of `edge` into a
  keyword."
  [edge]
  {:pre [(:relationship edge)]
   :post [(keyword? (:relationship %))]}
  (update edge :relationship keyword))

(defn transform-edges
  "Transforms every edge of the given `catalog` and converts the edges into a set."
  [{:keys [edges] :as report}]
  {:pre  [(coll? edges)]
   :post [(set? (:edges %))
          (every? keyword? (map :relationship (:edges %)))]}
  (assoc report :edges (set (map transform-edge* edges))))

(def transform
  "Applies every transformation to the catalog, converting it from wire format
  to our internal structure."
  (comp
   transform-edges
   transform-resources))

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
      #(not (or (empty? %) (ks/string-contains? "[" %)))
      (reverse containment-path)))))


(defn diff-fn
  [left right left-only-fn right-only-fn same-fn]
  (let  [[left-only right-only same]  (data/diff left right)]
    (merge
      (left-only-fn  (or left-only #{}))
      (right-only-fn  (or right-only #{}))
      (same-fn  (or same #{})))))


(defn normalize-resource-event
  "Prep `event` for comparison/computation of a hash"
  [event]
  (-> event
      (update :timestamp to-timestamp)
      (update :old_value sutils/db-serialize)
      (update :new_value sutils/db-serialize)
      (assoc :containing_class (find-containing-class (:containment_path event)))))

(defn- resource->skipped-resource-events
  "Fabricate a skipped resource-event"
  [resource]
  (-> resource
      ;; We also need to grab the timestamp when the resource is `skipped'
      (select-keys [:type :title :file :line :containment_path :timestamp])
      (set/rename-keys {:type :resource_type :title :resource_title})
      (merge {:status "skipped" :property nil :old_value nil :new_value nil :message nil})
      vector))

(defn- resource->resource-events
  [{:keys [skipped] :as resource}]
  (cond
    (= skipped true)
    (resource->skipped-resource-events resource)

    ;; If we get an unchanged resource, disregard it
    (empty? (:events resource))
    []

    :else
    (let [resource-metadata (-> resource
                                (select-keys [:resource_type :resource_title :file :line :containment_path])
                                (set/rename-keys {:type :resource_type :title :resource_title}))]
      (map (partial merge resource-metadata) (:events resource)))))

(defn resources->resource-events
  [resources]
  (->> resources
       (mapcat resource->resource-events)
       vec))

(defn normalize-report
  "Prep the report for comparison/computation of a hash"
  [{:keys [resources] :as report}]
  (-> report
      (update :start_time to-timestamp)
      (update :end_time to-timestamp)
      (update :producer_timestamp to-timestamp)
      (assoc :resource_events (->> resources
                                   resources->resource-events
                                   (map normalize-resource-event)))
      transform-edges
      transform-resources))


(defn report-command?
  [x]
  (= (:command x) "store report"))

(defn existing-resources
  [certname-id]
  (jdbc/query-to-vec
    [(format "select type, title, tags, exported, file, line, %s as resource
              from historical_resources inner join historical_resource_lifetimes
              on historical_resources.id = historical_resource_lifetimes.resource_id
              where historical_resource_lifetimes.certname_id = ?
              and upper_inf(time_range)"
             (sutils/sql-hash-as-str "hash")) certname-id]))

(defn existing-resource-refs
  [certname-id]
  (let [existing-data (jdbc/query-to-vec
                        [(format "select *
                                  from historical_resources inner join historical_resource_lifetimes
                                  on historical_resources.id = historical_resource_lifetimes.resource_id
                                  where historical_resource_lifetimes.certname_id = ?
                                  and upper_inf(time_range)"
                                 (sutils/sql-hash-as-str "hash")) certname-id])]

    (into {} (->> existing-data
                  (map (fn [x]
                         {{:type (:type x) :title (:title x)}
                          (:id x)}))))))

(defn cap-resources!
  [timerange certname-id]
  (fn [refs-to-hashes]
    (when (seq refs-to-hashes)
      (jdbc/query-to-vec
        "update historical_resource_lifetimes
         set time_range = historical_resource_lifetimes.time_range * ?
         from historical_resource_lifetimes as rl inner join historical_resources r on rl.resource_id=r.id
         where historical_resource_lifetimes.certname_id = ?
         and upper_inf(historical_resource_lifetimes.time_range)
         and r.hash=any(?)
         returning 1"
        timerange
        certname-id
        (->> (map :resource (vals refs-to-hashes))
             (map sutils/munge-hash-for-storage)
             vec
             (sutils/array-to-param "bytea" org.postgresql.util.PGobject))))
    {}))

(defn resource-ref->row
  [certname-id refs-to-resources refs-to-hashes resource-ref]
  (let [{:keys [type title exported
                parameters tags file line] :as foo} (get refs-to-resources resource-ref)
        hash-string (get refs-to-hashes resource-ref)]
    (storage/convert-tags-array
      {:type type
       :title title
       :tags tags
       :file file
       :hash (sutils/munge-hash-for-storage hash-string)
       :line line
       :exported exported})))

(defn insert-resources!
  [certname-id timerange refs-to-hashes refs-to-resources]
  (fn [refs-to-insert]
    (let [ref->row (partial resource-ref->row certname-id refs-to-resources
                            refs-to-hashes)
          candidate-rows (map ref->row (keys refs-to-insert))
          inserted-resources (storage/insert-records* :historical_resources candidate-rows)]
      (storage/insert-records*
        :historical_resource_lifetimes
        (map (fn [{:keys [id]}] {:resource_id id :certname_id certname-id :time_range timerange})
             inserted-resources))
      (reduce #(assoc %1 (select-keys %2 [:title :type]) {:id (:id %2)})
              {} inserted-resources))))

(pls/defn-validated add-resources!
  "Persist the given resource and associate it with the given catalog."
  [certname-id :- Long
   open-timerange :- s/Any
   cap-timerange :- s/Any
   refs-to-resources :- storage/resource-ref->resource-schema
   refs-to-hashes :- {storage/resource-ref-schema String}]
  (let [old-resources (existing-resources certname-id)
        diffable-resources (ks/mapvals storage/strip-params refs-to-resources)]
    (jdbc/with-db-transaction []

      ;(add-params! refs-to-resources refs-to-hashes)

      ;; cap expired resources
      ;; store new resources

      ;; todo: rename diffable-resources

      (println "OLD RESOURCES ARE")
      (clojure.pprint/pprint old-resources)

      (let [refs-to-ids (diff-fn old-resources
                                 diffable-resources
                                 (cap-resources! cap-timerange certname-id)
                                 (insert-resources! certname-id open-timerange
                                                    refs-to-hashes refs-to-resources)
                                 ;(update-catalog-resources! certname-id refs-to-hashes diffable-resources old-resources)
                                 ; todo is there an update that needs to happen here?
                                 (fn [xs] {}))]

        (merge-with #(assoc %1 :hash %2)
                    refs-to-ids (select-keys refs-to-hashes (keys refs-to-ids)))))))

(defn existing-edges-map
  [certname-id]
  (let [existing-data (jdbc/query-to-vec
                        [(format "select source_id, target_id, relationship, he.id,
                                  %s as source_hash, sources.type as source_type,
                                  sources.title as source_title,
                                  %s as target_hash, targets.type as target_type,
                                  targets.title as target_title from historical_edges he
                                  inner join historical_edges_lifetimes hel on
                                  he.id = hel.edge_id
                                  inner join historical_resources as sources
                                  on sources.id = he.source_id
                                  inner join historical_resources as targets
                                  on targets.id = he.target_id
                                  where hel.certname_id = ?
                                  and upper_inf(hel.time_range)"
                                 (sutils/sql-hash-as-str "sources.hash")
                                 (sutils/sql-hash-as-str "targets.hash"))
                         certname-id])]
    (->> existing-data
         (map (fn [x] {:source {:type (:source_type x)
                                :title (:source_title x)}
                       :target {:type (:target_type x)
                                :title (:target_title x)}
                       :relationship (:relationship x)
                       :id (:id x)})))))

(defn cap-edges!
  [timerange ids edges]
  (jdbc/query-to-vec
    "update historical_edges_lifetimes
     set time_range = historical_edges_lifetimes.time_range * ?
     from historical_edges_lifetimes hel inner join historical_edges he
     on hel.edge_id=he.id where upper_inf(historical_edges_lifetimes.time_range) and he.id = any(?)
     returning 1"
    timerange
    (sutils/array-to-param "bigint" Long ids)))

(pls/defn-validated insert-edges!
  "Insert edges for a given certname.

  Edges must be either nil or a collection of lists containing each element
  of an edge, eg:

    [[<source> <target> <type>] ...]"
  [certname-id :- s/Int
   open-timerange :- s/Any
   refs->ids :- s/Any
   edges :- s/Any]

  ;; Insert rows will not safely accept a nil, so abandon this operation
  ;; earlier.
  (when (seq edges)
    (let [candidate-rows (for [{:keys [source_title source_type
                                       target_title target_type relationship]} (keys edges)]
                           {:source_id (get refs->ids {:type source_type
                                                       :title source_title})
                            :target_id (get refs->ids {:type target_type
                                                       :title target_title})
                            :relationship (name relationship)})
          inserted-edges (storage/insert-records* :historical_edges candidate-rows)]
      (storage/insert-records*
        :historical_edges_lifetimes
        (map (fn [{:keys [id]}] {:edge_id id :certname_id certname-id :time_range open-timerange})
             inserted-edges))))
  {})

(def edge-schema
  {:source_title s/Str
   :source_type s/Str
   :target_title s/Str
   :target_type s/Str
   :relationship s/Any})

(pls/defn-validated add-edges!
  [certname-id :- s/Int
   open-timerange :- s/Any
   cap-timerange :- s/Any
   edges :- #{edge-schema}
   refs-to-hashes :- {storage/resource-ref-schema String}
   inserted-resources :- s/Any]
  (println "OPTEN TIMERANGE IS IS")
  (clojure.pprint/pprint open-timerange)
  (let [new-edges (zipmap
                    (map #(update % :relationship name) edges)
                    (repeat nil))
        catalog-edges-map (existing-edges-map certname-id)
        existing-edges (zipmap (map #(dissoc % :id) catalog-edges-map)
                               (repeat nil))
        existing-ids (map :id catalog-edges-map)
        refs->ids (existing-resource-refs certname-id)]

    (diff-fn existing-edges new-edges
             (partial cap-edges! cap-timerange existing-ids)
             (partial insert-edges! certname-id open-timerange refs->ids)
             identity)))

(defn update-report-associations
  [certname-id
   {:keys [resources edges certname producer_timestamp] :as report}
   refs-to-hashes]

  (let [open-timerange (->> producer_timestamp
                            (format "[%s,)")
                            sutils/munge-tstzrange-for-storage)
        cap-timerange (->> producer_timestamp
                           (format "(,%s]")
                           sutils/munge-tstzrange-for-storage)]
    (let [inserted-resources (add-resources! certname-id open-timerange cap-timerange
                                             resources refs-to-hashes)]

             (add-edges! certname-id open-timerange
                         cap-timerange edges refs-to-hashes
                         inserted-resources))))

(defn store-historical-data
  [certname-id producer-timestamp {:keys [resources] :as report}]
  (let [refs-to-hashes (ks/mapvals shash/resource-identity-hash resources)]
    (update-report-associations certname-id report refs-to-hashes)))

(defn add-report!*
  [orig-report
   received-timestamp]
  (let [{:keys [puppet_version certname report_format configuration_version
                producer_timestamp start_time end_time transaction_uuid environment
                status noop metrics logs resources resource_events catalog_uuid
                code_id cached_catalog_status edges]
         :as report} (normalize-report orig-report)
        report-hash (shash/report-identity-hash report)]
    (jdbc/with-db-transaction []
      (let [certname-id (storage/certname-id certname)
            row-map {:hash (sutils/munge-hash-for-storage report-hash)
                     :transaction_uuid (sutils/munge-uuid-for-storage transaction_uuid)
                     :catalog_uuid (sutils/munge-uuid-for-storage catalog_uuid)
                     :code_id code_id
                     :cached_catalog_status cached_catalog_status
                     :metrics (sutils/munge-jsonb-for-storage metrics)
                     :logs (sutils/munge-jsonb-for-storage logs)
                     :noop noop
                     :puppet_version puppet_version
                     :certname certname
                     :report_format report_format
                     :configuration_version configuration_version
                     :producer_timestamp producer_timestamp
                     :start_time start_time
                     :end_time end_time
                     :receive_time (to-timestamp received-timestamp)
                     :environment_id (storage/ensure-environment environment)
                     :status_id (storage/ensure-status status)}
            [{report-id :id}] (->> row-map
                                   storage/maybe-environment
                                   storage/maybe-resources
                                   (jdbc/insert! :reports))
            assoc-ids #(assoc %
                              :report_id report-id
                              :certname_id certname-id)]
        (when-not (empty? resource_events)
          (->> resource_events
               (sp/transform [sp/ALL :containment_path] #(some-> % sutils/to-jdbc-varchar-array))
               (map assoc-ids)
               (apply jdbc/insert! :resource_events)))
        (storage/update-latest-report! certname)
        (store-historical-data certname-id producer_timestamp
                               (update report :resources (fn [x] (map #(set/rename-keys % {:resource_title :title :resource_type :type})) x)))))))

(def report-wireformat-schema
  (assoc reports/report-wireformat-schema :edges [s/Any]))

(s/defn add-report!
  "Add a report and all of the associated events to the database."
  [report :- report-wireformat-schema
   received-timestamp :- pls/Timestamp]
  (add-report!* report received-timestamp))

(defn store-report*
  [{:keys [payload annotations]} db]
  (let [{id :id received-timestamp :received} annotations
        {:keys [certname puppet_version] :as report} payload
        producer-timestamp (to-timestamp (:producer_timestamp payload (now)))]
    (jdbc/with-transacted-connection db
      (storage/maybe-activate-node! certname producer-timestamp)
      (add-report! report received-timestamp))
    (log/infof "[%s] [%s] puppet v%s - %s"
               id (command-names :store-report)
               puppet_version certname)))

(defn store-report [{:keys [payload version annotations] :as command} db]
  (let [{received-timestamp :received} annotations
        latest-version-of-payload (case version
                                    3 (reports/wire-v3->wire-v7 payload received-timestamp)
                                    4 (reports/wire-v4->wire-v7 payload received-timestamp)
                                    5 (reports/wire-v5->wire-v7 payload)
                                    6 (reports/wire-v6->wire-v7 payload)
                                    payload)]
    (-> command
        (assoc :payload latest-version-of-payload)
        (store-report* db))))

(defn report-listener
  [db]
  (fn [command]
    (log/info "received pe report")
    (store-report command db)))

(defprotocol PeCommandService)

(defservice pe-command-service
  PeCommandService
  [[:PuppetDBServer shared-globals]
   [:DefaultedConfig get-config]
   [:MessageListenerService register-listener]
   PuppetDBCommandDispatcher]

  (start [this context]
         (log/info "starting pe command service")
         (let [{:keys [scf-write-db]} (shared-globals)]
           (register-listener report-command? (report-listener scf-write-db)))
         context))
