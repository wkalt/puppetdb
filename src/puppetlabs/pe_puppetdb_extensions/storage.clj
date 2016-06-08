(ns puppetlabs.pe-puppetdb-extensions.storage
  (:import [javax.xml.bind DatatypeConverter])
  (:require [puppetlabs.trapperkeeper.core :refer [defservice]]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.data :as data]
            [clojure.java.jdbc :as sql]
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
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.command :as cmd]
            [puppetlabs.puppetdb.scf.storage :as storage]))

(defn string->bytea
  [s]
  (DatatypeConverter/parseHexBinary s))

(defn bytea->string
  [h]
  (str/lower-case (DatatypeConverter/printHexBinary h)))

(def report-wireformat-schema
  (assoc reports/report-wireformat-schema :edges [s/Any]))

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
                                     :let [resource-spec {:type resource_type :title resource_title}
                                           new-resource (transform-resource* resource)]]
                                 [resource-spec new-resource]))]
    (assoc report :resources new-resources)))

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
    ;; todo: how should this behave?
    ;; If we get an unchanged resource, disregard it
    (empty? (:events resource))
    []

    :else
    (let [resource-metadata (-> resource
                                (select-keys [:resource_type :resource_title
                                              :file :line :containment_path])
                                (set/rename-keys {:type :resource_type
                                                  :title :resource_title}))]
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

(defn existing-resources
  [certname-id]
  (jdbc/query-with-resultset
    ["select type, title, tags, exported, file, line, hash, hr.id
      from historical_resources hr inner join historical_resource_lifetimes hrl
      on hr.id = hrl.resource_id
      where hrl.certname_id = ?
      and upper_inf(time_range)"
     certname-id]
    (fn [rs]
      (let [rss (sql/result-set-seq rs)]
        (doall (jdbc/convert-result-arrays' set [:tags] rss))))))

(defn existing-resource-refs
  [certname-id]
  (jdbc/query-with-resultset
    ["select type, title, resource_id as id
      from historical_resources hr inner join
      historical_resource_lifetimes hrl on hr.id = hrl.resource_id
      where hrl.certname_id = ?  and upper_inf(time_range)"
     certname-id]
    (fn [rs]
      (let [rss (sql/result-set-seq rs)]
        (into {} (->> rss
                      (map (fn [x] {{:type (:type x) :title (:title x)}
                                    (:id x)}))))))))

(defn resource-identity-hash
  [resource]
  (shash/generic-identity-hash
    (select-keys resource [:tags :exported :file :type :line :title])))

(println )

(defn cap-resources!
  [certname-id cap-timerange old-resource-refs resources-to-cap]
  (when (seq resources-to-cap)
    (let [ids-to-cap (map :id (vals (select-keys old-resource-refs (keys resources-to-cap))))]


      (def ids-to-cap ids-to-cap)
      ;(def my-resources old-resource-refs)

      (jdbc/query-with-resultset
        ["update historical_resource_lifetimes
          set time_range = historical_resource_lifetimes.time_range * ?
          from historical_resource_lifetimes as rl inner join
          historical_resources r on rl.resource_id=r.id
          where historical_resource_lifetimes.certname_id = ?
          and upper_inf(historical_resource_lifetimes.time_range)
          and r.id=any(?)
          and rl.id = historical_resource_lifetimes.id
          returning r.id as id, type, title"
         cap-timerange
         certname-id
         (sutils/array-to-param "bigint" Long ids-to-cap)]
        (fn [rs]
          (let [rss (sql/result-set-seq rs)]
            (reduce #(assoc %1 (select-keys %2 [:type :title]) (:id %2)) {} rss)))))))

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
       :hash hash-string
       :line line
       :exported exported})))

(defn existing-historical-resources
  [hashes]
  (jdbc/query-with-resultset
    ["select id, hash, type, title from historical_resources where hash=any(?)"
     (->> hashes
          (map sutils/munge-hash-for-storage)
          vec
          (sutils/array-to-param "bytea" org.postgresql.util.PGobject))]
    (fn [rs]
      (let [rss (sql/result-set-seq rs)]
        (into [] rss)))))

(defn insert-resources!
  [certname-id open-timerange refs-to-hashes refs-to-resources resources-to-insert]
  (let [ref->row (partial resource-ref->row certname-id refs-to-resources refs-to-hashes)
        candidate-rows (map ref->row (keys resources-to-insert))
        existing-candidates (existing-historical-resources (map :hash candidate-rows))
        existing-hashes (set (map (comp bytea->string :hash) existing-candidates))
        inserted-resources (storage/insert-records*
                             :historical_resources
                             (->> candidate-rows
                                  (remove #(contains? existing-hashes (:hash %)))
                                  (map #(update % :hash string->bytea))))
        relevant-resources (concat existing-candidates inserted-resources)]
    (storage/insert-records* :historical_resource_lifetimes
                             (map (fn [{:keys [id]}] {:resource_id id
                                                      :certname_id certname-id
                                                      :time_range open-timerange})
                                  relevant-resources))
    (reduce #(assoc %1 (select-keys %2 [:title :type]) (:id %2))
            {} relevant-resources)))

(defn associate-param-type
  [{:keys [value] :as p}]
  (let [k (cond
            (keyword? value) :value_string
            (string? value) :value_string
            (integer? value) :value_integer
            (float? value) :value_float
            (ks/boolean? value) :value_boolean
            (nil? value) nil
            (coll? value) :value_json)]
    (merge (dissoc p :value) (when k {k value}))))

(defn bundle-params
  [params resource_id]
  (->> params
       (map (fn [x]
              (let [label (name (first x))
                    value (second x)]
                {:parameters (associate-param-type
                               {:name label
                                :value value
                                :value_hash (shash/generic-identity-hash value)})
                 :resource_id resource_id})))))

(defn param-bundles
  [certname-id]
  (jdbc/query-with-resultset
    ["select hrp.id as param_id,
      resource_id, value_hash,
      name from historical_resource_params hrp
      inner join historical_resource_param_lifetimes hrpl on
      hrp.id = hrpl.param_id where certname_id=? and
      upper_inf(hrpl.time_range)"
     certname-id]
    (fn [rs]
      (let [rss (sql/result-set-seq rs)]
        (reduce
          (fn [x y]
            (assoc x
                   {:resource_id (:resource_id y)
                    :parameters (-> y
                                    (update :value_hash bytea->string)
                                    (dissoc :resource_id :param_id))}
                   (:param_id y))) {} rss)))))

(defn cap-params!
  [certname-id cap-timerange full-param-bundles bundle-keys]
  (let [ids-to-cap (vals (select-keys full-param-bundles bundle-keys))]
    (jdbc/query-with-resultset
      ["update historical_resource_param_lifetimes
        set time_range = historical_resource_param_lifetimes.time_range * ?
        from historical_resource_param_lifetimes hrpl inner join historical_resource_params hrp
        on hrpl.param_id = hrp.id
        where upper_inf(historical_resource_param_lifetimes.time_range)
        and hrpl.param_id = any(?)
        and hrpl.certname_id = ?
        and hrpl.id = historical_resource_param_lifetimes.id
        returning 1"
       cap-timerange
       (sutils/array-to-param "bigint" Long ids-to-cap)
       certname-id]
      (constantly #{}))))

(defn existing-params
  [hash->resource-map]
  (let [hashes-to-check (map :hash (keys hash->resource-map))
        munged-hashes (->> hashes-to-check
                           (map sutils/munge-hash-for-storage)
                           vec
                           (sutils/array-to-param "bytea" org.postgresql.util.PGobject))
        result (jdbc/query-to-vec
                 "select name, param_id, value_hash
                  from historical_resource_params hrp
                  inner join historical_resource_param_lifetimes hrpl on
                  hrp.id=hrpl.param_id
                  where value_hash = any(?)"
                 munged-hashes)]

    (jdbc/query-with-resultset
      ["select name, param_id, value_hash
        from historical_resource_params hrp
        inner join historical_resource_param_lifetimes hrpl on
        hrp.id=hrpl.param_id
        where value_hash = any(?)"
       munged-hashes]
      (fn [rs]
        (let [rss (sql/result-set-seq rs)]
          (reduce #(assoc-in %1 [{:name (:name %2)
                                  :hash (bytea->string (byte-array (:value_hash %2)))}
                                 :param_id]
                             (:param_id %2)) hash->resource-map rss))))))

(defn build-hash-map
  [m {:keys [parameters resource_id]}]
  (let [{:keys [value_hash name]} parameters
        munged-hash (string->bytea value_hash)]
    (-> m
        (update-in [{:name name :hash value_hash} :resource_id] conj resource_id)
        (update {:name name :hash value_hash}
                #(utils/assoc-when % :parameters (assoc parameters :value_hash munged-hash))))))

(defn param-refs->lifetime-list
  [param-refs]
  (let [gather-resource-refs (fn [acc x]
                               (let [{:keys [param_id resource_id parameters]} (val x)]
                                 (apply conj acc (map (fn [y] {:resource_id y
                                                               :param_id param_id
                                                               :name (:name parameters)})
                                                      resource_id))))]
    (reduce gather-resource-refs [] param-refs)))

(defn existing-lifetimes
  [lifetime-list]
  (jdbc/query-with-resultset
    ["select resource_id, param_id, name from historical_resource_param_lifetimes
      where upper_inf(time_range) and param_id = any(?)"
     (->> lifetime-list
          (map :param_id)
          vec
          (sutils/array-to-param "bigint" Long))]
    (fn [rs]
      (let [rss (sql/result-set-seq rs)]
        (into [] rss)))))

(defn insert-params!
  [certname-id open-timerange full-param-bundles bundles-to-insert]
  (def my-bundles bundles-to-insert)
  (when (seq bundles-to-insert)
    (let [param-bundles (vals (select-keys full-param-bundles bundles-to-insert))
          aggregated-param-refs (reduce build-hash-map {} param-bundles)
          with-existing-params (existing-params aggregated-param-refs)
          params-to-insert (vec (set (map (comp #(utils/update-when
                                                   % [:value_json]
                                                   sutils/munge-jsonb-for-storage)
                                                :parameters
                                                second)
                                          (remove (fn [[k v]]
                                                    (:param_id v)) with-existing-params))))
          inserted-records (storage/insert-records*
                             :historical_resource_params
                             (utils/distinct-by
                               (fn [x] (bytea->string (:value_hash x)))
                               (map #(dissoc % :name) params-to-insert)))
          hashes->ids (reduce #(assoc %1 (bytea->string (:value_hash %2)) (:id %2))
                              {} inserted-records)

          params-with-ids (map #(assoc % :param_id
                                       (get hashes->ids (bytea->string (:value_hash %))))
                               params-to-insert)

          with-all-params (reduce #(assoc-in %1 [{:hash (bytea->string (:value_hash %2))
                                                  :name (:name %2)} :param_id]
                                             (:param_id %2))
                                  with-existing-params params-with-ids)

          lifetime-list (param-refs->lifetime-list with-all-params)
          existing (existing-lifetimes lifetime-list)
          lifetimes-to-insert (remove (set existing) lifetime-list)]
      (storage/insert-records* :historical_resource_param_lifetimes
                               (map (fn [{:keys [param_id resource_id name]}]
                                      {:param_id param_id
                                       :name name
                                       :resource_id resource_id
                                       :certname_id certname-id
                                       :time_range open-timerange
                                       :deviation_status "notempty"})
                                    lifetimes-to-insert)))))

(defn get-resource-parameters
  [resource-refs->resources resource-refs->resource-ids]
  (->> resource-refs->resources
       (map (fn [x] (let [resource_id (get resource-refs->resource-ids (first x))]
                      (bundle-params (:parameters (second x)) resource_id))))
       ;; todo I don't like this
       flatten))

(defn new-param-bundles->bundle-refs
  [acc p]
  (assoc acc (update p :parameters #(select-keys % [:name :value_hash])) p))

(defn add-params!
  [certname-id
   resource-refs->resources
   cap-timerange
   open-timerange
   resource-refs->resource-ids]
  (let [param-candidates (ks/mapvals :parameters resource-refs->resources)
        new-param-bundles (get-resource-parameters resource-refs->resources
                                                   resource-refs->resource-ids)
        existing-param-bundles (param-bundles certname-id)
        new-params-to-diff (reduce new-param-bundles->bundle-refs {} new-param-bundles)]

    (diff-fn (set (keys existing-param-bundles))
             (set (keys new-params-to-diff))
             (partial cap-params! certname-id cap-timerange existing-param-bundles)
             (partial insert-params! certname-id open-timerange new-params-to-diff)
             identity)))

(defn munge-resources-to-refs
  [resources]
  (reduce #(assoc %1 (select-keys %2 [:type :title]) %2)
          {} resources))

(defn conserved-resource-ids
  [certname-id refs-to-hashes old-resources resources-to-fetch]
  (ks/mapvals :id (select-keys old-resources (keys resources-to-fetch))))

(pls/defn-validated add-resources!
  "Persist the given resource and associate it with the given catalog."
  [certname-id :- Long
   open-timerange :- s/Any
   cap-timerange :- s/Any
   refs-to-resources :- storage/resource-ref->resource-schema
   refs-to-hashes :- {storage/resource-ref-schema String}]

  (let [old-resources (-> certname-id
                          existing-resources
                          munge-resources-to-refs)
        new-resource-refs (ks/mapvals storage/strip-params refs-to-resources)]

    (jdbc/with-db-transaction []
      (diff-fn (ks/mapvals #(dissoc % :hash :id) old-resources)
               new-resource-refs
               (partial cap-resources! certname-id cap-timerange old-resources)
               (partial insert-resources! certname-id open-timerange
                        refs-to-hashes refs-to-resources)
               (partial conserved-resource-ids certname-id refs-to-hashes
                        old-resources)))))

(defn existing-edges-map
  [certname-id]
  (jdbc/query-with-resultset
    ["select relationship,
      sources.type as source_type,
      sources.title as source_title,
      targets.type as target_type,
      targets.title as target_title,
      hel.id from historical_edges he
      inner join historical_edges_lifetimes hel on
      he.id = hel.edge_id
      inner join historical_resources as sources
      on sources.id = he.source_id
      inner join historical_resources as targets
      on targets.id = he.target_id
      where hel.certname_id = ?
      and upper_inf(hel.time_range)"
     certname-id]
    (fn [rs]
      (let [rss (sql/result-set-seq rs)]
        (reduce #(assoc %1 (dissoc %2 :id) (:id %2)) {} rss)))))

(defn cap-edges!
  [certname-id cap-timerange refs-to-ids edges]
  (let [ids-to-cap (vals (select-keys refs-to-ids (keys edges)))]
    (jdbc/query-with-resultset
      ["update historical_edges_lifetimes
        set time_range = historical_edges_lifetimes.time_range * ?
        from historical_edges_lifetimes hel inner join historical_edges he
        on hel.edge_id=he.id where upper_inf(historical_edges_lifetimes.time_range)
        and he.id = any(?)
        and hel.certname_id = ?
        and hel.id = historical_edges_lifetimes.id
        returning 1"
       cap-timerange
       (sutils/array-to-param "bigint" Long ids-to-cap)
       certname-id]
      (constantly #{}))))

(pls/defn-validated insert-edges!
  "Insert edges for a given certname.

  Edges must be either nil or a collection of lists containing each element
  of an edge, eg:

    [[<source> <target> <type>] ...]"
  [certname-id :- s/Int
   open-timerange :- s/Any
   refs->ids :- s/Any
   edges :- s/Any]

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
   inserted-resources :- s/Any]
  (let [new-edges (zipmap
                    (map #(update % :relationship name) edges)
                    (repeat nil))
        catalog-edges-map (existing-edges-map certname-id)
        existing-edges (zipmap (keys catalog-edges-map)
                               (repeat nil))
        refs->ids (existing-resource-refs certname-id)]

    (diff-fn existing-edges new-edges
             (partial cap-edges! certname-id cap-timerange catalog-edges-map)
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
    (let [refs->resource-ids (add-resources! certname-id open-timerange cap-timerange
                                             resources refs-to-hashes)]
      (add-params! certname-id resources cap-timerange open-timerange refs->resource-ids)
      (add-edges! certname-id open-timerange
                  cap-timerange edges refs->resource-ids))))

(defn store-historical-data
  [certname-id producer-timestamp {:keys [resources] :as report}]
  (def the-resources resources)
  (let [refs-to-hashes (ks/mapvals resource-identity-hash resources)]
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
        ;(storage/update-latest-report! certname)
        (store-historical-data certname-id producer_timestamp
                                  (update report :resources (fn [x] (map #(set/rename-keys % {:resource_title :title :resource_type :type})) x)))))))

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
  (def my-db db)
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
