(ns puppetlabs.pe-puppetdb-extensions.command
  (:require [puppetlabs.trapperkeeper.core :refer [defservice]]
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

(defn normalize-report
  "Prep the report for comparison/computation of a hash"
  [{:keys [resources] :as report}]
  (-> report
      (update :start_time to-timestamp)
      (update :end_time to-timestamp)
      (update :producer_timestamp to-timestamp)
      (assoc :resource_events (->> resources
                                   reports/resources->resource-events
                                   (map normalize-resource-event)))))


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
                parameters tags file line]} (get refs-to-resources resource-ref)
        hash-string (get refs-to-hashes resource-ref)]
    (storage/convert-tags-array
      {:hash (sutils/munge-hash-for-storage hash-string)
       :certname_id certname-id
       :type type
       :title title
       :tags tags
       :exported exported
       :file file
       :line line
       :parameters (sutils/munge-jsonb-for-storage parameters)})))

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


(defn add-resources!
  [certname-id open-timerange cap-timerange resources]
  (let [old-resources (existing-resources certname-id)
        ;        diffable-resources (ks/mapvals strip-params)
        ]
    (println "old resources are" old-resources)
    (jdbc/with-db-transaction []
      ;; add parameters
      (diff-fn
        old-resources
        resources
        (cap-resources! cap-timerange certname-id)
        (insert-resources! certname-id open-timerange)
        (fn [xs] {})))
    nil))

(defn store-historical-data
  [certname-id producer-timestamp resources edges]
  (let [open-timerange (->> producer-timestamp
                            (format "[%s,)")
                            sutils/munge-tstzrange-for-storage)
        cap-timerange (->> producer-timestamp
                           (format "(,%s]")
                           sutils/munge-tstzrange-for-storage)
        inserted-resources (add-resources! certname-id open-timerange cap-timerange
                                             resources)]

    (println "INSERTED RESOURCES ARE")
    (clojure.pprint/pprint inserted-resources)
    
    )
  )

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

      (println "refs to resources")
      (clojure.pprint/pprint refs-to-resources)

      (println "refs to hashes")
      (clojure.pprint/pprint refs-to-hashes)
      ;(add-params! refs-to-resources refs-to-hashes)

      ;; cap expired resources
      ;; store new resources

      ;; todo: rename diffable-resources

      (let [refs-to-ids (diff-fn old-resources
                                 diffable-resources
                                 (cap-resources! cap-timerange certname-id)
                                 (insert-resources! certname-id open-timerange
                                                    refs-to-resources
                                                    refs-to-hashes diffable-resources)
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
    "update historical_edges_lifetimes hel
     set time_range = time_range * ?
     from historical_edges he inner join historical_edges_lifetimes
     where he.id = any(?)
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
    (let [rows (for [{:keys [source target relationship]} (keys edges)]
                 {:certname_id certname-id
                  :source_id (get refs->ids source)
                  :target_id (get refs->ids target)
                  :time_range open-timerange
                  :relationship (name relationship)})]

      (apply jdbc/insert! :hist_edges rows))))

(pls/defn-validated add-edges!
  [certname-id :- s/Int
   open-timerange :- s/Any
   cap-timerange :- s/Any
   edges :- #{storage/edge-schema}
   refs-to-hashes :- {storage/resource-ref-schema String}
   inserted-resources :- s/Any]
  (let [new-edges (zipmap
                    (map #(update % :relationship name) edges)
                    (repeat nil))
        catalog-edges-map (existing-edges-map certname-id)
        existing-edges (zipmap (map #(dissoc % :id) catalog-edges-map)
                               (repeat nil))
        existing-ids (map :id catalog-edges-map)
        refs->ids (existing-resources certname-id)]

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


      (println "inserted resources")
      (clojure.pprint/pprint inserted-resources)

      
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
                     :resources (sutils/munge-jsonb-for-storage resources)
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
        (store-historical-data certname-id producer_timestamp report)))))

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
                                    payload)
        validated-payload (cmd/upon-error-throw-fatality
                           (s/validate report-wireformat-schema latest-version-of-payload))]
    (-> command
        (assoc :payload validated-payload)
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
   [:MessageListenerService register-listener]]

  (start [this context]
         (log/info "starting pe command service")
         (let [{:keys [scf-write-db]} (shared-globals)]
           (register-listener report-command? (report-listener scf-write-db)))
         context))
