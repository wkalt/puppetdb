(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :refer [keywordize-keys]]
            [clj-time.coerce :as coerce-time]
            [java-jdbc.sql :as sql]
            [cheshire.core :as json]
            [migratus.core :as migratus]
            [schema.core :as sc]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [AnnotatedGroup CheckIn Environment Group
                                                  GroupDelta group->classification groups->tree
                                                  HierarchyNode Node PuppetClass Rule
                                                  ValidationNode]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid OptimizedStorage
                                                               PrimitiveStorage]]
            [puppetlabs.classifier.storage.naive :as naive]
            [puppetlabs.classifier.storage.sql-utils :refer [aggregate-column aggregate-submap-by
                                                             expand-seq-params ordered-group-by]]
            [puppetlabs.classifier.util :refer [dissoc-nil map-delta merge-and-clean
                                                relative-complements-by-key uuid? ->uuid]]
            [slingshot.slingshot :refer [throw+]])
  (:import java.sql.BatchUpdateException
           java.util.UUID
           org.postgresql.util.PSQLException))

(def foreign-key-violation-code "23503")
(def uniqueness-violation-code "23505")
(def serialization-failure-code "40001")

(defn public-tables
  "Get the names of all public tables in a database"
  [db]
  (let [query "SELECT table_name FROM information_schema.tables WHERE LOWER(table_schema) = 'public'"
        results (jdbc/query db [query])]
    (map :table_name results)))

(defn drop-public-tables
  "Drops all public tables in a database. Super dangerous."
  [db]
  (if-let [tables (seq (public-tables db))]
    (apply jdbc/db-do-commands db (map #(format "DROP TABLE %s CASCADE" %) (seq tables)))))

(defn convert-result-arrays
  "Converts Java and JDBC arrays in a result set using the provided function
  (eg. vec, set). Values which aren't arrays are unchanged."
  ([result-set]
     (convert-result-arrays vec result-set))
  ([f result-set]
     (let [convert #(cond
                     (kitchensink/array? %) (f %)
                     (isa? (class %) java.sql.Array) (f (.getArray %))
                     :else %)]
       (map #(kitchensink/mapvals convert %) result-set))))

(defn query
  "An implementation of query that returns a fully evaluated result (no
  JDBCArray objects, etc)"
  [db-spec sql-and-params]
  (let [convert (fn [rs]
                  (doall
                    (convert-result-arrays (comp (partial remove nil?) vec)
                                           (jdbc/result-set-seq rs))))]
    (jdbc/db-query-with-resultset db-spec (vec sql-and-params) convert)))

(defn migrate [db]
  (migratus/migrate {:store :database
                     :migration-dir "migrations"
                     :db db}))

(defn- throw-uniqueness-failure
  [e entity-kind]
  (let [msg (.getMessage e)
        [_ constraint-name] (re-find #"violates unique constraint \"(.+?)\"" msg)
        [_ & tuples] (re-find #"Detail: Key \((.+?)\)=\((.+?)\)" msg)
        [fields values] (map #(str/split % #", ") tuples)]
    (throw+ {:kind ::uniqueness-violation
             :entity-kind entity-kind
             :constraint constraint-name
             :fields fields
             :values values})))

(defn- wrap-uniqueness-failure-info
  [entity-kind insertion-thunk]
  (try (insertion-thunk)
    (catch PSQLException e
      (when-not (= (.getSQLState e) uniqueness-violation-code)
        (throw e))
      (throw-uniqueness-failure e entity-kind))
    (catch BatchUpdateException  e
      (let [root-e (-> e seq last)]
        (when-not (= (.getSQLState root-e) uniqueness-violation-code)
          (throw e)) ; seems safer to throw the original exception
        (throw-uniqueness-failure root-e entity-kind)))))

;; Postgres Storage Record
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord Postgres [db])

(defn new-db [spec]
  (Postgres. spec))

;;; PrimitiveStorage & OptimizedStorage protocol implementations
;;;
;;; Functions here are referred to below when calling extend. This indirection
;;; lets us use pre- and post-conditions as well as schema.

;; Node Check-Ins
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn ^:always-validate store-check-in* :- CheckIn
  [{db :db}, check-in :- CheckIn]
  (let [check-in' (if (contains? check-in :classification)
                   (update-in check-in [:classification] json/encode)
                   check-in)]
    (jdbc/insert! db, :node_check_ins
                  (-> check-in'
                    (update-in [:time] coerce-time/to-sql-time)
                    (update-in [:explanation] json/encode)
                    (set/rename-keys {:transaction-uuid :transaction_uuid}))))
  check-in)

(defn- convert-check-in-fields
  [row]
  (let [with-reqd (-> row
                    (update-in [:time] coerce-time/from-sql-time)
                    (update-in [:explanation] (comp (partial kitchensink/mapkeys ->uuid)
                                                    (fn [exp] (json/decode exp true))))
                    (set/rename-keys {:transaction_uuid :transaction-uuid})
                    (dissoc-nil :transaction-uuid))]
    (if (:classification with-reqd)
      (update-in with-reqd [:classification] json/decode true)
      (dissoc with-reqd :classification))))

(sc/defn ^:always-validate get-check-ins* :- [CheckIn]
  [{db :db}, node-name :- String]
  (->> (sql/select * :node_check_ins
                   (sql/where {:node node-name})
                   (sql/order-by {:time :desc}))
    (query db)
    (map convert-check-in-fields)))

(sc/defn ^:always-validate get-nodes* :- [Node]
  "Retrieve all Nodes from the database, including their CheckIns. The Nodes are
  in ascending name order, and their CheckIns are in reverse chronological order
  (most recent CheckIn first)."
  [{db :db}]
  (->> (sql/select * :node_check_ins (sql/order-by [:node {:time :desc}]))
    (query db)
    (map convert-check-in-fields)
    (ordered-group-by :node)
    (map (fn [[n [_ & c-is]]]
           {:name n, :check-ins (map #(dissoc % :node) c-is)}))))

;; Environments
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn ^:always-validate create-environment* :- Environment
  [{db :db}
   environment :- Environment]
  (jdbc/insert! db :environments environment)
  environment)

(sc/defn ^:always-validate create-environment-if-missing
  [{db :db}
   environment :- Environment]
  (let [{name :name} environment]
    (jdbc/execute! db
                   ["INSERT INTO environments (name) SELECT ?
                    WHERE NOT EXISTS (SELECT 1 FROM environments WHERE name = ?)"
                    name name])))

(sc/defn ^:always-validate get-environment* :- (sc/maybe Environment)
  [{db :db} environment-name]
  {:pre [(string? environment-name)]}
  (let [[environment]
        (query db (sql/select :name :environments
                              (sql/where {:name environment-name})))]
    environment))

(sc/defn ^:always-validate get-environments* :- [Environment]
  [{db :db}]
  (query db (sql/select :name :environments)))

(sc/defn delete-environment*
  [{db :db}, environment-name :- String]
  (jdbc/delete! db :environments (sql/where {:name environment-name})))

;; Classes
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private class-selection
  "SELECT name,
          c.environment_name AS environment,
          cp.parameter,
          cp.default_value
  FROM classes c
  LEFT OUTER JOIN class_parameters cp
    ON c.name = cp.class_name AND c.environment_name = cp.environment_name AND cp.deleted = false")

(defn select-class
  [class-name environment-name]
  [(str class-selection " WHERE c.name = ? AND c.environment_name = ? AND c.deleted = false")
   class-name environment-name])

(defn select-class-where-deleted
  [class-name environment-name]
  [(str class-selection " WHERE c.name = ? AND c.environment_name = ? AND c.deleted = true")
   class-name environment-name])

(defn select-classes
  [environment-name]
  [(str class-selection " WHERE c.environment_name = ? AND c.deleted = false") environment-name])

(defn select-all-classes []
  [(str class-selection " WHERE c.deleted = false")])

(defn- keywordize-parameters
  [class]
  (update-in class, [:parameters]
             (fn [params]
               (into {} (for [[param default] params]
                          [(keyword param) default]))) ))

(defn- deserialize-default-values
  [class]
  (update-in class [:parameters]
             (partial kitchensink/mapvals json/decode)))

(defn- create-parameter
  "Create or update and undelete a parameter"
  [{db :db} parameter default-value class-name environment-name]
  (wrap-uniqueness-failure-info "class parameter"
   #(jdbc/with-db-transaction
      [txn-db db]
      (let [parameter (name parameter)
            where-param (sql/where {:parameter parameter
                                    :class_name class-name
                                    :environment_name environment-name})
            [deleted] (query txn-db (sql/select [:deleted] :class_parameters where-param))]
        (if (nil? deleted)
          ;; the parameter doesn't exist, so create it
          (jdbc/insert! txn-db, :class_parameters
                        {:class_name class-name
                         :parameter parameter
                         :default_value (json/encode default-value)
                         :environment_name environment-name})
          ;; else the parameter exists but might be marked deleted, so update it
          (jdbc/update! txn-db, :class_parameters
                        {:default_value (json/encode default-value), :deleted false}
                        where-param))))))

(sc/defn ^:always-validate create-class* :- PuppetClass
  "Create or update and undelete a class"
  [this, {:keys [name parameters environment] :as class} :- PuppetClass]
  (wrap-uniqueness-failure-info "class"
    #(jdbc/with-db-transaction
       [txn-db (:db this)]
       (let [txn-storage (Postgres. txn-db)]
         (create-environment-if-missing txn-storage {:name environment})
         (let [[deleted] (query txn-db (select-class-where-deleted name environment))
               where-class (sql/where {:name name, :environment_name environment})
               new-class-row {:name name, :environment_name environment, :deleted false}]
           (if deleted
             (jdbc/update! txn-db :classes {:deleted false} where-class)
             (jdbc/insert! txn-db :classes new-class-row)))
         (doseq [[param value] parameters]
           (create-parameter txn-storage param value name environment))
         class))))

(sc/defn ^:always-validate get-class* :- (sc/maybe PuppetClass)
  [{db :db}, environment-name :- String, class-name :- String]
  (let [result (query db (select-class class-name environment-name))]
    (if-not (empty? result)
      (->> result
        (aggregate-submap-by :parameter :default_value :parameters)
        (map keywordize-parameters)
        (map deserialize-default-values)
        first))))

(sc/defn ^:always-validate get-classes* :- [PuppetClass]
  [{db :db}, environment-name :- String]
  (let [result (query db (select-classes environment-name))]
    (->> result
      (aggregate-submap-by :parameter :default_value :parameters)
      (map keywordize-parameters)
      (map deserialize-default-values))))

(sc/defn ^:always-validate get-all-classes* :- [PuppetClass]
  [{db :db}]
  (let [result (query db (select-all-classes))]
    (->> result
      (aggregate-submap-by :parameter :default_value :parameters)
      (map keywordize-parameters)
      (map deserialize-default-values))))

(sc/defn delete-class*
  [{db :db}, environment-name :- String, class-name :- String]
  (jdbc/delete! db :classes (sql/where {:name class-name, :environment_name environment-name})))

(defn- update-class
  "This function does not create a transaction. If you are calling it, make sure
  you are doing so inside of an existing transaction!"
  [{db :db, :as this} new-class old-class]
  (let [{:keys [environment name]} new-class
        [new-params old-params] (for [class [new-class old-class]]
                                  (->> class
                                    :parameters
                                    (sort-by first)))
        [to-add to-delete in-both] (relative-complements-by-key first
                                                               new-params
                                                               old-params)]
    (doseq [[param value] to-delete]
      (let [where-param (sql/where {:parameter (clojure.core/name param)
                                    :class_name name
                                    :environment_name environment})
            [param-used?] (query db (sql/select ["1"] :group_class_parameters where-param))]
        (if param-used?
          (jdbc/update! db :class_parameters {:deleted true} where-param)
          (try (jdbc/delete! db :class_parameters where-param)
            (catch PSQLException e
              (when-not (= (.getSQLState e) foreign-key-violation-code)
                (throw e))
              (jdbc/update! db :class_parameters {:deleted true} where-param))))))
    (doseq [[param value] to-add]
      (create-parameter this param value name environment))
    (doseq [[[param new-value] [_ old-value]] in-both]
      (when-not (= new-value old-value)
        (jdbc/update! db :class_parameters {:default_value (json/encode new-value)}
                      (sql/where {:parameter (clojure.core/name param)
                                  :class_name name
                                  :environment_name environment}))))))

(defn- mark-class-deleted
  [this {:keys [environment parameters], class-name :name}]
  (jdbc/with-db-transaction
    [txn-db (:db this)]
    (jdbc/update! txn-db :classes {:deleted true}
                  (sql/where {:environment_name environment, :name class-name}))
    (doseq [[param _] parameters
            :let [where-param (sql/where {:environment_name environment
                                          :class_name class-name
                                          :parameter (name param)})]]
      (jdbc/update! txn-db :class_parameters {:deleted true} where-param))))

(defn- sort-classes
  [classes]
  (let [env-name-comp (fn [[env1 name1]
                           [env2 name2]]
                        (if (not= env1 env2)
                          (compare env1 env2)
                          (compare name1 name2)))]
    (sort-by (juxt :environment :name) env-name-comp classes)))

(defn set-last-sync
  [{db :db}]
  (let [last-sync (query db (sql/select * :last_sync))]
    (if (empty? last-sync)
      (jdbc/execute! db ["INSERT INTO last_sync (time) VALUES (now())"])
      (jdbc/execute! db ["UPDATE last_sync SET time = now()"]))))

(sc/defn get-last-sync* :- (sc/maybe org.joda.time.DateTime)
  [{db :db}]
  (let [[last-sync] (query db (sql/select * :last_sync))]
    (if (nil? last-sync)
      nil
      (coerce-time/from-sql-time (:time last-sync)))))

(sc/defn ^:always-validate synchronize-classes*
  [this, puppet-classes :- [PuppetClass]]
  (wrap-uniqueness-failure-info "class"
    #(jdbc/with-db-transaction
       [txn-db (:db this)]
       ;; Nothing else should lock this table, so we can use NOWAIT
       (jdbc/execute! txn-db ["LOCK TABLE classes, class_parameters IN EXCLUSIVE MODE NOWAIT"])
       (jdbc/execute! txn-db ["SET CONSTRAINTS ALL IMMEDIATE"])
       (let [txn-storage (Postgres. txn-db)
             db-classes (get-all-classes* txn-storage)
             [to-add to-delete in-both] (relative-complements-by-key (juxt :environment :name)
                                                                     (sort-classes puppet-classes)
                                                                     (sort-classes db-classes))]
         (doseq [{:keys [environment name] :as c} to-delete]
           (let [where-class (sql/where {:environment_name environment, :name name})
                 [class-used?] (query txn-db (sql/select ["1"]
                                                         :group_classes
                                                         (sql/where
                                                           {:environment_name environment
                                                            :class_name name})))]
             (if class-used?
               (mark-class-deleted txn-storage c)
               (try (delete-class* txn-storage environment name)
                 (catch PSQLException e
                   (when-not (= (.getSQLState e) foreign-key-violation-code)
                     (throw e))
                   (mark-class-deleted txn-storage c))))))
         (doseq [class to-add]
           (create-class* txn-storage class))
         (doseq [[new-class old-class] in-both]
           (when-not (= new-class old-class)
             (update-class txn-storage new-class old-class)))
         (set-last-sync txn-storage)))))

;; Rules
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn ^:always-validate create-rule :- Rule
  [{db :db} {:keys [when group-id] :as rule} :- Rule]
  (jdbc/with-db-transaction
    [txn-db db]
    (let [storage-rule {:group_id group-id
                        :match (json/generate-string when)}
          [inserted-rule] (jdbc/insert! txn-db :rules storage-rule)
          rule-id (:id inserted-rule)]
      (assoc rule :id rule-id))))

(sc/defn ^:always-validate get-rules* :- [Rule]
  [{db :db}]
  (for [{:keys [match group_id]} (query db (sql/select * :rules))]
    {:when (json/decode match)
     :group-id group_id}))

;; Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Retrieving Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def group-selection
  "SELECT g.name,
          g.id                 AS id,
          g.environment_name   AS environment,
          g.environment_trumps AS environment_trumps,
          g.parent_id          AS parent,
          g.description        AS description,
          gv.variable          AS variable,
          gv.value             AS variable_value,
          gc.class_name        AS class,
          gcp.parameter        AS parameter,
          gcp.value            AS parameter_value,
          r.match              AS rule
  FROM groups g
       LEFT OUTER JOIN group_classes gc ON g.id = gc.group_id
       LEFT OUTER JOIN group_class_parameters gcp ON gc.group_id = gcp.group_id AND gc.class_name = gcp.class_name
       LEFT OUTER JOIN group_variables gv ON g.id = gv.group_id
       LEFT OUTER JOIN rules r ON g.id = r.group_id ")

(defn- group-selection-with-order
  [selection]
  (str selection " ORDER BY g.id, r.id ASC"))

(defn select-group-children
  [group-id]
  [(str group-selection " WHERE g.parent_id = ? AND g.id != ?") group-id group-id])

(defn- select-group-by-id
  [id]
  [(group-selection-with-order (str group-selection "WHERE g.id = ?"))
   id])

(defn- select-all-groups []
  [(group-selection-with-order group-selection)])

(defn- deserialize-variable-values
  "This function expects a row map that has already had its variable-related
  fields aggregated into a submap. It deserializes the values of the row's
  variables submap (which are expected to be JSON)."
  [row]
  (update-in row [:variables]
             (fn [variables]
               (into {} (for [[k v] variables]
                          [k (json/parse-string v)])))))

(defn- deserialize-group-class-parameters
  [row]
  (update-in row [:classes]
             (fn [classes]
               (into {} (for [[c params] classes]
                          [c (kitchensink/mapvals json/decode params)])))))

(defn- deserialize-rule
  [row]
  (if-let [serialized-condition (:rule row)]
    (assoc row :rule (json/decode serialized-condition))
    (dissoc row :rule)))

(defn- aggregate-fields-into-groups
  [result]
  (->> result
    (aggregate-submap-by :parameter :parameter_value :parameters)
    (aggregate-submap-by :class :parameters :classes)
    (aggregate-submap-by :variable :variable_value :variables)
    (map deserialize-variable-values)
    (map deserialize-group-class-parameters)
    (map deserialize-rule)
    (map #(dissoc-nil % :description))
    (map #(set/rename-keys % {:environment_trumps :environment-trumps}))
    (keywordize-keys)))

(sc/defn ^:always-validate get-group* :- (sc/maybe Group)
  [this, id :- (sc/either String UUID)]
  {:pre [(uuid? id)]}
  (let [[group] (aggregate-fields-into-groups (query (:db this) (select-group-by-id (->uuid id))))]
    group))

(sc/defn ^:always-validate annotate-group* :- AnnotatedGroup
  [{db :db}, group :- Group]
  (let [deleted-kw :puppetlabs.classifier/deleted
        class-names (->> group :classes keys (map name))
        param-names (->> group
                      :classes
                      vals
                      (mapcat keys)
                      (map name))
        marked-classes (if-not (empty? class-names)
                         (->> (query db (expand-seq-params
                                          ["SELECT name FROM classes
                                           WHERE name IN ? AND deleted = true" class-names]))
                           (map (comp keyword :name))))
        marked-params (if-not (or (empty? class-names) (empty? param-names))
                        (->> (query
                               db
                               (expand-seq-params
                                 ["SELECT parameter, class_name FROM class_parameters
                                  WHERE class_name IN ? AND parameter IN ? AND deleted = true"
                                  class-names param-names]))
                          (group-by (comp keyword :class_name))
                          (kitchensink/mapvals (partial map (comp keyword :parameter)))))
        deleted (kitchensink/deep-merge
                  (into {} (for [[c params] marked-params]
                             [c (kitchensink/deep-merge
                                  {deleted-kw false}
                                  (into {} (for [p params]
                                             [p {deleted-kw true
                                                 :value (get-in group [:classes c p])}])))]))
                  (zipmap marked-classes (repeat {deleted-kw true})))]
    (if (empty? deleted)
      group
      (assoc group :deleted deleted))))

(sc/defn ^:always-validate get-groups* :- [Group]
  [{db :db}]
  (aggregate-fields-into-groups (query db (select-all-groups))))

(sc/defn ^:always-validate get-group-ids* :- [UUID]
  [{db :db}]
  (->> (query db ["SELECT id FROM groups"])
    (map :id)))

(sc/defn ^:always-validate get-parent :- (sc/maybe Group)
  [this, group :- Group]
  (get-group* this (:parent group)))

(sc/defn ^:always-validate get-immediate-children :- [Group]
  [{db :db}, group-id :- java.util.UUID]
  (->> (query db (select-group-children group-id))
    aggregate-fields-into-groups))

(sc/defn ^:always-validate get-subtree** :- HierarchyNode
  "A helper for get-subtree* that retains seen ids in order to break cycles.
  It only checks for the root node repeating. Since there is only single
  inheritance, any cycles are unreachable from the rest of the tree so there's
  no need to check for cycles that don't involve the root node."
  [this, subtree-root-id :- java.util.UUID, group :- Group]
  (let [children (->> (:id group)
                   (get-immediate-children this)
                   (remove #(= (:id %) subtree-root-id)))]
    {:group group
     :children (->> children
                 (map (partial get-subtree** this subtree-root-id))
                 set)}))

(sc/defn ^:always-validate get-subtree* :- HierarchyNode
  [this, group :- Group]
  (get-subtree** this (:id group) group))

;; Validating Group
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn ^:always-validate validate-classes-and-parameters :- (sc/maybe ValidationNode)
  "Validates class and class parameter references (including inherited ones) for
  the group and all its descendents using classification/validation-tree to
  ensure that all the referents exist. If any do not, returns a ValidationNode
  subtree rooted at the group; if all references are valid, returns nil."
  [subtree ancestors classes]
  (let [vtree (class8n/validation-tree subtree, (seq classes)
                                       (map group->classification ancestors))]
    (if (class8n/valid-tree? vtree)
      nil
      vtree)))

(sc/defn ^:always-validate validate-hierarchy-structure*
  "Performs validation of the group's place in the hierarchy (i.e. that its
  parent exists and the group doesn't create a cycle in the hierarchy). If the
  group causes any such errors in the hierarchy (i.e. a missing parent or
  a cycle), throws an exception."
  [{id :id :as group} ancestors]
  (when (and (= id (:parent group))
             (not= id root-group-uuid))
    (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
             :cycle [group]}))

  ;; If the group's parent is being changed, that edge is not yet in the
  ;; database so get-ancestors will not see it, meaning we have to check
  ;; ourselves for cycles involving the group.
  (when (some #(= id (:id %)) ancestors)
    (let [cycle (->> ancestors
                  (take-while #(not= id (:id %)))
                  (concat [group]))]
      (when-not (and (= id root-group-uuid) (= (count cycle) 1))
        (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
               :cycle cycle})))))

(sc/defn ^:always-validate validation-failures :- (sc/maybe ValidationNode)
  "Validates a group using `validate-hierarchy-structure*` and
  `validate-classes-and-parameters`. If the group has a hierarchy structure
  problem, an exception will be thrown. If it or its descendents have references
  to missing classes or class parameters, returns a ValidationNode subtree
  rooted at the group. If all validation succeeds, returns nil."
  [this group]
  (let [parent (get-parent this group)
        _ (when (nil? parent)
            (throw+ {:kind ::missing-parent, :group group}))
        ancestors (storage/get-ancestors this group)]
    (validate-hierarchy-structure* group ancestors)
    (let [subtree (get-subtree* this group)
          classes (get-all-classes* this)]
      (validate-classes-and-parameters subtree ancestors classes))))

(sc/defn ^:always-validate group-validation-failures* :- (sc/maybe ValidationNode)
  [this group]
  (if-let [vtree (validation-failures this group)]
    vtree))

(defn- validate-group-and-throw-missing-referents
  [this group]
  (when-let [vtree (group-validation-failures* this group)]
    (throw+ {:kind ::missing-referents
             :tree vtree
             :ancestors (storage/get-ancestors this group)})))

;; Creating & Updating Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- add-group-class-link
  [{db :db} id environment class-name]
  (jdbc/insert! db :group_classes
                [:group_id :environment_name :class_name]
                [id environment (name class-name)]))

(sc/defn ^:always-validate create-group* :- Group
  [{db :db} group :- Group]
   (let [{group-name :name, :as group
          :keys [classes description environment id parent rule variables]} group]
     (wrap-uniqueness-failure-info "group"
       #(jdbc/with-db-transaction
          [txn-db db]
          (let [txn-storage (Postgres. txn-db)]
            (validate-group-and-throw-missing-referents txn-storage group)
            (create-environment-if-missing txn-storage {:name environment})
            (jdbc/insert! txn-db :groups
                          [:name :description :environment_name :environment_trumps :id :parent_id]
                          (conj ((juxt :name :description :environment :environment-trumps) group)
                                id parent))
            (doseq [[v-key v-val] variables]
              (jdbc/insert! txn-db :group_variables
                            [:variable :group_id :value]
                            [(name v-key) id (json/generate-string v-val)]))
            (doseq [class classes]
              (let [[class-name class-params] class]
                (add-group-class-link txn-storage id environment class-name)
                (doseq [class-param class-params]
                  (let [[param value] class-param
                        value (json/encode value)]
                    (jdbc/insert! txn-db :group_class_parameters
                                  [:parameter :class_name :environment_name :group_id :value]
                                  [(name param) (name class-name) environment id value])))))
            (when rule
              (create-rule txn-storage {:when rule, :group-id id}))
            group)))))

(defn- delete-group-class-link
  [{db :db} group-id class-name]
  (jdbc/delete! db, :group_classes
                (sql/where {"group_id" group-id
                            "class_name" class-name})))

(defn- delete-group-class-parameter
  [{db :db} group-id class-name parameter]
  (jdbc/delete! db, :group_class_parameters
                (sql/where {"group_id" group-id
                            "class_name" class-name
                            "parameter" parameter})))

(defn- update-group-class-parameter
  [{db :db} group-id class-name parameter value]
  (jdbc/update! db, :group_class_parameters, {:value (json/encode value)}
                (sql/where {"group_id" group-id
                            "class_name" class-name
                            "parameter" parameter})))

(defn- add-group-class-parameter
  [{db :db} group-id class-name environment-name parameter value]
  (wrap-uniqueness-failure-info "group class parameter"
    #(jdbc/with-db-transaction [txn-db db]
       ;; create the group<->class link in the group_classes table if necessary
       (jdbc/execute! txn-db
                      ["INSERT INTO group_classes (group_id, class_name, environment_name)
                       SELECT ?, ?, ? WHERE NOT EXISTS
                       (SELECT 1 FROM group_classes
                       WHERE group_id = ? AND class_name = ? AND environment_name = ?)"
                       group-id class-name environment-name group-id class-name environment-name])
       (jdbc/insert! txn-db :group_class_parameters
                     {:group_id group-id
                      :class_name class-name
                      :environment_name environment-name
                      :parameter parameter
                      :value (json/encode value)}))))

(defn- update-group-classes
  [{db :db} extant delta]
  (let [group-id (:id extant)
        environment (:environment extant)]
    (wrap-uniqueness-failure-info "group-class link"
      #(jdbc/with-db-transaction [txn-db db]
         (let [txn-storage (Postgres. txn-db)]
           (doseq [[class parameters] (:classes delta)
                   :let [class-name (name class)]]
             (cond
               (nil? parameters)
               (delete-group-class-link txn-storage group-id class-name)

               (empty? parameters) ; only if parameters defined, since if nil previous cond fires
               (add-group-class-link txn-storage group-id environment class-name)

               :else ; handle each parameter individually
               (doseq [[parameter value] parameters
                       :let [parameter-name (name parameter)]]
                 (cond
                   (nil? value)
                   (delete-group-class-parameter txn-storage group-id class-name parameter-name)

                   ;; parameter is set in extant group, so update
                   (not= (get-in extant [:classes class parameter] ::not-found) ::not-found)
                   (update-group-class-parameter txn-storage group-id
                                                 class-name parameter-name value)

                   :otherwise ; parameter is not nil and not previously set, so insert
                   (add-group-class-parameter txn-storage group-id
                                              class-name environment parameter-name value))))))))))

(defn- update-group-variables
  [{db :db} extant delta]
  (let [group-id (:id extant)]
    (wrap-uniqueness-failure-info "group variable"
      #(jdbc/with-db-transaction [txn-db db]
         (doseq [[variable value] (:variables delta)
                 :let [variable-name (name variable)
                       variable-value (json/encode value)]]
           (cond
             (nil? value)
             (jdbc/delete! txn-db, :group_variables
                           (sql/where {"group_id" group-id, "variable" variable-name}))

             (not= (get-in extant [:variables variable] ::not-found) ::not-found)
             (jdbc/update! txn-db, :group_variables, {:value variable-value}
                           (sql/where {"group_id" group-id, "variable" variable-name}))

             :otherwise ; new variable for the group
             (jdbc/insert! txn-db, :group_variables
                           {:group_id group-id
                            :variable variable-name
                            :value variable-value})))))))

(defn- update-group-environment
  [{db :db} extant delta]
  (let [old-env (:environment extant)
        new-env (:environment delta)
        group-id (:id extant)
        where-group-link (sql/where {"group_id" group-id})]
    (when (and new-env (not= new-env old-env))
      (wrap-uniqueness-failure-info "group"
        #(jdbc/with-db-transaction [txn-db db]
          (create-environment-if-missing (Postgres. txn-db) {:name new-env})
          (jdbc/update! txn-db :groups {:environment_name new-env} (sql/where {"id" group-id}))
          (jdbc/update! txn-db :group_classes {:environment_name new-env} where-group-link)
          (jdbc/update! txn-db, :group_class_parameters
                        {:environment_name new-env}
                        where-group-link))))))

(defn- update-group-rule
  [{db :db, :as this} extant {id :id, :as delta}]
  (let [new-rule (:rule delta)]
    (when (and new-rule (not= new-rule (:rule extant)))
      (if (:rule extant)
        (jdbc/update! db, :rules, {:match (json/generate-string new-rule)}
                      (sql/where {:group_id id}))
        (create-rule this {:when new-rule, :group-id id})))
    (when (and (nil? new-rule) (contains? delta :rule))
      (jdbc/delete! db :rules (sql/where {:group_id id})))))

(defn- update-group-field
  [{db :db} map-field row-field extant delta]
  (let [new-value (get delta map-field)]
    (when (and (not (nil? new-value))
               (not= new-value (get extant map-field)))
      (jdbc/update! db, :groups, (hash-map row-field new-value)
                    (sql/where {:id (:id delta)})))))

(sc/defn ^:always-validate delta-validation-failures* :- (sc/maybe ValidationNode)
  "This validates that the delta 1) does not perform an illegal edit such as
  changing the root group's rule, 2) does not cause the hierarchy to become
  malformed and 3) does not introduce any bad class or class parameter
  references to the hierarchy. If the delta fails validation for the first two
  reasons, an exception will be thrown; if it fails for the last,
  a ValidationNode for the group that the delta affects will be returned which
  describes the errors that the edit would introduce to the group's subtree.  If
  the delta passes all validation, then nil will be returned."
  [this, delta :- GroupDelta, extant :- Group]
  (let [group' (merge-and-clean extant delta)
        ancestors' (storage/get-ancestors this group')
        _ (validate-hierarchy-structure* group' ancestors')
        subtree' (get-subtree* this group')
        classes (get-all-classes* this)]
    (if-let [vtree' (validate-classes-and-parameters subtree' ancestors' classes)]
      (let [ancestors (if (= (:parent group') (:parent extant))
                        ancestors'
                        (storage/get-ancestors this extant))
            subtree (assoc subtree' :group extant)
            vtree (validate-classes-and-parameters subtree ancestors classes)
            vtree-diff (if (nil? vtree)
                         vtree'
                         (class8n/validation-tree-difference vtree' vtree))]
        (if-not (class8n/valid-tree? vtree-diff)
          vtree-diff)))))

(defn- validate-delta-and-throw-missing-referents
  [this delta extant]
  (when-let [vtree (delta-validation-failures* this delta extant)]
    (throw+ {:kind ::missing-referents
             :tree vtree
             :ancestors (storage/get-ancestors this (merge-and-clean extant delta))})))

(sc/defn ^:always-validate update-group* :- (sc/maybe Group)
  [{db :db}, delta :- GroupDelta]
  (let [update-thunk #(jdbc/with-db-transaction [txn-db db :isolation :repeatable-read]
                        (let [txn-storage (Postgres. txn-db)]
                          (when-let [extant (get-group* txn-storage (:id delta))]
                            (validate-delta-and-throw-missing-referents txn-storage delta extant)
                            (update-group-classes txn-storage extant delta)
                            (update-group-variables txn-storage extant delta)
                            (update-group-environment txn-storage extant delta)
                            (update-group-rule txn-storage extant delta)
                            (update-group-field txn-storage :description :description extant delta)
                            (update-group-field txn-storage :parent :parent_id extant delta)
                            (update-group-field txn-storage :name :name extant delta)
                            (update-group-field txn-storage
                                                :environment-trumps, :environment_trumps
                                                extant, delta)
                            ;; still in the transaction, so will see the updated rows
                            (get-group* txn-storage (:id delta)))))]
    (loop [retries 3]
      (let [result (try (wrap-uniqueness-failure-info "group" update-thunk)
                     (catch PSQLException e
                       (when-not (= (.getSQLState e) serialization-failure-code)
                         (throw e))
                       (when (zero? retries)
                         (throw e))
                       ::transaction-conflict))]
        (if (= result ::transaction-conflict)
          (recur (dec retries))
          result)))))

(sc/defn ^:always-validate delete-group*
  [this, id :- (sc/either String UUID)]
  {:pre [(uuid? id)]}
  (let [uuid (->uuid id)]
    (when (= uuid root-group-uuid)
      (throw (IllegalArgumentException. "It is forbidden to delete the default group.")))
    (let [children (get-immediate-children this id)]
      (when-not (empty? children)
        (throw+ {:kind ::children-present
                 :group (get-group* this id)
                 :children (set children)})))
    (jdbc/delete! (:db this) :groups (sql/where {:id (->uuid id)}))))

;; Batch Import
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- insert-missing-referents
  [this referents inherited-classification]
  (doseq [[class-kw params] referents
          :let [class-name (name class-kw)]]
    (if (nil? params) ;; entire class is missing
      (let [all-params (-> inherited-classification
                         (get-in [:classes class-kw])
                         keys)
            class {:name class-name
                   :environment (:environment inherited-classification)
                   :parameters (zipmap all-params (repeat nil))}]
        (create-class* this class))
      ;; otherwise, just particular parameters are missing
      (doseq [param params]
        (create-parameter this param nil class-name (:environment inherited-classification))))))

(defn- import-tree
  [this {:keys [group children] :as node}]
  (let [only-group (assoc node :children #{})
        ancestors (storage/get-ancestors this group)
        ancestor-class8ns (map group->classification ancestors)
        classes (get-all-classes* this)]
    (when-let [{missing-referents :errors} (validate-classes-and-parameters
                                             only-group ancestors classes)]
      (let [inherited-class8n (class8n/collapse-to-inherited (group->classification group)
                                                             ancestor-class8ns)]
        (insert-missing-referents this missing-referents inherited-class8n)))
    (if (not= (:id group) root-group-uuid)
      (create-group* this group)
      (let [old-root (get-group* this root-group-uuid)
            delta (assoc (map-delta old-root group) :id root-group-uuid)]
        (update-group* this delta)))
    (doseq [child children] (import-tree this child))))

(defn- delete-tree
  [this {:keys [group children]}]
  (doseq [child children] (delete-tree this child))
  (when (not= (:id group) root-group-uuid)
    (delete-group* this (:id group))))

(sc/defn ^:always-validate import-hierarchy* :- HierarchyNode
  [this, groups :- [Group]]
  (jdbc/with-db-transaction [txn-db (:db this)]
    (let [txn-storage (Postgres. txn-db)
          root-node (groups->tree groups)
          old-root-node (get-subtree* txn-storage (get-group* txn-storage root-group-uuid))]
      (delete-tree txn-storage old-root-node)
      (import-tree txn-storage root-node)
      (get-subtree* txn-storage (get-group* txn-storage root-group-uuid)))))

;; Postgres Record's Storage Protocol Extension
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend Postgres

  PrimitiveStorage
  {:store-check-in store-check-in*
   :get-check-ins get-check-ins*
   :get-nodes get-nodes*

   :create-group create-group*
   :get-group get-group*
   :get-groups get-groups*
   :get-subtree get-subtree*
   :update-group update-group*
   :delete-group delete-group*

   :import-hierarchy import-hierarchy*

   :create-class create-class*
   :get-class get-class*
   :get-classes get-classes*
   :synchronize-classes synchronize-classes*
   :get-last-sync get-last-sync*
   :delete-class delete-class*

   :get-rules get-rules*

   :create-environment create-environment*
   :get-environment get-environment*
   :get-environments get-environments*
   :delete-environment delete-environment*}

  OptimizedStorage
  {:get-group-ids get-group-ids*
   :get-ancestors naive/get-ancestors
   :annotate-group annotate-group*
   :group-validation-failures group-validation-failures*

   :get-all-classes get-all-classes*})
