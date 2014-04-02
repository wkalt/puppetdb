(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.walk :refer [keywordize-keys]]
            [java-jdbc.sql :as sql]
            [cheshire.core :as json]
            [migratus.core :as migratus]
            [schema.core :as sc]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [AnnotatedGroup Environment Group GroupDelta
                                                  group->classification HierarchyNode Node
                                                  PuppetClass Rule]]
            [puppetlabs.classifier.storage :refer [Storage]]
            [puppetlabs.classifier.storage.sql-utils :refer [aggregate-column aggregate-submap-by
                                                             expand-seq-params]]
            [puppetlabs.classifier.util :refer [flatten-tree-with merge-and-clean
                                                relative-complements-by-key uuid? ->uuid]]
            [slingshot.slingshot :refer [throw+]])
  (:import org.postgresql.util.PSQLException
           java.util.UUID))

(def foreign-key-violation-code "23503")
(def serialization-failure-code "40001")

(def root-group-uuid (java.util.UUID/fromString "00000000-0000-4000-8000-000000000000"))

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
    (jdbc/db-query-with-resultset db-spec sql-and-params convert)))

(defn migrate [db]
  (migratus/migrate {:store :database
                     :migration-dir "migrations"
                     :db db}))

;;; Storage protocol implementation
;;;
;;; Functions here are referred to below when calling extend. This indirection
;;; lets us use pre- and post-conditions as well as schema.

;; Nodes
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn select-node [node]
  (sql/select :name :nodes (sql/where {:name node})))

(sc/defn ^:always-validate create-node* :- Node
  [{db :db}
   node :- Node]
  (jdbc/insert! db :nodes node)
  node)

(sc/defn ^:always-validate get-node* :- (sc/maybe Node)
  [{db :db}, node-name :- String]
  (let [[node] (jdbc/query db (select-node node-name))]
    node))

(sc/defn ^:always-validate get-nodes* :- [Node]
  [{db :db}]
  (jdbc/query db (sql/select :name :nodes)))

(sc/defn delete-node*
  [{db :db}, node-name :- String]
  (jdbc/delete! db :nodes (sql/where {:name node-name})))

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
        (jdbc/query db (sql/select :name :environments
                                   (sql/where {:name environment-name})))]
    environment))

(sc/defn ^:always-validate get-environments* :- [Environment]
  [{db :db}]
  (jdbc/query db (sql/select :name :environments)))

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

(defn- create-parameter
  "Create or update and undelete a parameter"
  [db parameter default-value class-name environment-name]
  (jdbc/with-db-transaction
    [t-db db]
    (let [parameter (name parameter)
          where-param (sql/where {:parameter parameter
                                  :class_name class-name
                                  :environment_name environment-name})
          [deleted] (jdbc/query t-db (sql/select [:deleted] :class_parameters where-param))]
      (if (nil? deleted)
        ;; the parameter doesn't exist, so create it
        (jdbc/insert! t-db, :class_parameters
                      {:class_name class-name
                       :parameter parameter
                       :default_value default-value
                       :environment_name environment-name})
        ;; else the parameter exists but might be marked deleted, so update it
        (jdbc/update! t-db, :class_parameters
                      {:default_value default-value, :deleted false}
                      where-param)))))

(sc/defn ^:always-validate create-class* :- PuppetClass
  "Create or update and undelete a class"
  [{db :db}
   {:keys [name parameters environment] :as class} :- PuppetClass]
  (jdbc/with-db-transaction
    [t-db db]
    (create-environment-if-missing {:db t-db} {:name environment})
    (let [[deleted] (jdbc/query t-db (select-class-where-deleted name environment))]
      (if deleted
        (jdbc/update! t-db :classes {:deleted false} (sql/where {:name name, :environment_name environment}))
        (jdbc/insert! t-db :classes {:name name, :environment_name environment, :deleted false})))
    (doseq [[param value] parameters]
      (create-parameter t-db param value name environment)))
  class)

(sc/defn ^:always-validate get-class* :- (sc/maybe PuppetClass)
  [{db :db}, environment-name :- String, class-name :- String]
  (let [result (jdbc/query db (select-class class-name environment-name))]
    (if-not (empty? result)
      (->> result
        (aggregate-submap-by :parameter :default_value :parameters)
        (map keywordize-parameters)
        first))))

(sc/defn ^:always-validate get-classes* :- [PuppetClass]
  [{db :db}, environment-name :- String]
  (let [result (jdbc/query db (select-classes environment-name))]
    (->> result
      (aggregate-submap-by :parameter :default_value :parameters)
      (map keywordize-parameters))))

(sc/defn ^:always-validate get-all-classes :- [PuppetClass]
  [db]
  (let [result (jdbc/query db (select-all-classes))]
    (->> result
      (aggregate-submap-by :parameter :default_value :parameters)
      (map keywordize-parameters))))

(sc/defn delete-class*
  [{db :db}, environment-name :- String, class-name :- String]
  (jdbc/delete! db :classes (sql/where {:name class-name, :environment_name environment-name})))

(defn- update-class
  [db new-class old-class]
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
            [param-used?] (jdbc/query db (sql/select ["1"] :group_class_parameters where-param))]
        (if param-used?
          (jdbc/update! db :class_parameters {:deleted true} where-param)
          (try (jdbc/delete! db :class_parameters where-param)
            (catch PSQLException e
              (when-not (= (.getSQLState e) foreign-key-violation-code)
                (throw e))
              (jdbc/update! db :class_parameters {:deleted true} where-param))))))
    (doseq [[param value] to-add]
      (create-parameter db param value name environment))
    (doseq [[[param new-value] [_ old-value]] in-both]
      (when-not (= new-value old-value)
        (jdbc/update! db :class_parameters {:default_value new-value}
                      (sql/where {:parameter (clojure.core/name param)
                                  :class_name name
                                  :environment_name environment}))))))

(defn- mark-class-deleted
  [db {:keys [environment parameters], class-name :name}]
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/update! t-db :classes {:deleted true}
                  (sql/where {:environment_name environment, :name class-name}))
    (doseq [[param _] parameters
            :let [where-param (sql/where {:environment_name environment
                                          :class_name class-name
                                          :parameter (name param)})]]
      (jdbc/update! t-db :class_parameters {:deleted true} where-param))))

(defn- sort-classes
  [classes]
  (let [env-name-comp (fn [[env1 name1]
                           [env2 name2]]
                        (if (not= env1 env2)
                          (compare env1 env2)
                          (compare name1 name2)))]
    (sort-by (juxt :environment :name) env-name-comp classes)))

(sc/defn ^:always-validate synchronize-classes*
  [{db :db}
   puppet-classes :- [PuppetClass]]
  (jdbc/with-db-transaction
    [t-db db]
    ;; Nothing else should lock this table, so we can use NOWAIT
    (jdbc/execute! t-db ["LOCK TABLE classes, class_parameters IN EXCLUSIVE MODE NOWAIT"])
    (jdbc/execute! t-db ["SET CONSTRAINTS ALL IMMEDIATE"])
    (let [db-classes (get-all-classes t-db)
          [to-add to-delete in-both] (relative-complements-by-key (juxt :environment :name)
                                                                  (sort-classes puppet-classes)
                                                                  (sort-classes db-classes))]
      (doseq [{:keys [environment name] :as c} to-delete]
        (let [where-class (sql/where {:environment_name environment, :name name})
              [class-used?] (jdbc/query t-db (sql/select ["1"]
                                                       :group_classes
                                                       (sql/where
                                                         {:environment_name environment
                                                          :class_name name})))]
          (if class-used?
            (mark-class-deleted t-db c)
            (try (delete-class* {:db t-db} environment name)
              (catch PSQLException e
                (when-not (= (.getSQLState e) foreign-key-violation-code)
                  (throw e))
                (mark-class-deleted t-db c))))))
      (doseq [class to-add]
        (create-class* {:db t-db} class))
      (doseq [[new-class old-class] in-both]
        (when-not (= new-class old-class)
          (update-class t-db new-class old-class))))))

;; Rules
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn ^:always-validate create-rule :- Rule
  [db {:keys [when group-id] :as rule} :- Rule]
  (jdbc/with-db-transaction
    [t-db db]
    (let [storage-rule {:group_id group-id
                        :match (json/generate-string when)}
          [inserted-rule] (jdbc/insert! t-db :rules storage-rule)
          rule-id (:id inserted-rule)]
      (assoc rule :id rule-id))))

(sc/defn ^:always-validate get-rules* :- [Rule]
  [{db :db}]
  (for [{:keys [match group_id]} (jdbc/query db (sql/select * :rules))]
    {:when (json/decode match)
     :group-id group_id}))

;; Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Retrieving Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def group-selection
  "SELECT g.name,
          g.id               AS id,
          g.environment_name AS environment,
          g.parent_id        AS parent,
          gv.variable        AS variable,
          gv.value           AS variable_value,
          gc.class_name      AS class,
          gcp.parameter      AS parameter,
          gcp.value          AS parameter_value,
          r.match            AS rule
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
    (map deserialize-rule)
    (keywordize-keys)))

(sc/defn ^:always-validate get-group* :- (sc/maybe Group)
  [{db :db}
   id :- (sc/either String UUID)]
  {:pre [(uuid? id)]}
  (let [[group] (aggregate-fields-into-groups (query db (select-group-by-id (->uuid id))))]
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
                         (->> (jdbc/query db (expand-seq-params
                                               ["SELECT name FROM classes
                                                WHERE name IN ? AND deleted = true" class-names]))
                           (map (comp keyword :name))))
        marked-params (if-not (or (empty? class-names) (empty? param-names))
                        (->> (jdbc/query
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

(sc/defn ^:always-validate get-parent :- Group
  [db, group :- Group]
  (get-group* {:db db} (:parent group)))

(sc/defn ^:always-validate get-ancestors* :- [Group]
  [{db :db} group :- Group]
  (if (= (:name group) (:parent group))
    []
    (loop [current (get-parent db group), ancestors []]
      ;; if current is = to last parent, it is its own parent, so it's the root
      (if (= current (last ancestors))
        ancestors
        (recur (get-parent db current) (conj ancestors current))))))

(sc/defn ^:always-validate get-immediate-children :- [Group]
  [db, group-id :- java.util.UUID]
  (->> (query db (select-group-children group-id))
    aggregate-fields-into-groups))

(sc/defn ^:always-validate get-subtree* :- HierarchyNode
  [{db :db}, group :- Group]
  (let [children (get-immediate-children db (:id group))]
    {:group group
     :children (->> children
                 (map (partial get-subtree* {:db db}))
                 set)}))


;; Creating & Updating Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- validate-group
  "Validates the classes and class parameters (including those inherited from
  ancestors) of a group and all its children."
  [db group]
  (let [parent (get-parent db group)
        ancestors (concat [parent] (get-ancestors* {:db db} parent))
        subtree (-> (get-subtree* {:db db} group)
                  (assoc :group group))
        classes (->> subtree
                  (flatten-tree-with :environment)
                  set
                  (mapcat (partial get-classes* {:db db})))
        vtree (class8n/validation-tree subtree classes (map group->classification ancestors))]
    (when-not (class8n/valid-tree? vtree)
      (throw+ {:kind ::missing-referents
               :tree vtree
               :ancestors ancestors
               :classes classes}))))

(sc/defn ^:always-validate create-group* :- Group
  [{db :db}
   {:keys [classes environment id parent variables] group-name :name :as group} :- Group]
  (jdbc/with-db-transaction
    [t-db db]
    (validate-group t-db group)
    (create-environment-if-missing {:db t-db} {:name environment})
    (jdbc/insert! t-db :groups
                  [:name :environment_name :id :parent_id]
                  (conj ((juxt :name :environment) group) id parent))
    (doseq [[v-key v-val] variables]
      (jdbc/insert! t-db :group_variables
                    [:variable :group_id :value]
                    [(name v-key) id (json/generate-string v-val)]))
    (doseq [class classes]
      (let [[class-name class-params] class]
        (jdbc/insert! t-db :group_classes
                      [:group_id :class_name :environment_name]
                      [id (name class-name) environment])
        (doseq [class-param class-params]
          (let [[param value] class-param]
            (jdbc/insert! t-db :group_class_parameters
                          [:parameter :class_name :environment_name :group_id :value]
                          [(name param) (name class-name) environment id value])))))
    (create-rule t-db {:when (:rule group), :group-id (:id group)}))
    group)

(defn- delete-group-class-link
  [db group-id class-name]
  (jdbc/delete! db, :group_classes
                (sql/where {"group_id" group-id
                            "class_name" class-name})))

(defn- delete-group-class-parameter
  [db group-id class-name parameter]
  (jdbc/delete! db, :group_class_parameters
                (sql/where {"group_id" group-id
                            "class_name" class-name
                            "parameter" parameter})))

(defn- update-group-class-parameter
  [db group-id class-name parameter value]
  (jdbc/update! db, :group_class_parameters, {:value value}
                (sql/where {"group_id" group-id
                            "class_name" class-name
                            "parameter" parameter})))

(defn- add-group-class-parameter
  [db group-id class-name environment-name parameter value]
  (jdbc/with-db-transaction [t-db db]
    ;; create the group<->class link in the group_classes table if necessary
    (jdbc/execute! t-db
                   ["INSERT INTO group_classes (group_id, class_name, environment_name)
                    SELECT ?, ?, ? WHERE NOT EXISTS
                      (SELECT 1 FROM group_classes
                       WHERE group_id = ? AND class_name = ? AND environment_name = ?)"
                    group-id class-name environment-name group-id class-name environment-name])
    (jdbc/insert! t-db :group_class_parameters
                  {:group_id group-id
                   :class_name class-name
                   :environment_name environment-name
                   :parameter parameter
                   :value value})))

(defn- update-group-classes
  [db extant delta]
  (let [group-id (:id extant)
        environment (:environment extant)]
    (jdbc/with-db-transaction [t-db db]
      (doseq [[class parameters] (:classes delta)
              :let [class-name (name class)]]
        (if (nil? parameters)
          (delete-group-class-link t-db group-id class-name)
          ;; otherwise handle each parameter individually
          (doseq [[parameter value] parameters
                  :let [parameter-name (name parameter)]]
            (cond
              (nil? value)
              (delete-group-class-parameter t-db group-id class-name parameter-name)

              ;; parameter is set in extant group, so update
              (not= (get-in extant [:classes class parameter] ::not-found) ::not-found)
              (update-group-class-parameter t-db, group-id, class-name, parameter-name, value)

              :otherwise ; parameter is not nil and not previously set, so insert
              (add-group-class-parameter t-db group-id, class-name, environment
                                         parameter-name, value))))))))

(defn- update-group-variables
  [db extant delta]
  (let [group-id (:id extant)]
    (jdbc/with-db-transaction [t-db db]
      (doseq [[variable value] (:variables delta)
              :let [variable-name (name variable)
                    variable-value (json/encode value)]]
        (cond
          (nil? value)
          (jdbc/delete! t-db :group_variables
                        (sql/where {"group_id" group-id, "variable" variable-name}))

          (not= (get-in extant [:variables variable] ::not-found) ::not-found)
          (jdbc/update! t-db :group_variables {:value variable-value}
                        (sql/where {"group_id" group-id, "variable" variable-name}))

          :otherwise ; new variable for the group
          (jdbc/insert! t-db :group_variables
                        {:group_id group-id, :variable variable-name :value, variable-value}))))))

(defn- update-group-environment
  [db extant delta]
  (let [old-env (:environment extant)
        new-env (:environment delta)
        group-id (:id extant)
        where-group-link (sql/where {"group_id" group-id})]
    (when (and new-env (not= new-env old-env))
      (jdbc/with-db-transaction [t-db db]
        (create-environment-if-missing {:db db} {:name new-env})
        (jdbc/update! t-db :groups {:environment_name new-env} (sql/where {"id" group-id}))
        (jdbc/update! t-db :group_classes {:environment_name new-env} where-group-link)
        (jdbc/update! t-db :group_class_parameters {:environment_name new-env} where-group-link)))))

(defn- update-group-parent
  [db extant delta]
  (let [new-parent (:parent delta)]
    (when (and new-parent (not= new-parent (:parent extant)))
      (jdbc/update! db :groups {:parent_id new-parent}
                    (sql/where {:id (:id delta)})))))

(defn- update-group-rule
  [db extant delta]
  (let [new-rule (:rule delta)]
    (when (and new-rule (not= new-rule (:rule extant)))
      (jdbc/update! db, :rules, {:match (json/generate-string new-rule)}
                    (sql/where {:group_id (:id delta)})))))

(defn- update-group-name
  [db extant delta]
  (let [new-name (:name delta)]
    (when (and new-name (not= new-name (:name extant)))
      (jdbc/update! db, :groups, {:name new-name}
                    (sql/where {:id (:id delta)})))))

(sc/defn ^:always-validate update-group* :- (sc/maybe Group)
  [{db :db}
   delta :- GroupDelta]
  (let [update-thunk #(jdbc/with-db-transaction [t-db db :isolation :repeatable-read]
                        (when-let [extant (get-group* {:db t-db} (:id delta))]
                          (validate-group t-db (merge-and-clean extant delta))
                          (update-group-classes t-db extant delta)
                          (update-group-variables t-db extant delta)
                          (update-group-environment t-db extant delta)
                          (update-group-parent t-db extant delta)
                          (update-group-rule t-db extant delta)
                          (update-group-name t-db extant delta)
                          ;; still in the transaction, so will see the updated rows
                          (get-group* {:db t-db} (:id delta))))]
    (loop [retries 3]
      (let [result (try (update-thunk)
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
  [{db :db}
   id :- (sc/either String UUID)]
  {:pre [(uuid? id)]}
  (let [uuid (->uuid id)]
    (when (= uuid root-group-uuid)
      (throw (IllegalArgumentException. "It is forbidden to delete the default group.")))
    (jdbc/delete! db :groups (sql/where {:id (->uuid id)}))))

;; Record & Storage Protocol Extension
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord Postgres [db])

(defn new-db [spec]
  (Postgres. spec))

(extend Postgres
  Storage

  {:create-node create-node*
   :get-node get-node*
   :get-nodes get-nodes*
   :delete-node delete-node*

   :create-group create-group*
   :get-group get-group*
   :get-groups get-groups*
   :annotate-group annotate-group*
   :get-ancestors get-ancestors*
   :get-subtree get-subtree*
   :update-group update-group*
   :delete-group delete-group*

   :create-class create-class*
   :get-class get-class*
   :get-classes get-classes*
   :synchronize-classes synchronize-classes*
   :delete-class delete-class*

   :get-rules get-rules*

   :create-environment create-environment*
   :get-environment get-environment*
   :get-environments get-environments*
   :delete-environment delete-environment*})
