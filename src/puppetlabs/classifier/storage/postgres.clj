(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.walk :refer [keywordize-keys]]
            [java-jdbc.sql :as sql]
            [cheshire.core :as json]
            [migratus.core :as migratus]
            [schema.core :as sc]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.classifier.schema :refer [Group GroupDelta Node Rule Environment]]
            [puppetlabs.classifier.storage :refer [Storage]]
            [puppetlabs.classifier.storage.sql-utils :refer [aggregate-column aggregate-submap-by]]
            [puppetlabs.classifier.util :refer [merge-and-clean uuid? ->uuid relative-complements-by-key]])
  (:import org.postgresql.util.PSQLException
           java.util.UUID))

(def ^:private PuppetClass puppetlabs.classifier.schema/Class)

(def foreign-key-violation-code "23503")
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
  [{db :db} node-name]
  {:pre [(string? node-name)]}
  (let [[node] (jdbc/query db (select-node node-name))]
    node))

(sc/defn ^:always-validate get-nodes* :- [Node]
  [{db :db}]
  (jdbc/query db (sql/select :name :nodes)))

(defn delete-node* [{db :db} node-name]
  {:pre [(string? node-name)]}
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

(defn delete-environment* [{db :db} environment-name]
  {:pre [(string? environment-name)]}
  (jdbc/delete! db :environments (sql/where {:name environment-name})))

;; Rules
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn ^:always-validate create-rule :- Rule
  [db {:keys [when group-name] :as rule} :- Rule]
  {:pre [(contains? rule :group-name)]}
  (jdbc/with-db-transaction
    [t-db db]
    (let [storage-rule {:group_name group-name
                        :match (json/generate-string when)}
          [inserted-rule] (jdbc/insert! t-db :rules storage-rule)
          rule-id (:id inserted-rule)]
      (assoc rule :id rule-id))))

(sc/defn ^:always-validate get-rules* :- [Rule]
  [{db :db}]
  (for [{:keys [match group_name]} (jdbc/query db (sql/select * :rules))]
    {:when (json/decode match)
     :group-name group_name}))

;; Groups
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def group-selection
  "SELECT name,
          g.id               AS id,
          g.environment_name AS environment,
          gv.variable        AS variable,
          gv.value           AS variable_value,
          gc.class_name      AS class,
          gcp.parameter      AS parameter,
          gcp.value          AS parameter_value,
          r.match            AS rule
  FROM groups g
       LEFT OUTER JOIN group_classes gc ON g.name = gc.group_name
       LEFT OUTER JOIN group_class_parameters gcp ON gc.group_name = gcp.group_name AND gc.class_name = gcp.class_name
       LEFT OUTER JOIN group_variables gv ON g.name = gv.group_name
       LEFT OUTER JOIN rules r ON g.name = r.group_name ")

(defn- group-selection-with-order
  [selection]
  (str selection " ORDER BY g.name, r.id ASC"))

(defn select-group-by-name
  [group-name]
  [(group-selection-with-order (str group-selection "WHERE g.name = ?"))
   group-name])

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

(defn- deserialize-rules
  [row]
  (let [deserialize-rule (fn [rule-str] (if-let [condition (json/decode rule-str)]
                                          {:when condition}))]
    (update-in row [:rule] deserialize-rule)))

(defn- aggregate-fields-into-groups
  [result]
  (->> result
    (aggregate-submap-by :parameter :parameter_value :parameters)
    (aggregate-submap-by :class :parameters :classes)
    (aggregate-submap-by :variable :variable_value :variables)
    (map deserialize-variable-values)
    (map deserialize-rules)
    (keywordize-keys)))

(sc/defn ^:always-validate get-group-by-id* :- (sc/maybe Group)
  [{db :db}
   id :- (sc/either String UUID)]
  {:pre [(uuid? id)]}
  (let [[group] (aggregate-fields-into-groups (query db (select-group-by-id (->uuid id))))]
    group))

(sc/defn ^:always-validate get-group-by-name* :- (sc/maybe Group)
  [{db :db}
   group-name :- String]
  (let [[group] (aggregate-fields-into-groups (query db (select-group-by-name group-name)))]
    group))

(sc/defn ^:always-validate get-groups* :- [Group]
  [{db :db}]
  (aggregate-fields-into-groups (query db (select-all-groups))))

(sc/defn ^:always-validate create-group* :- Group
  [{db :db}
   {:keys [classes environment variables] group-name :name :as group} :- Group]
  (when (uuid? group-name)
    (throw (IllegalArgumentException. "group's name cannot be a UUID")))
  (let [uuid (UUID/randomUUID)]
    (jdbc/with-db-transaction
      [t-db db]
      (create-environment-if-missing {:db t-db} {:name environment})
      (jdbc/insert! t-db :groups
                    [:name :environment_name :id]
                    (conj ((juxt :name :environment) group) uuid))
      (doseq [[v-key v-val] variables]
        (jdbc/insert! t-db :group_variables
                      [:variable :group_name :value]
                      [(name v-key) group-name (json/generate-string v-val)]))
      (doseq [class classes]
        (let [[class-name class-params] class]
          (jdbc/insert! t-db :group_classes
                        [:group_name :class_name :environment_name]
                        [group-name (name class-name) environment])
          (doseq [class-param class-params]
            (let [[param value] class-param]
              (jdbc/insert! t-db :group_class_parameters
                            [:parameter :class_name :environment_name :group_name :value]
                            [(name param) (name class-name) environment group-name value])))))
      (create-rule t-db (assoc (:rule group) :group-name group-name)))
    (assoc group :id uuid)))

(defn- delete-group-class-link
  [db group-name class-name environment-name]
  (doseq [table [:group_class_parameters :group_classes]]
    (jdbc/delete! db table
                  ["group_name = ? AND class_name = ? AND environment_name = ?"
                   group-name class-name environment-name])))

(defn- delete-group-class-parameter
  [db group-name class-name environment-name parameter]
  (jdbc/with-db-transaction [t-db db]
   (jdbc/delete! t-db :group_class_parameters
                            (sql/where {"group_name" group-name
                                        "class_name" class-name
                                        "environment_name" environment-name
                                        "parameter" parameter}))))

(defn- update-group-class-parameter
  [db group-name class-name environment-name parameter value]
  (jdbc/with-db-transaction [t-db db]
    (jdbc/update! t-db :group_class_parameters {:value value}
                  (sql/where {"group_name" group-name
                              "class_name" class-name
                              "environment_name" environment-name
                              "parameter" parameter}))))

(defn- add-group-class-parameter
  [db group-name class-name environment-name parameter value]
  (jdbc/with-db-transaction [t-db db]
    (jdbc/execute! t-db
                   ["INSERT INTO group_classes (group_name, class_name, environment_name) SELECT ?, ?, ?
                    WHERE NOT EXISTS (SELECT 1 FROM group_classes
                    WHERE group_name = ? AND class_name = ? AND environment_name = ?)"
                    group-name class-name environment-name group-name class-name environment-name])
    (jdbc/insert! t-db :group_class_parameters
                  {:group_name group-name
                   :class_name class-name
                   :environment_name environment-name
                   :parameter parameter
                   :value value})))

(defn- update-group-classes
  [db extant delta]
  (let [group-name (:name extant)
        environment (:environment extant)]
    (jdbc/with-db-transaction [t-db db]
      (doseq [[class parameters] (:classes delta)
              :let [class-name (name class)]]
        (if (nil? parameters)
          (delete-group-class-link t-db group-name class-name environment)
          ;; otherwise handle each parameter individually
          (doseq [[parameter value] parameters
                  :let [parameter-name (name parameter)]]
            (cond
              (nil? value)
              (delete-group-class-parameter t-db group-name class-name environment parameter-name)

              (get-in extant [:classes class parameter]) ; parameter is set in extant group, so update
              (update-group-class-parameter t-db, group-name, class-name, environment
                                            parameter-name, value)

              :otherwise ; parameter is not nil and not previously set, so insert
              (add-group-class-parameter t-db group-name, class-name, environment
                                         parameter-name, value))))))))

(defn- update-group-variables
  [db extant delta]
  (let [group-name (:name extant)]
    (jdbc/with-db-transaction [t-db db]
      (doseq [[variable value] (:variables delta)
              :let [variable-name (name variable)
                    variable-value (json/encode value)]]
        (cond
          (nil? value)
          (jdbc/delete! t-db :group_variables
                        (sql/where {"group_name" group-name, "variable" variable-name}))

          (get-in extant [:variables variable])
          (jdbc/update! t-db :group_variables {:value variable-value}
                        (sql/where {"group_name" group-name, "variable" variable-name}))

          :otherwise ; new variable for the group
          (jdbc/insert! t-db :group_variables
                        {:group_name group-name, :variable variable-name :value, variable-value}))))))

(defn- update-group-environment
  [db extant delta]
  (let [old-env (:environment extant)
        new-env (:environment delta)
        group-name (:name extant)
        where-group (sql/where {"group_name" group-name})]
    (when (and new-env (not= new-env old-env))
      (jdbc/with-db-transaction [t-db db]
        (create-environment-if-missing {:db db} {:name new-env})
        (jdbc/update! t-db :groups {:environment_name new-env} (sql/where {"name" group-name}))
        (jdbc/update! t-db :group_classes {:environment_name new-env} where-group)
        (jdbc/update! t-db :group_class_parameters {:environment_name new-env} where-group)))))

(defn- update-group-rule
  [db extant delta]
  (when-let [new-rule (:rule delta)]
    (jdbc/update! db
                  :rules {:match (json/generate-string (:when new-rule))}
                  (sql/where {:group_name (:name extant)}))))

(sc/defn ^:always-validate update-group* :- (sc/maybe Group)
  [{db :db}
   delta :- GroupDelta]
  (let [group-name (:name delta)
        update-thunk #(jdbc/with-db-transaction [t-db db :isolation :repeatable-read]
                        (when-let [extant (if-let [group-name (:name delta)]
                                            (get-group-by-name* {:db t-db} group-name)
                                            (get-group-by-id* {:db t-db} (:id delta)))]
                          (update-group-classes t-db extant delta)
                          (update-group-variables t-db extant delta)
                          (update-group-environment t-db extant delta)
                          (update-group-rule t-db extant delta)
                          ;; still in the transaction, so will see the updated rows
                          (get-group-by-name* {:db t-db} (:name extant))))]
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

(sc/defn ^:always-validate delete-group-by-id*
  [{db :db}
   id :- (sc/either String UUID)]
  {:pre [(uuid? id)]}
  (jdbc/delete! db :groups (sql/where {:id (->uuid id)})))

(sc/defn ^:always-validate delete-group-by-name*
  [{db :db}
   group-name :- String]
  (jdbc/delete! db :groups (sql/where {:name group-name})))

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
    (let [parameter (clojure.core/name parameter)
          [deleted] (jdbc/query t-db (sql/select [:parameter]
                                                 :class_parameters
                                                 (sql/where {:parameter parameter
                                                             :class_name class-name
                                                             :environment_name environment-name
                                                             :deleted true})))]
      (if deleted
        (jdbc/update! t-db :class_parameters
                      {:default_value default-value
                       :deleted false}
                      (sql/where {:parameter parameter
                                  :class_name class-name
                                  :environment_name environment-name}))
        (jdbc/insert! t-db :class_parameters
                      {:class_name class-name
                       :parameter parameter
                       :default_value default-value
                       :environment_name environment-name
                       :deleted false})))))

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
  [{db :db} environment-name class-name]
  (let [result (jdbc/query db (select-class class-name environment-name))]
    (if-not (empty? result)
      (->> result
        (aggregate-submap-by :parameter :default_value :parameters)
        (map keywordize-parameters)
        first))))

(sc/defn ^:always-validate get-classes* :- [PuppetClass]
  [{db :db} environment-name]
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
                                                                  puppet-classes
                                                                  db-classes)]
      (doseq [{:keys [environment name]} to-delete]
        (let [where-class (sql/where {:environment_name environment, :name name})
              [class-used?] (jdbc/query t-db (sql/select ["1"]
                                                       :group_classes
                                                       (sql/where
                                                         {:environment_name environment
                                                          :class_name name})))]
          (if class-used?
            (jdbc/update! t-db :classes {:deleted true} where-class)
            (try (jdbc/delete! t-db :classes where-class)
              (catch PSQLException e
                (when-not (= (.getSQLState e) foreign-key-violation-code)
                  (throw e))
                (jdbc/update! t-db :classes {:deleted true} where-class))))))
      (doseq [class to-add]
        (create-class* {:db t-db} class))
      (doseq [[new-class old-class] in-both]
        (when-not (= new-class old-class)
          (update-class t-db new-class old-class))))))

(defn delete-class* [{db :db} environment-name class-name]
  (jdbc/delete! db :classes (sql/where {:name class-name, :environment_name environment-name})))

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
   :get-group-by-id get-group-by-id*
   :get-group-by-name get-group-by-name*
   :get-groups get-groups*
   :update-group update-group*
   :delete-group-by-id delete-group-by-id*
   :delete-group-by-name delete-group-by-name*

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
