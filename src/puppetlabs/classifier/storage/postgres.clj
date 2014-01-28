(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.walk :refer [keywordize-keys]]
            [java-jdbc.sql :as sql]
            [cheshire.core :as json]
            [migratus.core :as migratus]
            [schema.core :as sc]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.classifier.schema :refer [Group Node Rule Environment]]
            [puppetlabs.classifier.storage :refer [Storage]]
            [puppetlabs.classifier.storage.sql-utils :refer [aggregate-submap-by]]))

(def ^:private PuppetClass puppetlabs.classifier.schema/Class)

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

(defn select-node [node]
  (sql/select :name :nodes (sql/where {:name node})))

;;; Storage protocol implementation
;;;
;;; Functions here are referred to below when calling extend. This indirection
;;; lets us use pre- and post-conditions as well as schema.

(sc/defn ^:always-validate create-node*
  [{db :db}
   node :- Node]
  (jdbc/insert! db :nodes node))

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

(sc/defn ^:always-validate create-group*
  [{db :db}
   {:keys [environment] group-name :name :as group} :- Group]
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/insert! t-db :groups
                  [:name :environment_name]
                  ((juxt :name :environment) group))
    (doseq [[v-key v-val] (:variables group)]
      (jdbc/insert! t-db :group_variables
                    [:variable :group_name :value]
                    [(name v-key) group-name (json/generate-string v-val)]))
    (doseq [class (:classes group)]
      (let [[class-name class-params] class]
        (jdbc/insert! t-db :group_classes
                      [:group_name :class_name :environment_name]
                      [group-name (name class-name) environment])
        (doseq [class-param class-params]
          (let [[param value] class-param]
            (jdbc/insert! t-db :group_class_parameters
                          [:parameter :class_name :environment_name :group_name :value]
                          [(name param) (name class-name) environment group-name value])))))))

(def group-selection
  "SELECT name,
          g.environment_name AS environment,
          gv.variable        AS variable,
          gv.value           AS variable_value,
          gc.class_name      AS class,
          gcp.parameter      AS parameter,
          gcp.value          AS parameter_value
  FROM groups g
       LEFT OUTER JOIN group_classes gc ON g.name = gc.group_name
       LEFT OUTER JOIN group_class_parameters gcp ON gc.group_name = gcp.group_name AND gc.class_name = gcp.class_name
       LEFT OUTER JOIN group_variables gv ON g.name = gv.group_name")

(defn select-group
  [group-name]
  [(str group-selection " WHERE g.name = ?") group-name])

(defn select-all-groups []
  [group-selection])

(defn- deserialize-variable-values
  "This function expects a row map that has already had its variable-related
  fields aggregated into a submap. It deserializes the values of the row's
  variables submap (which are expected to be JSON)."
  [row]
  (update-in row [:variables]
             (fn [variables]
               (into {} (for [[k v] variables]
                          [k (json/parse-string v)])))))

(defn- aggregate-fields-into-groups
  [result]
  (->> result
    (aggregate-submap-by :parameter :parameter_value :parameters)
    (aggregate-submap-by :class :parameters :classes)
    (aggregate-submap-by :variable :variable_value :variables)
    (map deserialize-variable-values)
    (keywordize-keys)))

(sc/defn ^:always-validate get-group* :- (sc/maybe Group)
  [{db :db}
   group-name :- String]
  (let [[group] (aggregate-fields-into-groups (query db (select-group group-name)))]
    group))

(sc/defn ^:always-validate get-groups* :- [Group]
  [{db :db}]
  (aggregate-fields-into-groups (query db (select-all-groups))))

(defn delete-group* [{db :db} group-name]
  (jdbc/delete! db :groups (sql/where {:name group-name})))

(sc/defn ^:always-validate create-class*
  [{db :db}
   {:keys [name parameters environment] :as class} :- PuppetClass]
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/insert! t-db :classes {:name name :environment_name environment})
    (doseq [[param value] parameters]
      (jdbc/insert! t-db :class_parameters
                    [:class_name :parameter :default_value :environment_name]
                    [name (clojure.core/name param) value environment]))))

(defn extract-parameters [result]
  (into {} (for [[param default] (->> result
                                   (map (juxt :parameter :default_value))
                                   (remove (comp nil? first)))]
             [(keyword param) default])))

(def ^:private class-selection
  "SELECT name,
          c.environment_name AS environment,
          cp.parameter,
          cp.default_value
  FROM classes c
  LEFT OUTER JOIN class_parameters cp
    ON c.name = cp.class_name AND c.environment_name = cp.environment_name")

(defn select-class
  [class-name]
  [(str class-selection " WHERE c.name = ?") class-name])

(defn select-all-classes []
  [class-selection])

(defn- keywordize-parameters
  [class]
  (update-in class, [:parameters]
             (fn [params]
               (into {} (for [[param default] params]
                          [(keyword param) default]))) ))

(sc/defn ^:always-validate get-class* :- (sc/maybe PuppetClass)
  [{db :db} class-name]
  (let [result (jdbc/query db (select-class class-name))]
    (if-not (empty? result)
      (->> result
        (aggregate-submap-by :parameter :default_value :parameters)
        (map keywordize-parameters)
        first))))

(sc/defn get-classes* :- [PuppetClass]
  [{db :db}]
  (let [result (jdbc/query db (select-all-classes))]
    (->> result
      (aggregate-submap-by :parameter :default_value :parameters)
      (map keywordize-parameters))))

(defn delete-class* [{db :db} class-name]
  (jdbc/delete! db :classes (sql/where {:name class-name})))

(sc/defn ^:always-validate create-rule*
  [{db :db}
   {:keys [when groups] :as rule} :- Rule]
  (jdbc/with-db-transaction
    [t-db db]
    (let [storage-rule {:match (json/generate-string when)}
          [inserted-rule] (jdbc/insert! t-db :rules storage-rule)
          rule-id (:id inserted-rule)]
      (doseq [group groups]
        (jdbc/insert! t-db :rule_groups {:rule_id rule-id :group_name group})))))

(defn- group-rule [[[rule-id match] records]]
  (let [groups (map :group_name records)]
    {:id rule-id
     :when (json/parse-string match)
     :groups (remove nil? groups)}))

(sc/defn ^:always-validate get-rules* :- [Rule]
  [{db :db}]
  (let [result (jdbc/query db
          ["SELECT * FROM rules r LEFT OUTER JOIN rule_groups g ON r.id = g.rule_id"])
        rules (group-by (juxt :id :match) result)]
    (map group-rule rules)))

(sc/defn ^:always-validate create-environment*
  [{db :db}
   environment :- Environment]
  (jdbc/insert! db :environments environment))

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
   :delete-group delete-group*
   :create-class create-class*
   :get-class get-class*
   :get-classes get-classes*
   :delete-class delete-class*
   :create-rule create-rule*
   :get-rules get-rules*
   :create-environment create-environment*
   :get-environment get-environment*
   :get-environments get-environments*
   :delete-environment delete-environment*})
