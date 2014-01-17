(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
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

(defn select-group [group]
  (sql/select :name :groups (sql/where {:name group})))

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

(def select-group
  "SELECT name,
          g.environment_name AS environment,
          gc.class_name AS class,
          gcp.parameter AS parameter,
          gcp.value AS value
  FROM groups g
       LEFT OUTER JOIN group_classes gc ON g.name = gc.group_name
       LEFT OUTER JOIN group_class_parameters gcp ON gc.group_name = gcp.group_name AND gc.class_name = gcp.class_name
  WHERE g.name = ?")

(sc/defn ^:always-validate get-group* :- (sc/maybe Group)
  [{db :db}
   group-name :- String]
  (let [[result] (->> (query db [select-group group-name])
                   (aggregate-submap-by :parameter :value :parameters)
                   (aggregate-submap-by :class :parameters :classes)
                   (keywordize-keys))]
    result))

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

(sc/defn ^:always-validate get-class* :- (sc/maybe PuppetClass)
  [{db :db} class-name]
  (let [result (jdbc/query db [(str
                                 "SELECT * FROM classes c"
                                 " LEFT OUTER JOIN class_parameters cp"
                                 " ON c.name = cp.class_name"
                                 " WHERE c.name = ?")
                               class-name])]
    (if-not (empty? result)
      {:name class-name
       :environment (:environment_name (first result))
       :parameters (extract-parameters result)})))

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
   :delete-node delete-node*
   :create-group create-group*
   :get-group get-group*
   :delete-group delete-group*
   :create-class create-class*
   :get-class get-class*
   :delete-class delete-class*
   :create-rule create-rule*
   :get-rules get-rules*
   :create-environment create-environment*
   :get-environment get-environment*
   :delete-environment delete-environment*})
