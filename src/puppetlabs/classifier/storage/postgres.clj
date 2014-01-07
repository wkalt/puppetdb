(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [java-jdbc.sql :as sql]
            [cheshire.core :as json]
            [migratus.core :as migratus]
            [schema.core :as sc]
            [puppetlabs.classifier.schema :refer [Group Node Rule Environment]]
            [puppetlabs.classifier.storage :refer [Storage]]))

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

(defn create-node* [{db :db} node]
  {:pre [sc/validate Node node]}
  (jdbc/insert! db :nodes node))

(sc/defn ^:always-validate get-node* :- (sc/maybe Node)
  [{db :db} node-name]
  {:pre [(string? node-name)]}
  (let [[node] (jdbc/query db (select-node node-name))]
    node))

(defn delete-node* [{db :db} node-name]
  {:pre [(string? node-name)]}
  (jdbc/delete! db :nodes (sql/where {:name node-name})))

(defn create-group* [{db :db} group]
  {:pre [(sc/validate Group group)]}
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/insert! t-db :groups (select-keys group [:name]))
    (doseq [class (:classes group)]
      (jdbc/insert! t-db :group_classes
                    [:group_name :class_name]
                    [(:name group) class]))))

(sc/defn ^:always-validate get-group* :- (sc/maybe Group)
  [{db :db} group-name]
  {:pre [(string? group-name)]}
  (let [result (jdbc/query db [(str
                                 "SELECT * FROM groups g"
                                 " LEFT OUTER JOIN group_classes gc"
                                 " ON g.name = gc.group_name"
                                 " WHERE g.name = ?")
                               group-name])]
    (if-not (empty? result)
      {:name group-name
       :classes (for [r result
                      :let [class (:class_name r)]
                      :when class]
                  class)})))

(defn delete-group* [{db :db} group-name]
  (jdbc/delete! db :groups (sql/where {:name group-name})))

(defn create-class*
  [{db :db}
   {:keys [name parameters environment] :as class}]
  {:pre [(sc/validate PuppetClass class)]}
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/insert! t-db :classes {:name name :environment_name environment})
    (doseq [[param value] parameters]
      (jdbc/insert! t-db :class_parameters
                    [:class_name :parameter :default_value]
                    [name (clojure.core/name param) value]))))

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

(defn create-rule*
  [{db :db}  {:keys [when groups] :as rule}]
  {:pre [(sc/validate Rule rule)]}
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

(defn create-environment*
  [{db :db} environment]
  {:pre [sc/validate Environment environment]}
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
