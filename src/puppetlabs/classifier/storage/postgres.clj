(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [java-jdbc.sql :as sql]
            [puppetlabs.classifier.storage :refer [Storage]]
            [migratus.core :as migratus]
            [cheshire.core :as json]))

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
  {:pre [(map? node)]}
  (jdbc/insert! db :nodes node))

(defn get-node* [{db :db} node-name]
  {:pre [(string? node-name)]
   :post [map?]}
  (let [result (jdbc/query db (select-node node-name))]
    (first result)))

(defn delete-node* [{db :db} node-name]
  {:pre [(string? node-name)]}
  (jdbc/delete! db :nodes (sql/where {:name node-name})))

(defn create-group* [{db :db} group]
  {:pre [(map? group)]}
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/insert! t-db :groups (select-keys group [:name]))
    (doseq [class (:classes group)]
      (jdbc/insert! t-db :group_classes [:group_name :class_name] [(:name group) class]))))

(defn get-group* [{db :db} group-name]
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

(defn create-class* [{db :db} {:keys [name parameters]}]
  (jdbc/with-db-transaction
    [t-db db]
    (jdbc/insert! t-db :classes {:name name})
    (doseq [[param value] parameters]
      (jdbc/insert! t-db :class_parameters
                    [:class_name :parameter :default_value]
                    [name (clojure.core/name param) value]))))

(defn extract-parameters [result]
    (let [parameters
          (into {} (map (juxt :parameter :default_value) result))]
      (if (= parameters {nil nil})
             {}
             parameters)))

(defn get-class* [{db :db} class-name]
  (let [result (jdbc/query db [(str
                                 "SELECT * FROM classes c"
                                 " LEFT OUTER JOIN class_parameters cp"
                                 " ON c.name = cp.class_name"
                                 " WHERE c.name = ?")
                               class-name])]
    (if-not (empty? result)
      {:name class-name :parameters (extract-parameters result)})))

(defn delete-class* [{db :db} class-name]
  (jdbc/delete! db :classes (sql/where {:name class-name})))

(defn create-rule*
  [{db :db}
   {:keys [when groups]}]
  (jdbc/with-db-transaction
    [t-db db]
    (let [rule (jdbc/insert! t-db :rules {:match (json/generate-string when)})
          rule-id (:id (first rule))]
      (doseq [group groups]
        (jdbc/insert! t-db :rule_groups {:rule_id rule-id :group_name group})))))

(defn- group-rule [[[rule-id match] records]]
  (let [groups (map :group_name records)]
    {:id rule-id
     :when (json/parse-string match)
     :groups (remove nil? groups)}))

(defn get-rules* [{db :db}]
  (let [result (jdbc/query db
          ["SELECT * FROM rules r LEFT OUTER JOIN rule_groups g ON r.id = g.rule_id"])
        rules (group-by (juxt :id :match) result)]
    (map group-rule rules)))

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
   :get-rules get-rules*})

