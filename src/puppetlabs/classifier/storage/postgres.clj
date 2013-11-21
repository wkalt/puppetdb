(ns puppetlabs.classifier.storage.postgres
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.ddl :as ddl]
            [clojure.java.jdbc.sql :as sql]
            [puppetlabs.classifier.storage :refer [Storage]]))

(defn init-schema [db]
  (jdbc/db-do-commands
    db false
    (ddl/create-table
      :nodes
      ["name" "TEXT" "PRIMARY KEY"])
    (ddl/create-table
      :groups
      ["name" "TEXT" "PRIMARY KEY"])
    (ddl/create-table
      :classes
      ["name" "TEXT" "PRIMARY KEY"])
    (ddl/create-table
      :class_parameters
      ["parameter" "TEXT"]
      ["default_value" "TEXT"]
      ["class_name" "TEXT" "REFERENCES classes(name)" "ON DELETE CASCADE"]
      ["PRIMARY KEY(class_name, parameter)"])))

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
  (jdbc/insert! db :groups group))

(defn get-group* [{db :db} group-name]
  {:pre [(string? group-name)]}
  (let [result (jdbc/query db (select-group group-name))]
    (first result)))

(defn delete-group* [{db :db} group-name]
  (jdbc/delete! db :groups (sql/where {:name group-name})))

(defn create-class* [{db :db} {:keys [name parameters]}]
  (jdbc/db-transaction
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
  (when-let [result (seq (jdbc/query db
                  [(str
                    "SELECT * FROM classes c"
                    " LEFT OUTER JOIN class_parameters cp"
                    " ON c.name = cp.class_name"
                    " WHERE c.name = ?")
                  class-name]))]
    {:name class-name :parameters (extract-parameters result)}))

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
   :get-class get-class*})

