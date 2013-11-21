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
      ; Need to add a composite key on parameter, class_name
      ["parameter" "TEXT"]
      ["default_value" "TEXT"]
      ["class_name" "TEXT" "REFERENCES classes(name)" "ON DELETE CASCADE"])))

(defn select-node [node]
  (sql/select :name :nodes (sql/where {:name node})))

(defn select-group [group]
  (sql/select :name :groups (sql/where {:name group})))

(deftype Postgres [db]
  Storage

  (create-node [_ node]
    {:pre [(map? node)]}
    (jdbc/insert! db :nodes node))

  (get-node [_ node-name]
    {:pre [(string? node-name)]
     :post [map?]}
    (let [result (jdbc/query db (select-node node-name))]
      (first result)))

  (delete-node [_ node-name]
    {:pre [(string? node-name)]}
    (jdbc/delete! db :nodes (sql/where {:name node-name})))

  (create-group [_ group]
    {:pre [(map? group)]}
    (jdbc/insert! db :groups group))

  (get-group [_ group-name]
    {:pre [(string? group-name)]}
    (let [result (jdbc/query db (select-group group-name))]
      (first result)))

  (delete-group [_ group-name]
    (jdbc/delete! db :groups (sql/where {:name group-name}))))

(defn new-db [spec]
  (Postgres. spec))
