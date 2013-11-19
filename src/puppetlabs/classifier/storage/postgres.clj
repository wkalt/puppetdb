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
      ["name" "TEXT" "PRIMARY KEY"])))

(defn select-node [node]
  (sql/select :name :nodes (sql/where {:name node})))

(defn select-group [group]
  (sql/select :name :groups (sql/where {:name group})))

(deftype Postgres [db]
  Storage
  
  (create-node [_ node]
    (jdbc/insert! db :nodes {:name node}))

  (get-node [_ node]
    (let [result (jdbc/query db (select-node node))]
      (:name (first result))))

  (create-group [_ group]
    (jdbc/insert! db :groups {:name group}))

  (get-group [_ group]
    (let [result (jdbc/query db (select-group group))]
      (:name (first result)))))

(defn new-db [spec]
  (Postgres. spec))
