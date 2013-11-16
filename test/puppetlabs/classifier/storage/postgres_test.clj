(ns puppetlabs.classifier.storage.postgres-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.postgres :refer :all]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.sql :as sql]
            [clojure.java.jdbc.ddl :as ddl]))

(def test-db {:subprotocol "postgresql"
              :subname "classifier_test"
              :user "classifier_test"
              :passwd "classifier_test"})

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
    (apply jdbc/db-do-commands db (map ddl/drop-table (seq tables)))))

(defn with-test-db
  "Fixture that sets up a cleanly initialized and migrated database"
  [f]
  (drop-public-tables test-db)
  (init-schema test-db)
  (f))

(use-fixtures :each with-test-db)

(deftest ^:database test-migration
  (testing "creates a groups table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups"])))))
  (testing "creates a nodes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups"]))))))

(deftest ^:database test-node
  (testing "inserts nodes"
    (create-node (new-db test-db) "test")
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM nodes"]))))))
