(ns puppetlabs.classifier.storage.postgres-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.postgres :refer :all]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.sql :as sql]
            [clojure.java.jdbc.ddl :as ddl]))

(def test-db {:subprotocol "postgresql"
              :subname (or (System/getenv "CLASSIFIER_DBNAME") "classifier_test")
              :user (or (System/getenv "CLASSIFIER_DBUSER") "classifier_test")
              :password (or (System/getenv "CLASSIFIER_DBPASS") "classifier_test")})

(defn with-test-db
  "Fixture that sets up a cleanly initialized and migrated database"
  [f]
  (drop-public-tables test-db)
  (migrate test-db)
  (f))

(defn throw-next-exception
  [ex]
  (prn ex)
  (cond (.getCause ex) (throw-next-exception (.getCause ex))
        (.getNextException ex) (throw-next-exception (.getNextException ex))
        :else (throw ex)))

(defn expand-sql-exceptions
  [f]
  (try
    (f)
    (catch Throwable t
      (throw-next-exception t))))

(def db-fixtures
  (compose-fixtures expand-sql-exceptions with-test-db))

(use-fixtures :each db-fixtures)

(deftest ^:database test-migration
  (testing "creates a groups table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups"])))))
  (testing "creates a nodes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM nodes"])))))
  (testing "creates a classes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes"])))))
  (testing "creates a class parameters table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM class_parameters"]))))))

(deftest ^:database test-node
  (testing "inserts nodes"
    (create-node (new-db test-db) {:name "test"})
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM nodes"])))))
  (testing "retrieves a node"
    (is (= {:name "test"} (get-node (new-db test-db) "test"))))
  (testing "deletes a node"
    (delete-node (new-db test-db) "test")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM nodes"]))))))

(deftest ^:database groups
  (testing "insert a group"
    (create-group (new-db test-db) {:name "test"})
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups"])))))
  (testing "retrieves a group"
    (is (= {:name "test"} (get-group (new-db test-db) "test"))))
  (testing "deletes a group"
    (delete-group (new-db test-db) "test")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups"]))))))

(deftest ^:database classes
  (testing "store a class with no parameters"
    (create-class (new-db test-db) {:name "myclass" :parameters {}}))
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM classes"]))))
  (testing "store a class with multiple parameters"
    (create-class (new-db test-db) {:name "classtwo"
                                    :parameters {:param1 "value1"
                                                 :param2 "value2"}})
    (is (= 2 (count (jdbc/query test-db
      ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "classtwo"])))))
  (testing "retrieve a class with no parameters"
    (let [testclass {:name "noclass" :parameters {}}]
      (create-class (new-db test-db) testclass)
      (is (= testclass (get-class (new-db test-db) "noclass")))))
  (testing "retrieve a class with parameters"
    (let [testclass {:name "testclass" :parameters {"p1" "v1" "p2" "v2"}}]
      (create-class (new-db test-db) testclass)
      (is (= testclass (get-class (new-db test-db) "testclass")))))
  (testing "deletes a class with no paramters"
    (delete-class (new-db test-db) "noclass")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "noclass"])))))
  (testing "deletes a class with parameters"
    (delete-class (new-db test-db) "testclass")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "testclass"]))))
    (is (= 0 (count (jdbc/query test-db
      ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "testclass"]))))))
