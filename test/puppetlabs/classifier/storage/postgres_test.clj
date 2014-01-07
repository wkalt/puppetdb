(ns puppetlabs.classifier.storage.postgres-test
  (:require [clojure.test :refer :all]
            [clojure.set :refer [project]]
            [schema.test]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.postgres :refer :all]
            [clojure.java.jdbc :as jdbc]))

(def test-db {:subprotocol "postgresql"
              :subname (or (System/getenv "CLASSIFIER_DBNAME") "classifier_test")
              :user (or (System/getenv "CLASSIFIER_DBUSER") "classifier_test")
              :password (or (System/getenv "CLASSIFIER_DBPASS") "classifier_test")})

(def db (new-db test-db))

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

(use-fixtures :once schema.test/validate-schemas)

(deftest ^:database test-migration
  (testing "creates a groups table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups"])))))
  (testing "creates a nodes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM nodes"])))))
  (testing "creates a classes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes"])))))
  (testing "creates a class parameters table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM class_parameters"])))))
  (testing "creates a rules table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM rules"]))))))

(deftest ^:database test-node
  (testing "inserts nodes"
    (create-node db {:name "test"})
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM nodes"])))))
  (testing "retrieves a node"
    (is (= {:name "test"} (get-node db "test"))))
  (testing "deletes a node"
    (delete-node db "test")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM nodes"]))))))

(deftest ^:database groups
  (testing "insert a group"
    (create-group db {:name "test" :classes []})
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups"])))))
  (testing "store a group with multiple classes"
    (create-environment db {:name "test"})
    (create-class db {:name "hi" :parameters {} :environment "test"})
    (create-class db {:name "bye" :parameters {} :environment "test"})
    (create-group db {:name "group-two"
                      :classes ["hi", "bye"]})
    (is (= 2 (count (jdbc/query test-db
                                ["SELECT * FROM groups g join group_classes gc on gc.group_name = g.name WHERE g.name = ?" "group-two"])))))
  (testing "retrieves a group"
    (is (= {:name "test" :classes []} (get-group db "test"))))
  (testing "retrieves a group with classes"
    (is (= {:name "group-two" :classes ["hi" "bye"]}
           (get-group db "group-two"))))
  (testing "deletes a group"
    (delete-group db "test")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups WHERE name = ?" "test"]))))))

(deftest ^:database classes
  (create-environment db {:name "test"})
  (testing "store a class with no parameters"
    (create-class db {:name "myclass" :parameters {} :environment "test"}))
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM classes"]))))
  (testing "store a class with multiple parameters"
    (create-class db {:name "classtwo"
                                    :parameters {:param1 "value1"
                                                 :param2 "value2"}
                                    :environment "test"})
    (is (= 2 (count (jdbc/query test-db
      ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "classtwo"])))))
  (testing "retrieve a class with no parameters"
    (let [testclass {:name "noclass" :parameters {} :environment "test"}]
      (create-class db testclass)
      (is (= testclass (get-class db "noclass")))))
  (testing "retrieve a class with parameters"
    (let [testclass {:name "testclass" :parameters {:p1 "v1" :p2 "v2"} :environment "test"}]
      (create-class db testclass)
      (is (= testclass (get-class db "testclass")))))
  (testing "deletes a class with no paramters"
    (delete-class db "noclass")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "noclass"])))))
  (testing "deletes a class with parameters"
    (delete-class db "testclass")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "testclass"]))))
    (is (= 0 (count (jdbc/query test-db
      ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "testclass"]))))))

(deftest ^:database rules
  (let [hello-group {:name "hello" :classes ["salutation"]}
        goodbye-group {:name "goodbye" :classes ["valediction"]}
        salutation-class {:name "salutation" :parameters {} :environment "production"}
        valediction-class {:name "valediction" :parameters {} :environment "production"}
        test-rule-1 {:when ["=" "name" "test"]
                     :groups []}
        test-rule-2 {:when ["=" "name" "bar"]
                     :groups ["hello" "goodbye"]}]
    (testing "creates a rule"
      (create-rule db test-rule-1)
      (is (= 1 (count (jdbc/query test-db ["SELECT * FROM rules"])))))
    (testing "creates a rule with groups"
      (create-class db salutation-class)
      (create-class db valediction-class)
      (create-group db hello-group)
      (create-group db goodbye-group)
      (create-rule db test-rule-2)
      (is (= 2 (count (jdbc/query test-db ["SELECT * FROM rule_groups"])))))
    (testing "retrieves all rules"
      (is (= #{test-rule-1 test-rule-2}
             (project (get-rules db) [:when :groups]))))))

(deftest ^:database environments
  (let [test-env {:name "test"}]
    (testing "creates an environment"
      (create-environment db test-env)
      (is (= 1 (count (jdbc/query test-db ["SELECT * FROM environments"])))))
    (testing "retrieves an environment"
      (is (= test-env (get-environment db "test"))))
    (testing "deletes an environment"
      (delete-environment db "test")
      (is (= 0 (count (jdbc/query test-db ["SELECT * FROM environments"])))))))
