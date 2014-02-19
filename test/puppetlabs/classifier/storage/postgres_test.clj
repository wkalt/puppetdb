(ns puppetlabs.classifier.storage.postgres-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [clojure.set :refer [project]]
            [schema.test]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.postgres :refer :all]
            [puppetlabs.classifier.util :refer [merge-and-clean]])
  (:import org.postgresql.util.PSQLException
           java.util.UUID))

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

(deftest ^:database migration
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

(deftest ^:database nodes
  (testing "inserts nodes"
    (create-node db {:name "test"})
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM nodes"])))))
  (testing "retrieves a node"
    (is (= {:name "test"} (get-node db "test"))))
  (testing "retrieves all nodes"
    (create-node db {:name "test2"})
    (create-node db {:name "test3"})
    (is (= #{"test" "test2" "test3"} (->> (get-nodes db) (map :name) set))))
  (testing "deletes a node"
    (delete-node db "test")
    (delete-node db "test2")
    (delete-node db "test3")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM nodes"]))))))

(defn- get-group-by-name-less-id
  [db group-name]
  (-> (get-group-by-name db group-name)
    (dissoc :id)))

(defn- get-groups-less-ids
  [db]
  (->> (get-groups db)
    (map #(dissoc % :id))))

(defn- update-group-less-id
  [db delta]
  (-> (update-group db delta)
    (dissoc :id)))

(deftest ^:database groups
  (let [simplest-group {:name "simplest" :classes {} :environment "test" :variables {}}
        group-with-classes {:name "with-classes"
                            :classes {:hi {} :bye {}}
                            :environment "test"
                            :variables {}}]

    (testing "inserts a group"
      (create-group db simplest-group)
      (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups"])))))

    (testing "does not insert a group that has a UUID for the name"
      (is (thrown? IllegalArgumentException
                   (create-group db (assoc simplest-group :name (str (UUID/randomUUID)))))))

    (testing "stores a group with multiple classes"
      (create-class db {:name "hi" :parameters {} :environment "test"})
      (create-class db {:name "bye" :parameters {} :environment "test"})
      (create-group db group-with-classes)
      (is (= 2 (count (jdbc/query test-db
                                  ["SELECT * FROM groups g join group_classes gc on gc.group_name = g.name WHERE g.name = ?" "with-classes"])))))

    (testing "retrieves a group"
      (is (= simplest-group (get-group-by-name-less-id db "simplest"))))

    (testing "retrieves a group by UUID"
      (let [simplest-group' (get-group-by-name db "simplest")
            uuid-str (-> simplest-group' :id str)]
        (is (= simplest-group' (get-group-by-id db (:id simplest-group'))))
        (is (= simplest-group' (get-group-by-id db uuid-str)))))

    (testing "retrieves a group with classes"
      (is (= group-with-classes (get-group-by-name-less-id db "with-classes"))))

    (testing "retrieves all groups"
      (is (= #{simplest-group group-with-classes} (set (get-groups-less-ids db)))))

    (testing "deletes a group"
      (delete-group-by-name db "simplest")
      (is (= 0 (count (jdbc/query test-db ["SELECT * FROM groups WHERE name = ?" "simplest"])))))))

(deftest ^:database create-missing-environments
  (let [c {:name "chrono-manipulator"
           :environment "thefuture"
           :parameters {}}
        g {:name "time-machines"
           :environment "rnd"
           :classes {}
           :variables {}}]
    (testing "creates missing environments on class insertion"
      (create-class db c)
      (is (= {:name "thefuture"} (get-environment db "thefuture"))))
    (testing "creates missing environments on group insertion"
      (create-group db g)
      (is (= {:name "rnd"} (get-environment db "rnd"))))))

(deftest ^:database complex-groups
  (let [envs ["test" "tropical"]
        first-class {:name "first"
                     :parameters {:one "one-def"
                                  :two nil}}
        second-class {:name "second"
                      :parameters {:three nil
                                   :four nil
                                   :five "default"}}
        g1 {:name "complex-group"
            :classes {:first {:one "one-val"
                              :two "two-val"}
                      :second {:three "three-val"
                               :four "four-val"}}
            :environment "test"
            :variables {:fqdn "www.example.com"
                        :ntp_servers ["0.pool.ntp.org" "ntp.example.com"]
                        :cluster_index 8
                        :some_bool false}}
        g2 {:name "another-complex-group"
            :environment "tropical"
            :classes {:first {:two "another-two-val"}
                      :second {:four "four-val"}}
            :variables {:island false, :rainforest true}}]

    (doseq [env envs, c [first-class second-class]]
      (create-class db (assoc c :environment env)))

    (testing "stores groups with class parameters and top-level variables"
      (create-group db g1)
      (create-group db g2)
      (is (= g1 (get-group-by-name-less-id db (:name g1))))
      (is (= g2 (get-group-by-name-less-id db (:name g2))))
      (is (= #{g1 g2} (set (get-groups-less-ids db)))))

    (testing "can update group classes, class parameters, and variables"
      (let [g1-delta {:name "complex-group"
                      :classes {:first nil
                                :second {:three nil
                                         :four "red fish"
                                         :five "blue fish"}}
                      :variables {:some_bool nil
                                  :cluster_index 4
                                  :cluster_size 16
                                  :spirit_animal "turtle"}}
            g1' (merge-and-clean g1 g1-delta)]
        (is (= g1' (update-group-less-id db g1-delta)))
        (is (= g1' (get-group-by-name-less-id db (:name g1))))))

    (testing "returns nil when attempting to update unknown group"
      (is (nil? (update-group db {:name "DNE"}))))

    (testing "can update group environments"
      (let [g2-env-change {:name "another-complex-group", :environment "test"}
            g2' (merge-and-clean g2 g2-env-change)]
        (is (= g2' (update-group-less-id db g2-env-change)))
        (is (= g2' (get-group-by-name-less-id db (:name g2))))))

    (testing "trying to update environments when the group refers to classes
             that don't exist in the new environment results in a foreign-key
             violation exception"
      (let [g2-bad-env-change {:name "another-complex-group"
                               :environment "dne"}
            e (try (do
                     (update-group db g2-bad-env-change)
                     nil)
                (catch PSQLException e
                  e))]
        (is (and e (= (.getSQLState e) "23503")))))))

(deftest ^:database classes
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
  (testing "retrieves all classes"
    (is (= #{"myclass" "classtwo" "testclass" "noclass"} (->> (get-classes db) (map :name) set))))
  (testing "deletes a class with no parameters"
    (delete-class db "noclass")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "noclass"])))))
  (testing "deletes a class with parameters"
    (delete-class db "testclass")
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "testclass"]))))
    (is (= 0 (count (jdbc/query test-db
      ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "testclass"]))))))

(deftest ^:database rules
  (let [hello-group {:name "hello" :classes {:salutation {}}  :environment "production" :variables {}}
        goodbye-group {:name "goodbye" :classes {:valediction {}} :environment "production" :variables {}}
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
  (let [test-env {:name "test"}
        other-env {:name "underwater"}]
    (testing "creates an environment"
      (create-environment db test-env)
      (let [envs (jdbc/query test-db ["SELECT * FROM environments WHERE name = ?" "test"])]
        (is (= 1 (count envs)))))
    (testing "retrieves an environment"
      (is (= test-env (get-environment db "test"))))
    (testing "retrieves all environments"
      (create-environment db other-env)
      (is (= #{"test" "underwater" "production"} (->> (get-environments db) (map :name) set))))
    (testing "deletes an environment"
      (delete-environment db "test")
      (let [[test-env] (jdbc/query test-db ["SELECT * FROM environments WHERE name = ?" "test"])]
        (is (nil? test-env))))))

(deftest ^:database synchronize
  (let [before [{:name "changed", :parameters {:changed "1", :unreferred "5", :referred "6"}, :environment "production"}
                {:name "referred", :parameters {}, :environment "production"}
                {:name "unreferred", :parameters {:a "a"}, :environment "production"}]
        after [{:name "added", :parameters {}, :environment "production"}
               {:name "changed", :parameters {:added "1", :changed "2"}, :environment "production"}]]
    (synchronize-classes db before)
    (create-group db {:name "referrer" :classes {:referred {}, :changed {:referred "hi"}}, :environment "production", :variables {}})
    (synchronize-classes db after)

    (testing "unreferred is deleted"
      (is (nil? (get-class db "unreferred"))))

    (testing "referred is marked deleted"
      (let [[result] (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "referred"])]
        (is (true? (:deleted result)))))

    (testing "unreferred parameters are deleted"
      (let [[result]
            (jdbc/query test-db
                        ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                         "changed" "unreferred"])]
        (is (nil? result))))

    (testing "referred parameters are marked deleted"
      (let [[result]
            (jdbc/query test-db
                        ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                         "changed" "referred"])]
        (is (true? (:deleted result)))))

    (testing "changed parameters are changed"
      (let [[result]
            (jdbc/query test-db
                        ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                         "changed" "changed"])]
        (is (false? (:deleted result)))
        (is (= "2" (:default_value result)))))

    (synchronize-classes db before)

    (testing "referred is marked undeleted when re-added"
      (let [[result] (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "referred"])]
        (is (false? (:deleted result)))))

    (testing "referred parameter is marked undeleted when re-added"
      (let [[result]
            (jdbc/query test-db
                        ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                         "changed" "referred"])]
        (is (false? (:deleted result)))))))
