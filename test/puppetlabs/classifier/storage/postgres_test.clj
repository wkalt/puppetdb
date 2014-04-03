(ns puppetlabs.classifier.storage.postgres-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :refer [project]]
            [clojure.test :refer :all]
            [clojure.walk :refer [prewalk]]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.postgres :refer :all]
            [puppetlabs.classifier.util :refer [merge-and-clean]]
            [schema.test]
            [slingshot.slingshot :refer [try+]])
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
    ;; root group is already inserted
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups"])))))
  (testing "creates a nodes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM nodes"])))))
  (testing "creates a classes table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes"])))))
  (testing "creates a class parameters table"
    (is (= 0 (count (jdbc/query test-db ["SELECT * FROM class_parameters"])))))
  (testing "creates a rules table"
    ;; root group's rule is already inserted
    (is (= 1 (count (jdbc/query test-db ["SELECT * FROM rules"]))))))

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

(deftest ^:database groups
  (let [simplest {:name "simplest"
                  :id (UUID/randomUUID)
                  :environment "test"
                  :parent root-group-uuid
                  :rule ["=" "name" "foo"]
                  :classes {}
                  :variables {}}
        with-classes {:name "with-classes"
                      :id (UUID/randomUUID)
                      :environment "test"
                      :parent root-group-uuid
                      :rule ["=" "name" "bar"]
                      :classes {:hi {:greetings "salutations"} :bye {}}
                      :variables {}}
        with-variables {:name "with-variables"
                        :id (UUID/randomUUID)
                        :environment "test"
                        :parent root-group-uuid
                        :rule ["=" "name" "baz"]
                        :classes {}
                        :variables {:is-a-variable "yup totes"}}
        root (get-group db root-group-uuid)]

    (testing "stores a group"
      (create-group db simplest)
      (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups WHERE NOT id = ?"
                                           root-group-uuid])))))

    (testing "stores a group with multiple classes"
      (create-class db {:name "hi", :parameters {:greetings "hello"}, :environment "test"})
      (create-class db {:name "bye", :parameters {}, :environment "test"})
      (create-group db with-classes)
      (is (= 2 (count (jdbc/query test-db
                                  ["SELECT * FROM groups g
                                   JOIN group_classes gc ON gc.group_id = g.id
                                   WHERE g.id = ?"
                                   (:id with-classes)])))))

    (testing "stores a group with variables"
      (create-group db with-variables)
      (is (= 1 (count (jdbc/query test-db
                                  ["SELECT * FROM group_variables
                                   WHERE group_id = ?"
                                   (:id with-variables)])))))

    (testing "retrieves a group"
      (is (= simplest (get-group db (:id simplest)))))

    (testing "retrieves a group with classes and parameters"
      (is (= with-classes (get-group db (:id with-classes)))))

    (testing "retrieves a group with variables"
      (is (= with-variables (get-group db (:id with-variables)))))

    (testing "retrieves all groups"
      (is (= #{simplest with-classes with-variables root} (set (get-groups db)))))

    (testing "deletes a group"
      (delete-group db (:id simplest))
      (is (nil? (get-group db (:id simplest))))
      (is (empty? (jdbc/query test-db ["SELECT * FROM groups WHERE id = ?"
                                       (:id simplest)])))
      (is (empty? (jdbc/query test-db ["SELECT * FROM rules WHERE group_id = ?"
                                       (:id simplest)]))))

    (testing "deletes a group with classes and parameters"
      (delete-group db (:id with-classes))
      (is (nil? (get-group db (:id with-classes))))
      (is (empty? (jdbc/query test-db ["SELECT * FROM group_classes WHERE group_id = ?"
                                       (:id with-classes)])))
      (is (empty? (jdbc/query test-db ["SELECT * FROM group_class_parameters WHERE group_id = ?"
                                       (:id with-classes)]))))

    (testing "deletes a group with variables"
      (delete-group db (:id with-variables))
      (is (nil? (get-group db (:id with-variables))))
      (is (empty? (jdbc/query test-db ["SELECT * FROM group_variables WHERE group_id = ?"
                                       (:id with-variables)]))))

    (testing "can't delete the root group"
      (is (thrown? IllegalArgumentException (delete-group db root-group-uuid))))))

(deftest ^:database create-missing-environments
  (let [c {:name "chrono-manipulator"
           :environment "thefuture"
           :parameters {}}
        g {:name "time-machines"
           :id (UUID/randomUUID)
           :parent root-group-uuid
            :rule ["=" "foo" "foo"]
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
            :id (UUID/randomUUID)
            :environment "test"
            :parent root-group-uuid
            :rule ["or"
                   ["=" "name" "foo"]
                   ["=" "name" "bar"]]
            :classes {:first {:one "one-val"
                              :two "two-val"}
                      :second {:three "three-val"
                               :four "four-val"}}
            :variables {:fqdn "www.example.com"
                        :ntp_servers ["0.pool.ntp.org" "ntp.example.com"]
                        :cluster_index 8
                        :some_bool false}}
        g2 {:name "another-complex-group"
            :id (UUID/randomUUID)
            :environment "tropical"
            :parent root-group-uuid
            :rule ["=" "foo" "foo"]
            :classes {:first {:two "another-two-val"}
                      :second {:four "four-val"}}
            :variables {:island false, :rainforest true}}
        root (get-group db root-group-uuid)]

    (doseq [env envs, c [first-class second-class]]
      (create-class db (assoc c :environment env)))

    (testing "stores groups with rule, class parameters, and top-level variables"
      (create-group db g1)
      (create-group db g2)
      (is (= g1 (get-group db (:id g1))))
      (is (= g2 (get-group db (:id g2))))
      (is (= #{g1 g2 root} (set (get-groups db))))
      (let [rule-from-group (fn [g] {:when (:rule g), :group-id (:id g)})
            all-rules (map rule-from-group [g1 g2 root])]
        (is (= (set all-rules) (set (get-rules db))))))

    (testing "can update group's name, rule, classes, class parameters, and variables"
      (let [g1-delta {:id (:id g1)
                      :name "sally"
                      :rule ["and"
                             ["=" "name" "baz"]
                             ["=" "osfamily" "linux"]]
                      :classes {:first nil
                                :second {:three nil
                                         :four "red fish"
                                         :five "blue fish"}}
                      :variables {:some_bool true
                                  :ntp_servers nil
                                  :cluster_index 4
                                  :cluster_size 16
                                  :spirit_animal "turtle"}}
            g1' (merge-and-clean g1 g1-delta)]
        (is (= g1' (update-group db g1-delta)))
        (is (= g1' (get-group db (:id g1))))))

    (testing "returns nil when attempting to update unknown group"
      (is (nil? (update-group db {:id (UUID/randomUUID)}))))

    (testing "can update group environments"
      (let [g2-env-change {:id (:id g2), :environment "test"}
            g2' (merge-and-clean g2 g2-env-change)]
        (is (= g2' (update-group db g2-env-change)))
        (is (= g2' (get-group db (:id g2))))))

    (testing "trying to update environments when the group refers to classes
             that don't exist in the new environment results in a foreign-key
             violation exception"
      (let [g2-bad-env-change {:id (:id g2)
                               :environment "dne"}
            e (try+ (do
                      (update-group db g2-bad-env-change)
                      nil)
                (catch [:kind :puppetlabs.classifier.storage.postgres/missing-referents] e
                  e))]
        (is e)
        (is (= (get-in e [:tree :group :name]) (:name g2)))
        (is (= (get-in e [:tree :errors] {:first nil, :second nil})))))))

(deftest ^:database group-hierarchy
  (let [blank-group-named (fn [n] {:name n, :id (UUID/randomUUID), :environment "test",
                                   :rule ["=" "foo" "bar"], :classes {}, :variables {}})
        root (get-group db root-group-uuid)
        top (-> (blank-group-named "top")
              (assoc :parent root-group-uuid))
        child-1 (-> (blank-group-named "child1")
                  (assoc :parent (:id top)))
        child-2 (-> (blank-group-named "child2")
                  (assoc :parent (:id top)))
        grandchild (-> (blank-group-named "grandchild")
                     (assoc :parent (:id child-1)))]
    (doseq [g [top child-1 child-2 grandchild]]
      (create-group db g))

    (testing "can retrieve the ancestors of a group up through the root"
      (is (= [child-1 top root] (get-ancestors db grandchild))))

    (let [tree {:group top
                :children #{{:group child-2, :children #{}}
                            {:group child-1
                             :children #{{:group grandchild, :children #{}}}}}}]
      (testing "can retrieve the subtree rooted at a particular group"
        (is (= tree (get-subtree db top)))))))

(deftest ^:database classes
  (let [no-params {:name "myclass" :parameters {} :environment "test"}
        with-params {:name "classtwo"
                     :parameters {:param1 "value1", :param2 "value2"}
                     :environment "test"}]

   (testing "store a class with no parameters"
     (create-class db no-params)
     (is (= 1 (count (jdbc/query test-db ["SELECT * FROM classes"])))))

   (testing "store a class with multiple parameters"
     (create-class db with-params)
     (is (= 2 (count (jdbc/query test-db
                                 ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "classtwo"])))))

    (testing "returns nil when trying to retrieve a class that doesn't exist"
     (is (nil? (get-class db "wrong" "noclass"))) )

   (testing "retrieve a class with no parameters"
     (is (= no-params (get-class db (:environment no-params) (:name no-params)))))

   (testing "retrieve a class with parameters"
     (is (= with-params (get-class db (:environment with-params) (:name with-params)))))

   (testing "retrieves all classes"
     (is (= #{no-params with-params} (set (get-classes db "test")))))

   (testing "deletes a class with no parameters"
     (delete-class db (:environment no-params) (:name no-params))
     (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "noclass"])))))

   (testing "deletes a class with parameters"
     (delete-class db (:environment with-params) (:name no-params))
     (is (= 0 (count (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "testclass"]))))
     (is (= 0 (count (jdbc/query test-db
                                 ["SELECT * FROM classes c join class_parameters cp on cp.class_name = c.name where c.name = ?" "testclass"])))))))

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
  (let [before [{:name "changed-class", :environment "production"
                 :parameters {:changed-param "1", :unused-param "5", :used-param "6"}}
                {:name "used-class", :parameters {:unused-param "42"}, :environment "production"}
                {:name "unused-class", :parameters {:a "a"}, :environment "production"}]
        before-by-name (into {} (map (juxt :name identity) before))
        after [{:name "added", :parameters {}, :environment "production"}
               {:name "changed-class", :environment "production"
                :parameters {:added "1", :changed-param "2"}}]
        after-by-name (into {} (map (juxt :name identity) after))
        referrer {:name "referrer", :id (UUID/randomUUID), :environment "production", :parent root-group-uuid
                  :classes {:used-class {}, :changed-class {:used-param "hi"}}
                  :rule ["=" "foo" "foo"] , :variables {}}]

    (synchronize-classes db before)
    (create-group db referrer)
    (synchronize-classes db after)

    (testing "unused class is deleted"
      (let [{:keys [name environment]} (get before-by-name "unused-class")]
        (is (nil? (get-class db name environment)))))

    (testing "used class is marked deleted"
      (let [[class-row] (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "used-class"])]
        (is (true? (:deleted class-row)))))

    (testing "parameters of a used class are marked deleted"
      (let [[param-row] (jdbc/query
                          test-db
                          ["SELECT * FROM class_parameters WHERE class_name = ?" "used-class"])]
        (is (true? (:deleted param-row)))))

    (testing "unused parameters are deleted"
      (let [rows (jdbc/query
                   test-db
                   ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                    "changed-class" "unused-param"])]
        (is (empty? rows))))

    (testing "used parameters are marked deleted"
      (let [[param-row] (jdbc/query
                          test-db
                          ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                           "changed-class" "used-param"])]
        (is (true? (:deleted param-row)))))

    (testing "changed parameters are changed"
      (let [[changed-row] (jdbc/query
                            test-db
                            ["SELECT * FROM class_parameters WHERE class_name = ? AND parameter = ?"
                             "changed-class" "changed-param"])]
        (is (false? (:deleted changed-row)))
        (is (= "2" (:default_value changed-row)))))

    (synchronize-classes db before)

    (testing "used-class is marked undeleted when re-added"
      (let [[class-row] (jdbc/query test-db ["SELECT * FROM classes WHERE name = ?" "used-class"])]
        (is (false? (:deleted class-row)))))

    (testing "unused parameter of used class is un-marked as deleted"
      (let [[param-row] (jdbc/query test-db
                                    ["SELECT * FROM class_parameters
                                     WHERE class_name = ? AND parameter = ?"
                                     "used-class" "unused-param"])]
        (is (false? (:deleted param-row)))))

    (testing "changed class's used parameter is un-marked as deleted when re-added"
      (let [[param-row] (jdbc/query test-db
                                    ["SELECT * FROM class_parameters
                                     WHERE class_name = ? AND parameter = ?"
                                     "changed-class" "used-param"])]
        (is (false? (:deleted param-row)))))))

(deftest ^:database annotation-class-with-no-params
  (let [simple-class {:name "simple", :environment "production", :parameters {}}
        simple-group {:name "simple"
                      :id (UUID/randomUUID)
                      :environment "production"
                      :parent root-group-uuid
                      :classes {:simple {}}
                      :variables {}
                      :rule ["=" "something" "somethingelse"]}]
    (synchronize-classes db [simple-class])
    (create-group db simple-group)
    (testing "annotation works for a class with no parameters"
      (annotate-group db (get-group db (:id simple-group))))))

(deftest ^:database group-annotations
  (let [rocket-class {:name "rocket", :environment "space", :parameters {:stages "1"}}
        rocket-class-no-stages (assoc rocket-class :parameters {})
        payload-class {:name "payload", :environment "space", :parameters {}}
        avionics-class {:name "avionics", :environment "space", :parameters {:log "/dev/null"}}
        spaceship {:name "spaceship", :id (UUID/randomUUID,) :environment "space",
                   :parent root-group-uuid, :rule ["=" "foo" "foo"], :variables {}
                   :classes {:rocket {:stages "3"}
                             :avionics {:log "/var/log/avionics-data"}
                             :payload {}}}]

    (synchronize-classes db [payload-class avionics-class rocket-class])
    (create-group db spaceship)
    (synchronize-classes db [rocket-class-no-stages])

    (testing "group references to deleted classes and parameters are annotated as such"
      (let [annotated (->> (get-group db (:id spaceship))
                         (annotate-group db))]
        (is (= (:deleted annotated)
               {:payload {:puppetlabs.classifier/deleted true}
                :avionics {:puppetlabs.classifier/deleted true
                           :log {:puppetlabs.classifier/deleted true
                                 :value "/var/log/avionics-data"}}
                :rocket {:puppetlabs.classifier/deleted false
                         :stages {:puppetlabs.classifier/deleted true
                                  :value "3"}}}))
        (is (= spaceship (dissoc annotated :deleted)))))

    (testing "the annotated version of groups without references to deleted classes and parameters are identical to the regular old group"
      (synchronize-classes db [payload-class avionics-class rocket-class])
      (let [annotated (->> (get-group db (:id spaceship))
                        (annotate-group db))]
        (is (= spaceship annotated))))))
