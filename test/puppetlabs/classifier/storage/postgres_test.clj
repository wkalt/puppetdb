(ns puppetlabs.classifier.storage.postgres-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [clojure.walk :refer [prewalk]]
            [clj-time.core :as time]
            [java-jdbc.sql :as sql]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.postgres :refer :all]
            [puppetlabs.classifier.test-util :refer [blank-group blank-group-named vec->tree]]
            [puppetlabs.classifier.util :refer [merge-and-clean]]
            [schema.test]
            [slingshot.slingshot :refer [try+]]
            [slingshot.test])
  (:import java.sql.BatchUpdateException
           java.util.UUID
           org.postgresql.util.PSQLException))

(def test-db {:subprotocol "postgresql"
              :subname (or (System/getenv "CLASSIFIER_DBSUBNAME") "classifier_test")
              :user (or (System/getenv "CLASSIFIER_DBUSER") "classifier_test")
              :password (or (System/getenv "CLASSIFIER_DBPASS") "classifier_test")})

(def db (new-db test-db))

(defn with-test-db
  "Fixture that sets up a cleanly initialized and migrated database"
  [f]
  (drop-public-tables test-db)
  (migrate test-db)
  (f))

(defn expand-sql-exceptions
  [f]
  (try
    (f)
    (catch BatchUpdateException e
      (throw (-> e seq last)))))

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

(deftest ^:database groups
  (let [simplest {:name "simplest"
                  :id (UUID/randomUUID)
                  :environment "test"
                  :environment-trumps false
                  :description "this group has no references"
                  :parent root-group-uuid
                  :rule ["=" "name" "foo"]
                  :classes {}
                  :variables {}}
        no-rules {:name "anarchy"
                  :id (UUID/randomUUID)
                  :environment "test"
                  :environment-trumps false
                  :description "Mom and Dad are gone! No rules!"
                  :parent root-group-uuid
                  :classes {}
                  :variables {}}
        with-classes {:name "with-classes"
                      :id (UUID/randomUUID)
                      :environment "test"
                      :environment-trumps false
                      :parent root-group-uuid
                      :rule ["=" "name" "bar"]
                      :classes {:hi {:greetings "salutations"} :bye {}}
                      :variables {}}
        with-variables {:name "with-variables"
                        :id (UUID/randomUUID)
                        :environment "test"
                        :environment-trumps false
                        :parent root-group-uuid
                        :rule ["=" "name" "baz"]
                        :classes {}
                        :variables {:is-a-variable "yup totes"}}
        orphan {:name "orphan"
                :id (UUID/randomUUID)
                :environment "test"
                :environment-trumps false
                :parent (UUID/randomUUID)
                :rule ["=" "name" "foo"]
                :classes {}
                :variables {}}
        root (get-group db root-group-uuid)]

    (testing "stores a group"
      (create-group db simplest)
      (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups WHERE NOT id = ?"
                                           root-group-uuid])))))

    (testing "stores a group without a rule"
      (create-group db no-rules)
      (is (= 1 (count (jdbc/query test-db ["SELECT * FROM groups WHERE id = ?" (:id no-rules)])))))

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

    (testing "retrieves a group without a rule"
      (is (= no-rules (get-group db (:id no-rules)))))

    (testing "retrieves a group with classes and parameters"
      (is (= with-classes (get-group db (:id with-classes)))))

    (testing "retrieves a group with variables"
      (is (= with-variables (get-group db (:id with-variables)))))

    (testing "retrieves all groups"
      (is (= #{simplest no-rules with-classes with-variables root} (set (get-groups db)))))

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
      (is (thrown? IllegalArgumentException (delete-group db root-group-uuid))))

    (testing "specific error when parent doesn't exist"
      (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/missing-parent]
                    (create-group db orphan))))))

(deftest ^:database create-missing-environments
  (let [c {:name "chrono-manipulator"
           :environment "thefuture"
           :parameters {}}
        g {:name "time-machines"
           :id (UUID/randomUUID)
           :parent root-group-uuid
           :rule ["=" "foo" "foo"]
           :environment "rnd"
           :environment-trumps false
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
        science {:name "science"
                 :parameters {:fine-structure "~1/137"
                              :dark-energy nil}}
        magic {:name "magic"
               :parameters {:players 2
                            :deck-size 40
                            :colors ["w" "u" "b" "r" "g"]
                            :avatar nil}}
        g1 {:name "complex-group"
            :id (UUID/randomUUID)
            :environment "test"
            :environment-trumps true
            :parent root-group-uuid
            :rule ["or"
                   ["=" "name" "foo"]
                   ["=" "name" "bar"]]
            :classes {:science {:fine-structure "1/137"
                                :dark-energy "gnome farts"}
                      :magic {:avatar "Momir Vig"
                              :players 3}}
            :variables {:fqdn "www.example.com"
                        :ntp_servers ["0.pool.ntp.org" "ntp.example.com"]
                        :cluster_index 8
                        :some_bool false}}
        g2 {:name "another-complex-group"
            :id (UUID/randomUUID)
            :environment "tropical"
            :environment-trumps false
            :parent root-group-uuid
            :classes {:science {:fine-structure "1/128"}
                      :magic {:colors ["w" "u" "b" "r" "g" "p"]}}
            :variables {:island false, :rainforest true}}
        root (get-group db root-group-uuid)]

    (doseq [env envs, c [science magic]
            :let [c-with-env (assoc c :environment env)]]
      (create-class db c-with-env)
      (is (= c-with-env (get-class db env (:name c)))))

    (testing "stores groups with rule, class parameters, and top-level variables"
      (create-group db g1)
      (create-group db g2)
      (is (= g1 (get-group db (:id g1))))
      (is (= g2 (get-group db (:id g2))))
      (is (= #{g1 g2 root} (set (get-groups db))))
      (let [rule-from-group (fn [g] (when-let [rule (:rule g)]
                                      {:when rule, :group-id (:id g)}))
            all-rules (->> [g1 g2 root]
                        (map rule-from-group)
                        (keep identity))]
        (is (= (set all-rules) (set (get-rules db))))))

    (testing "can update group's name, environment, description, rule, classes, class parameters, and variables"
      (let [g1-delta {:id (:id g1)
                      :name "sally"
                      :environment "tropical"
                      :environment-trumps false
                      :description "this description is tautological."
                      :rule ["and"
                             ["=" "name" "baz"]
                             ["=" "osfamily" "linux"]]
                      :classes {:science nil
                                :magic {:players nil
                                        :avatar "Urza"
                                        :deck-size 60}}
                      :variables {:some_bool true
                                  :ntp_servers nil
                                  :cluster_index 4
                                  :cluster_size 16
                                  :spirit_animal "turtle"}}
            g1' (merge-and-clean g1 g1-delta)]
        (is (= g1' (update-group db g1-delta)))
        (is (= g1' (get-group db (:id g1))))))

    (testing "can update the root group"
      (let [root (get-group db root-group-uuid)
            root-delta {:id root-group-uuid, :variables {:nc true}}]
        (is (= (merge-and-clean root root-delta)
               (update-group db root-delta)))))

    (testing "throws an error when trying to update the root group's parent"
      (is (thrown+? [:kind :puppetlabs.classifier/inheritance-cycle]
                    (update-group db {:id root-group-uuid, :parent (:id g2)}))))

    (testing "returns nil when attempting to update unknown group"
      (is (nil? (update-group db {:id (UUID/randomUUID)}))))

    (testing "can update group environment"
      (let [g2-env-change {:id (:id g2), :environment "test"}
            g2' (merge-and-clean g2 g2-env-change)]
        (is (= g2' (update-group db g2-env-change)))
        (is (= g2' (get-group db (:id g2))))))

    (testing "can update a group's rule when one is not already set"
      (let [g2-rule-change {:id (:id g2)
                            :environment "tropical" ; undoes change from test above
                            :rule ["<=" "bedtime" "21:00"]}
            g2' (merge-and-clean g2 g2-rule-change)]
        (is (= g2' (update-group db g2-rule-change)))
        (is (= g2' (get-group db (:id g2))))))

    (testing "can remove a group's rule when updating"
      (let [remove-rule-delta {:id (:id g2), :rule nil}]
        (is (= g2 (update-group db remove-rule-delta)))
        (is (= g2 (get-group db (:id g2))))))

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
        (is (= (get-in e [:tree :errors]) {:science nil, :magic nil}))))))

(deftest ^:database hierarchy-and-cycles
  (let [root (get-group db root-group-uuid)
        top (merge (blank-group-named "top")
                   {:parent root-group-uuid})
        child-1 (merge (blank-group-named "child1")
                       {:parent (:id top)})
        child-2 (merge (blank-group-named "child2")
                       {:parent (:id top)})
        grandchild (merge (blank-group-named "grandchild")
                          {:parent (:id child-1)})]
    (doseq [g [top child-1 child-2 grandchild]]
      (create-group db g))

    (testing "can retrieve the ancestors of a group up through the root"
      (is (= [child-1 top root] (get-ancestors db grandchild))))

    (let [tree (vec->tree [top [child-1 [grandchild]] [child-2]])]
      (testing "can retrieve the subtree rooted at a particular group"
        (is (= tree (get-subtree db top)))))

    (testing "when a cycle is present"
      (jdbc/update! test-db :groups {:parent_id (:id child-1)} (sql/where {:id (:id top)}))

      (let [new-top (get-group db (:id top))
            new-top-subtree (vec->tree [new-top [child-1 [grandchild]] [child-2]])]
        (testing "get-ancestors will detect it and report the cycle in the error"
          (is (thrown+? [:kind :puppetlabs.classifier/inheritance-cycle
                         :cycle [child-1 new-top]]
                        (get-ancestors db grandchild))))

        (testing "get-subtree on a node in the cycle will not follow the cycle"
          (is (= new-top-subtree (get-subtree db new-top))))

        (testing "it can be fixed with a normal group update"
          (update-group db {:id (:id top), :parent root-group-uuid})
          (is (= [child-1 top root] (get-ancestors db grandchild))))))

    (testing "the storage layer checks for cycles"
      (testing "when creating a group"
        (let [self-id (UUID/randomUUID)
              self (merge (blank-group-named "self")
                          {:id self-id, :parent self-id})]
          (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/missing-parent
                         :group self]
                        (create-group db self))))

        (let [delta {:id (:id top), :parent (:id child-1)}
              top' (merge top delta)]
          (testing "when updating a group"
            (is (thrown+? [:kind :puppetlabs.classifier/inheritance-cycle
                           :cycle [top' child-1]]
                          (update-group db delta))))

          (testing "when validating a group"
            (is (thrown+? [:kind :puppetlabs.classifier/inheritance-cycle
                           :cycle [top' child-1]]
                          (group-validation-failures db top')))))))

    (testing "attempting to delete a group that has children results in an error"
      (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/children-present
                     :group top
                     :children #{child-1 child-2}]
                    (delete-group db (:id top)))))))

(deftest ^:database hierarchy-inheritance-validation
  (let [high-class {:name "high"
                    :environment "production"
                    :parameters {:refined "surely"}}
        top-group {:name "top"
                   :id (UUID/randomUUID)
                   :parent root-group-uuid
                   :environment "production"
                   :environment-trumps false
                   :classes {:high {:refined "most"}}
                   :variables {}
                   :rule ["=" "foo" "foo"]}
        side-group {:name "side"
                    :id (UUID/randomUUID)
                    :parent root-group-uuid
                    :environment "production"
                    :environment-trumps false
                    :classes {}
                    :variables {}
                    :rule ["=" "foo" "foo"]}
        bottom-group {:name "bottom"
                      :id (UUID/randomUUID)
                      :parent (:id side-group)
                      :environment "staging"
                      :environment-trumps false
                      :classes {}
                      :variables {}
                      :rule ["=" "foo" "foo"]}]
    (create-class db high-class)
    (create-group db top-group)
    (create-group db side-group)
    (create-group db bottom-group)

    (testing "the storage layer validates inherited values for the subtree"
      (let [bad-inheritance-delta {:id (:id side-group), :parent (:id top-group)}]
        (testing "when updating a group"
          (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/missing-referents]
                        (update-group db bad-inheritance-delta))))

        (testing "when validating a group"
          (let [side-group' (merge side-group bad-inheritance-delta)
                validation-errors {:group side-group'
                                   :errors nil
                                   :children #{{:group bottom-group
                                                :errors {:high nil}
                                                :children #{}}}}]
          (is (= validation-errors (group-validation-failures db side-group')))))))

    ;; mark class as deleted
    (let [class-env (:environment high-class)
            class-name (:name high-class)]
        (jdbc/update! test-db :classes {:deleted true}
                      ["environment_name = ? AND name = ?" class-env class-name])
        (is (nil? (get-class db class-env class-name))))

    (testing "validation allows groups to be updated when they contain missing referents"
      (let [rule-update {:id (:id top-group), :rule [">" ["fact" "hat_height"] "10"]}
            top-group' (merge-and-clean top-group rule-update)]
        (is (= top-group' (update-group db rule-update)))
        (is (= top-group' (get-group db (:id top-group)))))

      (testing "but not if the update adds new missing references"
        (let [bad-delta {:id (:id top-group), :classes {:first-class {}}}]
          (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/missing-referents]
                        (update-group db bad-delta)))))

      (let [fix-delta {:id (:id top-group), :classes {:high-class nil}}
            top-group' (merge-and-clean (get-group db (:id top-group)) fix-delta)]
        (is (= top-group' (update-group db fix-delta)))))))

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

(deftest ^:database last-sync
  (let [primero (jdbc/query test-db ["select * from last_sync"])
        _ (set-last-sync db)
        segundo (jdbc/query test-db ["select * from last_sync"])
        _ (set-last-sync db)
        tercero (jdbc/query test-db ["select * from last_sync"])]

    (testing "initially no last sync"
      (is (= 0 (count primero))))

    (testing "after first setting of last sync, one value"
      (is (= 1 (count segundo))))

    (testing "after setting it again, still one value that is later"
      (is (= 1 (count tercero)))

      (let [[{time1 :time}] segundo
            [{time2 :time}] tercero]
        (is (.after time2 time1))))))

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
        referrer {:name "referrer",
                  :classes {:used-class {}, :changed-class {:used-param "hi"}}
                  :id (UUID/randomUUID), :environment "production"
                  :environment-trumps false, :parent root-group-uuid, :rule ["=" "foo" "foo"]
                  :variables {}}]

    (synchronize-classes db before)
    (create-group db referrer)
    (synchronize-classes db after)

    (testing "sets last sync"
      (is (not (nil? (get-last-sync db)))))

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
        (is (= "\"2\"" (:default_value changed-row)))))

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
                      :environment-trumps false
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
        spaceship {:name "spaceship"
                   :classes {:rocket {:stages "3"}
                             :avionics {:log "/var/log/avionics-data"}
                             :payload {}}
                   :id (UUID/randomUUID,) :environment "space", :environment-trumps false
                   :parent root-group-uuid, :rule ["=" "foo" "foo"], :variables {}}]

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

(defn- rand-id [] (UUID/randomUUID))

(deftest ^:database node-check-ins
  (let [neuro-name-nodeval {:path "name", :value "Neuromancer"}
        neuro-explanation {(rand-id) {:value true, :form ["=" neuro-name-nodeval "Neuromancer"]}}
        check-ins {"Neuromancer" [{:node "Neuromancer"
                                   :time (time/now)
                                   :explanation neuro-explanation
                                   :transaction-uuid (UUID/randomUUID)
                                   :classification {:environment "production"
                                                    :classes {:construct-runner {:construct-type "RAM"}}
                                                    :variables {:datacenter "Tessier-Ashpool Orbital"}}}
                                  {:node "Neuromancer"
                                   :time (time/ago (time/weeks 1))
                                   :explanation neuro-explanation
                                   :transaction-uuid (UUID/randomUUID)}]
                   "Wintermute" [{:node "Wintermute"
                                  :time (time/ago (time/days 3))
                                  :explanation {(rand-id)
                                                {:value true
                                                 :form ["="
                                                        {:path "name", :value "Wintermute"}
                                                        "Wintermute"]}}
                                  :classification {:environment "production"
                                                   :classes {:masked {:personalities ["The Finn"
                                                                                      "Julie Deane"
                                                                                      "Armitage"]}}
                                                   :variables {:desire "merge with Neuromancer"}}}]}
        all-check-ins (into [] (for [[nn c-is] check-ins]
                                 {:name nn, :check-ins (->> c-is
                                                         (map #(dissoc % :node))
                                                         vec)}))]

    (testing "can store node check-ins"
      (store-check-in db (get-in check-ins ["Neuromancer" 0]))
      (store-check-in db (get-in check-ins ["Neuromancer" 1]))
      (is (= 2 (count (jdbc/query test-db ["SELECT * FROM node_check_ins WHERE node = ?"
                                           "Neuromancer"])))))

    (testing "can retrieve a node's check-ins"
      (is (= (get check-ins "Neuromancer") (get-check-ins db "Neuromancer"))))

    (testing "can retrieve all check-ins"
      (store-check-in db (get-in check-ins ["Wintermute" 0]))
      (is (= all-check-ins (get-nodes db))))))

(deftest ^:database hierarchy-import
  (let [extant-child-1 (blank-group-named "child 1")
        extant-child-2 (blank-group-named "child 2")
        extant-gchild-1 (merge (blank-group) {:name "grandchild 1", :parent (:id extant-child-2)})
        extant-gchild-2 (merge (blank-group) {:name "grandchild 2", :parent (:id extant-child-2)})
        extant-groups [extant-child-1 extant-child-2 extant-gchild-1 extant-gchild-2]
        root (get-group db root-group-uuid)
        root' (merge root {:name "default"
                           :variables {:x 3}})
        left-child (merge (blank-group-named "left child")
                          {:classes {:extant {:new-param "value"}}})
        left-gchild (merge (blank-group-named "left grandchild")
                               {:environment "different"
                                :parent (:id left-child)
                                :classes {:extant {:newer-param "probably haven't heard of it"}}})
        extant-classes [{:name "extant"
                         :environment (:environment left-child)
                         :parameters {:old-param "so 2010"}}
                        {:name "extant"
                         :environment (:environment left-gchild)
                         :parameters {}}]
        right-child (merge (blank-group-named "right child")
                           {:classes {:novel {:plot "thrilling"
                                              :characters "deep"}}})
        right-gchild (merge (blank-group-named "right grandchild")
                                {:environment "new"
                                 :parent (:id right-child)
                                 :classes {:adaptation {:faithful "as much as possible"}}})
        new-groups [root' left-child left-gchild right-child right-gchild]]
    (doseq [g extant-groups] (create-group db g))
    (doseq [c extant-classes] (create-class db c))

    (testing "importing a valid, complete hierarchy"
      (let [hierarchy-root (import-hierarchy db new-groups)]
        (is hierarchy-root)
        (is (= root' (:group hierarchy-root)))

        (testing "removes all previous groups"
          (doseq [{id :id} extant-groups]
            (is (nil? (get-group db id)))))

        (testing "creates the classes and class parameters referenced by the hierarchy"
          (let [new-classes [{:name "extant"
                              :environment (:environment left-child)
                              :parameters {:old-param "so 2010", :new-param nil}}
                             {:name "extant"
                              :environment (:environment left-gchild)
                              :parameters {:new-param nil, :newer-param nil}}
                             {:name "novel"
                              :environment (:environment right-child)
                              :parameters {:plot nil, :characters nil}}
                             {:name "novel"
                              :environment (:environment right-gchild)
                              :parameters {:plot nil, :characters nil}}
                             {:name "adaptation"
                              :environment (:environment right-gchild)
                              :parameters {:faithful nil}}]]
            (doseq [{:keys [environment name] :as new-class} new-classes]
              (is (= new-class (get-class db environment name))))))))))
