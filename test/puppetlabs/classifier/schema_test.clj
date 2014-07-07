(ns puppetlabs.classifier.schema-test
  (:require [clojure.test :refer :all]
            schema.test
            [puppetlabs.classifier.schema :refer :all]
            [puppetlabs.classifier.storage :refer [root-group-uuid]]
            [puppetlabs.classifier.test-util :refer [vec->tree]]
            [slingshot.test])
  (:import java.util.UUID))

(use-fixtures :once schema.test/validate-schemas)

(deftest unflattening-hierarchy
  (let [group-named-with-parent (fn [name parent-uuid]
                            {:parent parent-uuid
                             :id (UUID/randomUUID), :name name, :environment "production"
                             :environment-trumps false, :description "", :rule ["=" "name" "foo"]
                             :classes {}, :variables {}})
        root (-> (group-named-with-parent "root" root-group-uuid)
               (assoc :id root-group-uuid))
        child-1 (group-named-with-parent "level 1 first child" root-group-uuid)
        child-2 (group-named-with-parent "level 1 second child" root-group-uuid)
        child-3 (group-named-with-parent "level 1 third child" root-group-uuid)
        grandchild-1 (group-named-with-parent "level 2 first child" (:id child-1))
        grandchild-2 (group-named-with-parent "level 2 second child" (:id child-3))
        grandchild-3 (group-named-with-parent "level 2 third child" (:id child-3))]

    (testing "unflattening a valid hierarchy produces the expected result"
      (let [groups [root child-1 child-2 child-3 grandchild-1 grandchild-2 grandchild-3]
            expected-hierarchy (vec->tree [root
                                           [child-1
                                            [grandchild-1]]
                                           [child-2]
                                           [child-3
                                            [grandchild-2]
                                            [grandchild-3]]])]
        (dotimes [_ 10]
          (is (= expected-hierarchy (groups->tree (shuffle groups)))))))

    (testing "attempting to unflatten a hierarchy with a cycle throws an error"
      (let [loop-root (assoc root :parent (:id grandchild-2))
            loop-groups [loop-root child-1 child-2 child-3 grandchild-1 grandchild-2 grandchild-3]]
        (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/inheritance-cycle
                       :cycle [loop-root grandchild-2 child-3]]
                      (groups->tree loop-groups)))))

    (testing "attempting to unflatten a hierarchy with unreachable groups throws an error"
      (let [isolated-root (assoc child-3 :parent (:id grandchild-3))
            with-isolated-groups [root child-1 child-2
                                  isolated-root grandchild-1 grandchild-2 grandchild-3]]
        (is (thrown+? [:kind :puppetlabs.classifier/unreachable-groups
                       :groups [isolated-root grandchild-2 grandchild-3]]
                      (groups->tree with-isolated-groups)))))

    (testing "attempting to unflatten a hierarchy with duplicate group ids throws an error"
      (let [duped-groups [root
                          child-1 child-2 child-1 child-3
                          grandchild-1 grandchild-2 grandchild-3 grandchild-2]]
        (is (thrown+? [:kind :puppetlabs.classifier.storage.postgres/uniqueness-violation
                       :entity-kind "group"
                       :constraint "groups_pkey"
                       :fields ["id" "id"]
                       :values [(:id child-1) (:id grandchild-2)]]
                      (groups->tree duped-groups)))))))
