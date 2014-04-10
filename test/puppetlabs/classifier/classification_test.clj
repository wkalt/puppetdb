(ns puppetlabs.classifier.classification-test
  (:require [clojure.test :refer :all]
            schema.test
            [puppetlabs.classifier.classification :refer :all]
            [puppetlabs.classifier.schema :refer [group->classification]]
            [puppetlabs.classifier.storage.postgres :refer [root-group-uuid]])
  (:import java.util.UUID))

(use-fixtures :once schema.test/validate-schemas)

(defn base-group
  [name]
  {:name name, :id (UUID/randomUUID), :environment "production", :parent root-group-uuid
   :rule ["=" "foo" "bar"], :variables {}})

(defn vec->tree
  [[group & children]]
  {:group group
   :children (set (map vec->tree children))})

(defn find-group-node
  [group-name {:keys [group children] :as tree}]
  (if (= (:name group) group-name) tree
    (->> (map (partial find-group-node group-name) children)
      (keep identity)
      first)))

(deftest test-unknown-parameters
  (let [classification {:environment "production", :variables {}
                        :classes {:hunky-dory {:peachy? "totes"}
                                  :partial-hit {:this-parameter "right-here"
                                                :what-parameter? "this-one"}
                                  :doesnt-exist {:shouldnt-be-here "nope"}}}
        classes [{:name "hunky-dory", :environment "production", :parameters {:peachy? "true"}}
                 {:name "partial-hit", :environment "production"
                  :parameters {:this-parameter "foo"}}
                 {:name "doesnt-exist", :environment "staging"
                  :parameters {:shouldnt-be-here "bar"}}]]
    (testing "unknown parameters and classes are correctly identified as such"
      (is (= {:doesnt-exist nil, :partial-hit #{:what-parameter?}}
             (unknown-parameters classification classes))))))

(deftest test-tree-validation
  (let [good-node (assoc (base-group "good-node")
                         :classes {:hunky-dory {:peachy? "totes"}
                                   :partial-hit {:this-parameter "bar"}})
        changes-env (assoc (base-group "changes-env") :classes {}, :environment "testing")
        missing-class (assoc (base-group "missing-class")
                             :classes {:doesnt-exist {:what-is-this "nonsense?"}})
        missing-param (assoc (base-group "missing-param")
                             :classes {:doesnt-exist nil
                                       :partial-hit {:this-parameter "foo"
                                                     :what-parameter? "this-one"}})
        only-inherited (assoc (base-group "only-inherited") :classes {})
        tree (vec->tree [good-node [changes-env] [missing-class [missing-param [only-inherited]]]])
        failing-classes [{:name "hunky-dory", :environment "production", :parameters {:peachy? "true"}}
                         {:name "partial-hit", :environment "production"
                          :parameters {:this-parameter "foo"}}]]

    (testing "a validation tree"
      (let [bad-tree (validation-tree tree failing-classes)]

        (testing "adds inherited values from parents in the given tree while validating"
          (let [changes-env-errors (->> bad-tree
                                     (find-group-node (:name changes-env))
                                     :errors)]
            (is (not (empty? changes-env-errors)))
            (is (= (-> changes-env-errors keys set) #{:hunky-dory :partial-hit}))))

        (testing "preserves inheritance ordering"
          (let [only-inherited-errors (->> bad-tree
                                        (find-group-node (:name only-inherited))
                                        :errors)]
            (is (contains? only-inherited-errors :partial-hit))
            (is (not (contains? only-inherited-errors :doesnt-exist)))))

        (testing "is recognized as invalid by valid-tree? when it contains errors"
          (is (not (valid-tree? bad-tree))))))

    (testing "a valid validation tree is recognized as such by valid-tree?"
      (let [the-missing-class {:name "doesnt-exist", :environment "production"
                               :parameters {:what-is-this "fire inside"}}
            passing-classes (concat (assoc-in failing-classes
                                              [1 :parameters :what-parameter?] "this-one")
                                    (map #(assoc % :environment "testing") failing-classes)
                                    [the-missing-class])]
        (is (valid-tree? (validation-tree tree passing-classes)))))))

(deftest inheritance
  (let [child {:environment "staging"
               :classes {:thing {:param "overriding", :remove nil, :other "new"}}
               :variables {:remove nil, :new "whatsit"}}
        parent {:environment "production"
                :classes {:thing {:param "haha", :why "whynot"}}
                :variables {:remove "no"}}
        grandparent {:environment "a post-apocalyptic barren wasteland"
                     :classes {:more-different {:a "b" :c "d"}
                               :thing {:remove "still here"}}
                     :variables {:more "?"}}
        family [child parent grandparent]]

    (testing "merging the inheritance tree works"
      (is (= {:environment "staging"
              :classes {:more-different {:a "b" :c "d"}
                        :thing {:param "overriding", :other "new", :why "whynot"}}
              :variables {:new "whatsit"
                          :more "?"}}
             (collapse-to-inherited family))))))

(deftest classification-conflicts
  (testing "conflicts"
    (let [conflicting [{:environment "production"
                        :classes {:no-conflict {:foo "bar"}
                                  :conflict {:does "conflict", :doesnt "conflict"}}
                        :variables {:unique "snowflake"
                                    :generic "conflict"}}
                       {:environment "staging"
                        :classes {:no-conflict {:baz "quux"}
                                  :conflict {:does "conflicts" :doesnt "conflict"}}
                        :variables {:generic "conflicts"
                                    :also-unique "snowflake"}}]
          disjoint [{:environment "production"
                     :classes {:no-conflict {:foo "bar"}
                               :conflict? {:not "conflict"}}
                     :variables {:unique "snowflake"}}
                    {:environment "production"
                     :classes {:no-conflict {:baz "quux"}
                               :conflict? {:not "conflict"}}
                     :variables {:unique "snowflake"
                                 :also-unique "'nother snowflake"}}]]

      (testing "turns conflicting values into sets and omits all non-conflicting paths"
        (is (= {:environment #{"production" "staging"}
                :classes {:conflict {:does #{"conflict" "conflicts"}}}
                :variables {:generic #{"conflict" "conflicts"}}}
               (conflicts conflicting))))

      (testing "returns nil if there are no conflicts at all"
        (is (nil? (conflicts disjoint)))))))

(deftest find-unrelated
  (let [mk-group (fn [n]
                   {:name n, :id (UUID/randomUUID), :rule ["=" "foo" "foo"]
                    :environment "", :classes {}, :variables {}})
        set-parent (fn [g p] (assoc g :parent (:id p)))
        root (mk-group "root")
        root (set-parent root root)
        left-child (-> (mk-group "left-child")
                     (set-parent root))
        right-child (-> (mk-group "right-child")
                      (set-parent root))
        left-grandchild (-> (mk-group "left-grandchild")
                          (set-parent right-child))
        right-grandchild (-> (mk-group "right-grandchild")
                           (set-parent right-child))
        group->ancestors {root [root]
                          left-child [root]
                          right-child [root]
                          left-grandchild [right-child root]
                          right-grandchild [right-child root]}]
    (testing "inheritance-maxima finds unrelated groups given every group in the hierarchy"
      (is (= #{left-child left-grandchild right-grandchild}
             (set (inheritance-maxima group->ancestors)))))))
