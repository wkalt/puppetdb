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
  {:name name, :id (UUID/randomUUID), :environment "production", :environment-trumps false,
   :parent root-group-uuid, :rule ["=" "foo" "bar"], :variables {}})

(defn vec->tree
  [[group & children]]
  {:group group
   :children (set (map vec->tree children))})

(defn find-group-node
  [group-name {:keys [group children] :as tree}]
  (if (= (:name group) group-name)
    tree
    (->> (map (partial find-group-node group-name) children)
      (keep identity)
      first)))

(deftest test-unknown-parameters
  (let [classification {:environment "production", :environment-trumps false, :variables {}
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

(deftest tree-difference
  (testing "validation-tree-difference"
    (let [groups (repeatedly 3 #(assoc (base-group "foo") :classes {}))
          [foo-group bar-group baz-group] (sort-by (comp str :id) groups)
          vt_0 {:group foo-group
                :errors {:missing-class nil
                         :different-class #{:missing-param}}
                :children #{{:group bar-group
                             :errors {:missing-class nil
                                      :different-class #{:missing-param
                                                         :never-seen-this-param-before}}
                             :children #{}}
                            {:group baz-group
                             :errors {:missing-class nil
                                      :different-class #{:missing-param}
                                      :where-did-i-put-that-class #{:over-here?}}}}}
          vt_1 {:group foo-group
                :errors {:different-class #{:missing-param}}
                :children #{{:group bar-group
                             :errors {:different-class #{:never-seen-this-param-before}}
                             :children #{}}
                            {:group baz-group
                             :errors {:where-did-i-put-that-class #{:over-here?}
                                      :gym-class #{:smelliness}}}}}]

      (testing "returns a vtree that has all the errors in the first that were not in the second"
        (let [expected-diff {:group foo-group
                             :errors {:missing-class nil}
                             :children #{{:group bar-group
                                          :errors {:missing-class nil
                                                   :different-class #{:missing-param}}
                                          :children #{}}
                                         {:group baz-group
                                          :errors {:missing-class nil
                                                   :different-class #{:missing-param}}
                                          :children #{}}}}]
          (is (= expected-diff (validation-tree-difference vt_0 vt_1)))))

      (testing "returns a valid tree when a tree is diffed with itself"
        (is (valid-tree? (validation-tree-difference vt_0 vt_0)))
        (is (valid-tree? (validation-tree-difference vt_1 vt_1))))

      (testing "throws an error if given two trees with different structures"
        (is (thrown? IllegalArgumentException
                     (validation-tree-difference vt_0 (assoc vt_1 :group bar-group))))
        (is (thrown? IllegalArgumentException
                     (validation-tree-difference vt_0 {:group foo-group
                                                       :errors nil
                                                       :children #{}})))))))

(deftest inheritance
  (let [child {:environment "staging"
               :environment-trumps false
               :classes {:thing {:param "overriding", :remove nil, :other "new"}}
               :variables {:remove nil, :new "whatsit"}}
        parent {:environment "production"
               :environment-trumps false
                :classes {:thing {:param "haha", :why "whynot"}}
                :variables {:remove "no"}}
        grandparent {:environment "a post-apocalyptic barren wasteland"
                     :environment-trumps false
                     :classes {:more-different {:a "b" :c "d"}
                               :thing {:remove "still here"}}
                     :variables {:more "?"}}
        family [child parent grandparent]]

    (testing "merging the inheritance tree works"
      (is (= {:environment "staging"
              :environment-trumps false
              :classes {:more-different {:a "b" :c "d"}
                        :thing {:param "overriding", :other "new", :why "whynot"}}
              :variables {:new "whatsit"
                          :more "?"}}
             (collapse-to-inherited family))))))

(deftest classification-conflicts
  (testing "conflicts"
    (testing "turns conflicting values into sets and omits all non-conflicting paths"
      (let [non-env-conflicts [{:environment "production"
                                :environment-trumps false
                                :classes {:no-conflict {:foo "bar"}
                                          :conflict {:does "conflict", :doesnt "conflict"}}
                                :variables {:unique "snowflake"
                                            :generic "conflict"}}
                               {:environment "production"
                                :environment-trumps false
                                :classes {:no-conflict {:baz "quux"}
                                          :conflict {:does "conflicts", :doesnt "conflict"}}
                                :variables {:generic "conflicts"
                                            :also-unique "snowflake"}}
                               {:environment "production"
                                :environment-trumps false
                                :classes {:no-conflict {:baz "quux"}
                                          :conflict {:does "conflicting", :doesnt "conflict"}}
                                :variables {:generic "conflagration"
                                            :one-of-a-kind "means unique"}}]
            no-trump-env-conflicts [{:environment "production"
                                     :environment-trumps false
                                     :classes {}, :variables {}}
                                    {:environment "staging"
                                     :environment-trumps false
                                     :classes {}, :variables {}}]
            trump-env-conflicts [{:environment "production"
                                  :environment-trumps true
                                  :classes {}, :variables {}}
                                 {:environment "staging"
                                  :environment-trumps true
                                  :classes {}, :variables {}}]]
        (is (= {:classes {:conflict {:does #{"conflict" "conflicts" "conflicting"}}}
                :variables {:generic #{"conflict" "conflicts" "conflagration"}}}
               (conflicts non-env-conflicts)))
        (is (= {:environment #{"production" "staging"}} (conflicts no-trump-env-conflicts)))
        (is (= {:environment #{"production" "staging"}} (conflicts trump-env-conflicts)))))

    (testing "returns nil if there are no conflicts at all"
      (let [disjoint [{:environment "production"
                       :environment-trumps false
                       :classes {:no-conflict {:foo "bar"}
                                 :conflict? {:not "conflict"}}
                       :variables {:unique "snowflake"}}
                      {:environment "production"
                       :environment-trumps false
                       :classes {:lone-wolf {:pardners []}}
                       :variables {:very-unique "makes no sense"}}
                      {:environment "production"
                       :environment-trumps false
                       :classes {:no-conflict {:baz "quux"}
                                 :conflict? {:not "conflict"}}
                       :variables {:unique "snowflake"
                                   :also-unique "'nother snowflake"}}]]
        (is (nil? (conflicts disjoint)))))

    (testing "uses the environment-trumps flag to resolve environment conflicts"
      (let [with-env-trump [{:environment "production"
                             :environment-trumps false
                             :classes {}, :variables {}}
                            {:environment "staging"
                             :environment-trumps false
                             :classes {}, :variables {}}
                            {:environment "bugfix-4385"
                             :environment-trumps true
                             :classes {}, :variables {}}
                            {:environment "bugfix-4385"
                             :environment-trumps true
                             :classes {}, :variables {}}]]
        (is (nil? (conflicts with-env-trump)))))))

(deftest conflict-explanations
  (let [root {:name "root", :id root-group-uuid, :parent root-group-uuid
              :environment "production", :environment-trumps false, :classes {}, :variables {}}
        pre-wormhole {:name "Farscape Project"
                      :environment "sol system"
                      :environment-trumps false
                      :classes {:ship {:name "Farscape-1"
                                       :livery "IASA"
                                       :living false
                                       :propulsion "gravitational drive"}}
                      :variables {:wormhole-knowledge "none"}
                      :id (UUID/randomUUID), :parent root-group-uuid}
        post-wormhole {:name "Through the Wormhole"
                       :environment "some distant part of the universe"
                       :environment-trumps false
                       :classes {:ship {:name "Moya"
                                        :livery "Peacekeeper"
                                        :living true
                                        :propulsion ["hetch" "starburst"]}}
                       :variables {:wormhole-knowledge "a little"}
                       :id (UUID/randomUUID), :parent root-group-uuid}
        post-ancients {:name "Encounter with the Goddess"
                       :environment "some distant part of the universe"
                       :environment-trumps false
                       :variables {:wormhole-knowledge "extensive subconscious"}
                       :classes {}, :id (UUID/randomUUID), :parent root-group-uuid}
        groups [pre-wormhole post-wormhole post-ancients]
        conflicts (conflicts (map group->classification groups))
        group->ancestors {pre-wormhole [root]
                          post-wormhole [root]
                          post-ancients [root]}
        expected {:environment #{{:value "sol system"
                                  :from pre-wormhole
                                  :defined-by pre-wormhole}
                                 {:value "some distant part of the universe"
                                  :from post-wormhole
                                  :defined-by post-wormhole}
                                 {:value "some distant part of the universe"
                                  :from post-ancients
                                  :defined-by post-ancients}}
                  :classes {:ship {:name #{{:value "Farscape-1"
                                            :from pre-wormhole
                                            :defined-by pre-wormhole}
                                           {:value "Moya"
                                            :from post-wormhole
                                            :defined-by post-wormhole}}
                                   :livery #{{:value "IASA"
                                              :from pre-wormhole
                                              :defined-by pre-wormhole}
                                             {:value "Peacekeeper"
                                              :from post-wormhole
                                              :defined-by post-wormhole}}
                                   :living #{{:value false
                                              :from pre-wormhole
                                              :defined-by pre-wormhole}
                                             {:value true
                                              :from post-wormhole
                                              :defined-by post-wormhole}}
                                   :propulsion #{{:value "gravitational drive"
                                                  :from pre-wormhole
                                                  :defined-by pre-wormhole}
                                                 {:value ["hetch" "starburst"]
                                                  :from post-wormhole
                                                  :defined-by post-wormhole}}}}
                  :variables {:wormhole-knowledge
                              #{{:value "none"
                                 :from pre-wormhole
                                 :defined-by pre-wormhole}
                                {:value "a little"
                                 :from post-wormhole
                                 :defined-by post-wormhole}
                                {:value "extensive subconscious"
                                 :from post-ancients
                                 :defined-by post-ancients}}}}]

    (testing "classification conflicts can be explained"
      (is (= expected (explain-conflicts conflicts group->ancestors))))))

(deftest find-unrelated
  (let [mk-group (fn [n]
                   {:name n, :id (UUID/randomUUID), :rule ["=" "foo" "foo"]
                    :environment "", :environment-trumps false, :classes {}, :variables {}})
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
