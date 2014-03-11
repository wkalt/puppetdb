(ns puppetlabs.classifier.classification-test
  (:require [clojure.test :refer :all]
            schema.test
            [puppetlabs.classifier.classification :refer :all]
            [puppetlabs.classifier.schema :refer [group->classification]]))

(use-fixtures :once schema.test/validate-schemas)

(defn base-group
  [name]
  {:name name, :environment "production", :parent "root"
   :rule {:when ["=" "foo" "bar"]}, :variables {}})

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

        (testing "preservers inheritance ordering"
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
