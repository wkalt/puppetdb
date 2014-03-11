(ns puppetlabs.classifier.classification-test
  (:require [clojure.test :refer :all]
            schema.test
            [puppetlabs.classifier.classification :refer :all]))

(use-fixtures :once schema.test/validate-schemas)

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
             (inherited-classification family))))))
