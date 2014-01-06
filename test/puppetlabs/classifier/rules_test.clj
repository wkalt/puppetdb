(ns puppetlabs.classifier.rules-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [puppetlabs.classifier.rules :refer :all]))

(use-fixtures :once schema.test/validate-schemas)

(deftest rules
  (testing "simplest classification"
    (let [node {:name "foo"}
          rule {:when ["=" "name" "foo"]
                :groups ["bar"]}
          expected ["bar"]]
      (is (= (apply-rule node rule) expected)))))
