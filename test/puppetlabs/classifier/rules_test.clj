(ns puppetlabs.classifier.rules-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.rules :refer :all]))

(deftest rules
  (testing "simplest classification"
    (let [node {:name "foo"}
          rule {:when ["=" "name" "foo"]
                :groups ["bar"]}
          expected ["bar"]]
      (is (= (apply-rule node rule) expected)))))
