(ns puppetlabs.classifier.storage.sql-utils-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [keywordize-keys]]
            [puppetlabs.classifier.storage.sql-utils :refer :all]))

(def test-vals
  '({:value "one-val",
     :parameter "one",
     :class "first",
     :environment "test",
     :name "group-params"}
    {:value "two-val",
     :parameter "two",
     :class "first",
     :environment "test",
     :name "group-params"}
    {:value "three-val"
     :parameter "three"
     :class "second"
     :environment "test"
     :name "group-params"}))

(deftest aggregation
  (testing "handles nils"
    (is (= '({:a 1 :b 2 :cs {}})
           (->> '({:a 1 :b 2 :c nil :d nil})
                (aggregate-into :cs [:c :d])))))
  (testing "nested calls"
    (is (= '({:name "group-params"
            :environment "test"
            :classes {:first {:one "one-val"
                              :two "two-val"}
                      :second {:three "three-val"}}})
           (->> test-vals
             (aggregate-into :parameters [:parameter :value])
             (aggregate-into :classes [:class :parameters])
             (keywordize-keys))))))
