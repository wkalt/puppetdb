(ns puppetlabs.classifier.storage.sql-utils-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [keywordize-keys]]
            [puppetlabs.classifier.storage.sql-utils :refer :all]))

(deftest submap-aggregation
  (testing "handles nils"
    (is (= '({:a 1 :b 2 :cs {}})
           (->> '({:a 1 :b 2 :c nil :d nil})
                (aggregate-submap-by :c :d :cs)))))
  (testing "nested calls"
    (let [rows [{:value "one-val",
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
                 :name "group-params"}]]
      (is (= '({:name "group-params"
                :environment "test"
                :classes {:first {:one "one-val"
                                  :two "two-val"}
                          :second {:three "three-val"}}})
             (->> rows
               (aggregate-submap-by :parameter :value :parameters)
               (aggregate-submap-by :class :parameters :classes)
               (keywordize-keys)))))))

(deftest column-aggregation
  (testing "aggregates as expected"
    (let [rows [{:white 1 :blue 2 :green 0}
                {:white 1 :blue 3 :green 0}
                {:white 1 :blue 4 :green 1}]]
      (is (= [{:white 1 :blues [2 3] :green 0} {:white 1 :blues [4] :green 1}]
             (aggregate-column :blue :blues rows))))))
