(ns puppetlabs.classifier.rules-test
  (:require [clojure.test :refer :all]
            [schema.core :as sc]
            [schema.test]
            [puppetlabs.classifier.schema :refer [RuleCondition]]
            [puppetlabs.classifier.rules :refer :all]))

(use-fixtures :once schema.test/validate-schemas)

(defn is-rule-match
  [rule node]
  (is (= (apply-rule node rule) (:groups rule))))

(defn is-not-rule-match
  [rule node]
  (is (nil? (apply-rule node rule))))

(deftest simple-equality-rule
  (testing "simplest classification"
    (let [foo-node {:name "foo"}
          bar-node {:name "bar"}
          rule {:when ["=" "name" "foo"]
                :groups ["some-group"]}]
      (is-rule-match rule foo-node)
      (is-not-rule-match rule bar-node))))

(deftest regex-rules
  (testing "regex-based classification"
    (let [foo-node {:name "foo.example.com"}
          bar-node {:name "bar.example.com"}
          baz-node {:name "foo.template.com"}
          containment-rule {:when ["~" "name" "foo"]
                            :groups ["bar"]}
          regex-rule {:when ["~" "name" "^[^.]+\\.example\\.com$"]
                      :groups ["bar"]}]
      (testing "simple containment rules match or don't as expected"
        (is-rule-match containment-rule foo-node)
        (is-rule-match containment-rule baz-node))
        (is-not-rule-match containment-rule bar-node)
      (testing "fancy regex rules match or don't as expected"
        (is-rule-match regex-rule foo-node)
        (is-rule-match regex-rule bar-node)
        (is-not-rule-match regex-rule baz-node)))))

(deftest numeric-rules
  (testing "numeric comparison classification"
    (let [match-node {"total_disk" (-> 1.074e11 long str)
                      "total_ram" (-> 8.59e9 long str)
                      "uptime" "3600"
                      "load" "0.03"}
          miss-node {"total_disk" (-> 9e9 long str)
                     "total_ram" (-> 4e9 long str)
                     "uptime" (str (* 60 60 24))
                     "load" "0.8"}
          badval-node {"load" "what's a number?"}
          database-rule {:when [">=" "total_ram" (-> 6.442e9 long str)]
                         :groups ["postgres"]}
          filebucket-rule {:when [">" "total_disk" (-> 5e10 long str)]
                           :groups ["filebucket"]}
          yungins-rule {:when ["<=" "uptime" "3600"] :groups ["yungin's"]}
          slackers-rule {:when ["<" "load" "0.1"] :groups ["slackers"]}]
      (testing "> operator works as expected"
        (is-rule-match filebucket-rule match-node)
        (is-not-rule-match filebucket-rule miss-node))
      (testing ">= operator works as expected"
        (is-rule-match database-rule match-node)
        (is-not-rule-match database-rule miss-node))
      (testing "< operator works as expected"
        (is-rule-match slackers-rule match-node)
        (is-not-rule-match slackers-rule miss-node))
      (testing "<= operator works as expected"
        (is-rule-match yungins-rule match-node)
        (is-not-rule-match yungins-rule miss-node))
      (testing "non-numeric fact values cause numeric operators to fail"
        (is-not-rule-match slackers-rule badval-node)))))

(deftest boolean-expression-rules
  (testing "full-on boolean expression classification"
    (let [ds9-node {"holodecks" "14"
                    "bars" "1"
                    "warp_cores" "0"
                    "docking_pylons" "3"}
          ncc-1701-d-node {"bars" "1"
                           "holodecks" "5"
                           "warp_cores" "1"
                           "docking_pylons" "0"}
          ds9-rule {:when ["and"
                           ["=" "bars" "1"]
                           [">" "holodecks" "5"]
                           ["not" [">" "warp_cores" "0"]]
                           ["=" "docking_pylons" "3"]]
                    :groups ["DS9"]}
          ncc-1701-d-rule {:when ["or"
                                  ["=" "warp_cores" "1"]
                                  ["=" "docking_pylons" "0"]]}]
      (is-rule-match ds9-rule ds9-node)
      (is-not-rule-match ds9-rule ncc-1701-d-node)
      (is-rule-match ncc-1701-d-rule ncc-1701-d-node)
      (is-not-rule-match ncc-1701-d-rule ds9-node))))
