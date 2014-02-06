(ns puppetlabs.classifier.rules-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [puppetlabs.classifier.rules :refer :all]))

(use-fixtures :once schema.test/validate-schemas)

(defn is-rule-match
  [rule node]
  (is (= (apply-rule node rule) (:groups rule))))

(deftest rules
  (testing "simplest classification"
    (let [foo-node {:name "foo"}
          bar-node {:name "bar"}
          rule {:when ["=" "name" "foo"]
                :groups ["some-group"]}]
      (is-rule-match rule foo-node)
      (is (nil? (apply-rule bar-node rule)))))

  (testing "regex-based classifications"
    (let [node {:name "foo.example.com"}
          containment-rule {:when ["~" "name" "foo"]
                            :groups ["bar"]}
          regex-rule {:when ["~" "name" "^[^.]+\\.example\\.com$"]
                      :groups ["bar"]}]
      (is-rule-match containment-rule node)
      (is-rule-match regex-rule node)))

  (testing "numeric comparison classes"
    (let [node {"total_disk" (-> 1.074e11 long str)
                "total_ram" (-> 8.59e9 long str)
                "uptime" "3600"
                "load" "0.03"}
          badval-node {"load" "NaN"}
          database-rule {:when [">=" "total_ram" (-> 6.442e9 long str)]
                         :groups ["postgres"]}
          filebucket-rule {:when [">" "total_disk" (-> 5e10 long str)]
                           :groups ["filebucket"]}
          yungins-rule {:when ["<=" "uptime" "3600"] :groups ["yungin's"]}
          slackers-rule {:when ["<" "load" "0.1"] :groups ["slackers"]}]
      (is-rule-match database-rule node)
      (is-rule-match filebucket-rule node)
      (is-rule-match yungins-rule node)
      (is-rule-match slackers-rule node)
      (is (nil? (apply-rule badval-node slackers-rule)))))

  (testing "classifications with negations"
    (let [node {:name "foo"}
          neg-rule {:when ["not" ["=" "name" "bar"]] :groups ["baz"]}]
      (is-rule-match neg-rule node))))
