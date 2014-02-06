(ns puppetlabs.classifier.rules
  (:require [schema.core :as sc]
            [puppetlabs.kitchensink.core :refer [parse-number]]
            [puppetlabs.classifier.schema :refer [Rule RuleCondition]])
  (:import java.util.regex.Pattern))

(sc/defn match
  [rule-condition :- RuleCondition, node]
  (if (= (first rule-condition) "not")
    (not (match (second rule-condition) node))
    (let [[operator field target] rule-condition
          numeric-operator? (contains? #{"<" "<=" ">" ">="} operator)
          node-value (let [v (or (get node (keyword field))
                                 (get node field))]
                       (if numeric-operator? (parse-number v) v))
          target-value (if numeric-operator? (parse-number target) target)]
      (boolean
        (if node-value
          (case operator
            "=" (= node-value target)
            "~" (re-find (Pattern/compile target) node-value)
            "<" (< node-value target-value)
            "<=" (<= node-value target-value)
            ">" (>= node-value target-value)
            ">=" (>= node-value target-value)
            false))))))

(defn apply-rule
  "Apply a single rule to a node"
  [node rule]
  {:pre [(map? node)]
   :post [seq?]}
  (if (match (:when rule) node)
    (rule :groups)))
