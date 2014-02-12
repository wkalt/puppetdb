(ns puppetlabs.classifier.rules
  (:require [schema.core :as sc]
            [puppetlabs.kitchensink.core :refer [parse-number]]
            [puppetlabs.classifier.schema :refer [Rule RuleCondition]])
  (:import java.util.regex.Pattern))

(sc/defn match?
  [rule-condition :- RuleCondition, node]
  (let [[operator & args] rule-condition]
    (case operator
      "not" (not (match? (first args) node))
      "and" (every? identity (map #(match? % node) args))
      "or" (boolean (some identity (map #(match? % node) args)))
      (let [[field target] args
            numeric-operator? (contains? #{"<" "<=" ">" ">="} operator)
            node-value (let [v (or (get node (keyword field))
                                   (get node field))]
                         (if numeric-operator? (parse-number v) v))
            target-value (if numeric-operator? (parse-number target) target)]
        (if (and node-value target-value)
          (case operator
            "=" (= node-value target-value)
            "~" (boolean (re-find (Pattern/compile target-value) node-value))
            "<" (< node-value target-value)
            "<=" (<= node-value target-value)
            ">" (> node-value target-value)
            ">=" (>= node-value target-value)
            false)
          false)))))

(defn apply-rule
  "Apply a single rule to a node"
  [node rule]
  {:pre [(map? node)]
   :post [seq?]}
  (if (match? (:when rule) node)
    (rule :groups)))
