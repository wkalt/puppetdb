(ns puppetlabs.classifier.rules
  (:require [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.kitchensink.core :refer [parse-number]]
            [puppetlabs.classifier.schema :refer [ExplainedCondition Rule RuleCondition]])
  (:import java.util.regex.Pattern
           java.util.UUID))

(def always-matches ["~" "name" ".*"])

(defn- lookup-field
  [m ks]
  (let [get* (fn [m k] (or (get m (keyword k)) (get m (str k))))]
    (if (sequential? ks)
      (reduce get* m ks)
      (get* m ks))))

(sc/defn match? :- Boolean
  [rule-condition :- RuleCondition, node]
  (let [[operator & args] rule-condition]
    (case operator
      "not" (not (match? (first args) node))
      "and" (every? identity (map #(match? % node) args))
      "or" (boolean (some identity (map #(match? % node) args)))
      (let [[field target] args
            numeric-operator? (contains? #{"<" "<=" ">" ">="} operator)
            node-value (let [v (lookup-field node field)]
                         (cond
                           (and numeric-operator? (string? v)) (parse-number v)
                           (or (true? v) (false? v)) (str v)
                           :else v))
            target-value (if (and numeric-operator? (string? target))
                           (parse-number target)
                           target)]
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

(sc/defn apply-rule :- (sc/maybe UUID)
  "Apply a single rule to a node"
  [node rule :- Rule]
  (if (match? (:when rule) node)
    (:group-id rule)))

(sc/defn explain-rule* :- ExplainedCondition
  [node, condition :- RuleCondition]
  (let [recurse (partial explain-rule* node)
        op (first condition)]
    (case op
      "not" {:value (match? condition node)
             :form ["not" (recurse (last condition))]}

      ("and" "or") {:value (match? condition node)
                    :form (vec (cons op (map recurse (rest condition))))}

      ("=" "~" "<" "<=" ">" ">=") {:value (match? condition node)
                                   :form (let [[op field target] condition
                                               node-value {:path field
                                                           :value (lookup-field node field)}]
                                           [op node-value target])})))

(sc/defn explain-rule :- ExplainedCondition
  [rule :- RuleCondition, node]
  (explain-rule* node rule))

(sc/defn ^:always-validate condition->pdb-query
  [condition :- RuleCondition]
  (let [[op & tail] condition]
    (if (contains? #{"and" "or" "not"} op)
      (cons op (map condition->pdb-query tail))

      (let [[field value] tail]
        (if-not (sequential? field)
          [op field value]

          (let [[kind & path] field]
            (cond
              (> (count path) 1)
              (throw+ {:kind ::illegal-puppetdb-query
                       :msg "PuppetDB does not support structured facts."
                       :condition condition})

              (= kind "trusted")
              (throw+ {:kind ::illegal-puppetdb-query
                       :msg "PuppetDB does not support trusted facts."
                       :condition condition})

              :else
              ["and" ["=" "name" (first path)]
                     [op "value" value]])))))))
