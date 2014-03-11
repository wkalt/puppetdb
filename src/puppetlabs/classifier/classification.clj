(ns puppetlabs.classifier.classification
  (:require [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Classification Node Rule]]
            [puppetlabs.classifier.util :refer [merge-and-clean]]
            [schema.core :as sc]))

(sc/defn matching-groups :- [String]
  "Given a node and the set of rules to apply, find the names of all groups
  the node is classified into."
  [node :- Node, rules :- [Rule]]
  (->> rules
       (map (partial rules/apply-rule node))
       (keep identity)))

(sc/defn inherited-classification :- Classification
  "Given a group classification and the chain of its ancestors' classifications,
  return a single group representing all inherited values."
  ([inheritance :- [Classification]]
   (apply merge-and-clean (reverse inheritance)))
  ([group-classification :- Classification
    ancestors :- [Classification]]
   (inherited-classification (concat [group-classification]
                                     ancestors))))
