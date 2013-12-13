(ns puppetlabs.classifier.rules)

(defn match
  [[condition field value] node]
  (if (= condition "=")
    (= (get node (keyword field)) value)))

(defn apply-rule
  "Apply a single rule to a node"
  [node rule]
  {:pre [(map? node)]
   :post [seq?]}
  (when (match (rule :when) node)
    (rule :groups)))
