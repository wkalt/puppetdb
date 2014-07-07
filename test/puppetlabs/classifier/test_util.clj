(ns puppetlabs.classifier.test-util)

(defn vec->tree
  [[group & children]]
  {:group group
   :children (set (map vec->tree children))})
