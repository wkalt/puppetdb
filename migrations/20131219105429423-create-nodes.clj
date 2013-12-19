;; migrations/20131219105429423-create-nodes.clj

(defn up []
  ["CREATE TABLE nodes (name TEXT PRIMARY KEY)"])

(defn down []
  ["DROP TABLE nodes"])
