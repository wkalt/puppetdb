;; migrations/20131219105544413-create-groups.clj

(defn up []
  ["CREATE TABLE groups (name TEXT PRIMARY KEY)"])

(defn down []
  ["DROP TABLE groups"])
