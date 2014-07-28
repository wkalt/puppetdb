(ns puppetlabs.classifier.test-util
  (:require [puppetlabs.classifier.storage :refer [root-group-uuid]])
  (:import java.util.UUID))

(defn vec->tree
  [[group & children]]
  {:group group
   :children (set (map vec->tree children))})

(defn blank-group
  []
  {:name "blank"
   :id (UUID/randomUUID)
   :parent root-group-uuid
   :rule ["=" "foo" "bar"]
   :environment "test"
   :environment-trumps false
   :classes {}
   :variables {}})

(defn blank-group-named
  [n]
  (merge (blank-group) {:name n}))

(defn blank-root-group
  []
  (merge (blank-group-named "default")
         {:id root-group-uuid
          :rule ["~" "name" ".*"]}))
