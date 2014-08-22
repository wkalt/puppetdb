(ns puppetlabs.classifier.test-util
  (:require [puppetlabs.classifier.storage :refer [root-group-uuid]]
            [puppetlabs.kitchensink.core :refer [deep-merge mapvals]])
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

(defn extract-classes
  [groups]
  (let [by-env (group-by :environment groups)
        w-classes (mapvals (fn [gs] (apply deep-merge (map :classes gs))) by-env)]
    (for [[env classes] w-classes
          [class-kw params] classes]
      {:name (name class-kw)
       :environment (name env)
       :parameters (mapvals (constantly nil) params)})))
