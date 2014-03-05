(ns puppetlabs.classifier.classification
  (:require [clojure.set :refer [difference subset?]]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Classification Group group->classification
                                                  HierarchyNode Node Rule ValidationNode]]
            [puppetlabs.classifier.util :refer [merge-and-clean]]
            [schema.core :as sc]))

(def PuppetClass puppetlabs.classifier.schema/Class)

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

(sc/defn unknown-parameters
  "Returns a map describing classes and class parameters the group defines which
  are not defined by those classes in `classes` that are in the group's
  environment. The keys of the map
  are keyworded class names, but the values differ depending on whether the
  class exists but has different parameters, or could not be found at all. If
  the class exists, but the group defines extraneous parameters, the value will
  be a set of the extraneous parameter names (as keywords); if the class does
  not exist in the group's environment, then the value will be nil.
  Note that it is the caller's responsibility to add inherited values if the
  caller wishes inherited values to be checked."
  [classification :- Classification
   classes :- [PuppetClass]]
  (let [clss8n-env (-> classification :environment keyword)
        env-classes-by-kw-name (into {} (for [{:keys [environment name] :as c} classes]
                                          (if (= (keyword environment) clss8n-env)
                                            [(keyword name) c])))]
    (reduce merge
          (for [[class-key clss8n-params] (:classes classification)]
            (if-let [class (get env-classes-by-kw-name class-key)]
              (let [clss8n-params-set (-> clss8n-params keys set)
                    extant-params-set (-> class :parameters keys set)]
                (if-not (subset? clss8n-params-set extant-params-set)
                  {class-key (difference clss8n-params-set extant-params-set)}))
              ;; else (class doesn't exist)
              {class-key nil})))))

(sc/defn validation-tree* :- ValidationNode
  [{:keys [group children]} :- HierarchyNode
   classes :- [PuppetClass]
   ancestors :- [Classification]]
  (let [group-classification (group->classification group)
        inherited (inherited-classification group-classification ancestors)
        recurse (fn [child] (validation-tree* child classes (conj ancestors group-classification)))]
    {:group group
     :errors (unknown-parameters inherited classes)
     :children (set (map recurse children))}))

(sc/defn validation-tree :- ValidationNode
  "Turn a group hierarchy tree into a validation tree, which is composed of
  ValidationNodes rather than HierarchyNodes. The only difference is that
  a ValidationNode contains an :errors key with the group's validation failures
  (if any) as returned by `unknown-parameters`.
  "
  [tree :- HierarchyNode
   classes :- [PuppetClass]]
  (validation-tree* tree classes ()))

(sc/defn valid-tree? :- Boolean
  "Returns true if the given validation tree has no errors."
  [validation-tree :- ValidationNode]
  (let [{:keys [errors children]} validation-tree]
    (if-not (empty? errors)
      false
      (every? identity (map valid-tree? children)))))
