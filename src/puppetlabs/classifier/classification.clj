(ns puppetlabs.classifier.classification
  (:require [clojure.set :refer [difference intersection subset? union]]
            [clojure.walk :as walk]
            [puppetlabs.kitchensink.core :refer [deep-merge deep-merge-with]]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Classification ClassificationConflict
                                                  ClassificationOutput ExplainedConflict Group
                                                  group->classification HierarchyNode Node
                                                  PuppetClass Rule SubmittedNode ValidationNode]]
            [puppetlabs.classifier.util :refer [merge-and-clean]]
            [schema.core :as sc]))

(sc/defn matching-groups :- [java.util.UUID]
  "Given a node and the set of rules to apply, return the ids of all groups the
  node is classified into."
  [node :- SubmittedNode, rules :- [Rule]]
  (->> rules
       (map (partial rules/apply-rule node))
       (keep identity)))

(sc/defn collapse-to-inherited :- Classification
  "Given a group classification and the chain of its ancestors' classifications,
  return a single classification representing all inherited values."
  ([inheritance :- [Classification]]
   (apply merge-and-clean (reverse inheritance)))
  ([group-classification :- Classification
    ancestors :- [Classification]]
   (collapse-to-inherited (concat [group-classification] ancestors))))

(sc/defn inheritance-maxima :- [Group]
  "Given a map of groups to their ancestors, determine the maxima according to
  the partial ordering defined by inheritance (i.e. a < b if a is an ancestor of
  b). Since it is a partial ordering, it is possible to have multiple maxima."
  [group->ancestors :- {Group [Group]}]
  (let [descendent-of? (fn [g descendent?]
                         (let [ancs (->> (get group->ancestors descendent?)
                                      (map :id)
                                      set)]
                           (contains? ancs (:id g))))]
    (reduce (fn [maxima group]
              (let [maxima' (remove #(descendent-of? % group) maxima)]
                (if (some (partial descendent-of? group) maxima')
                  maxima'
                  (conj maxima' group))))
            []
            (keys group->ancestors))))

(sc/defn conflicts :- (sc/maybe ClassificationConflict)
  "Return a map conforming to the ClassificationConflict schema that describes
  the conflicts between the given classifications, or nil if there are no
  conflicts. The ClassificationConflict only contains paths to conflicting
  values; all others are omitted. A conflict is when multiple
  classifications define different values for the environment, a class
  parameter, or a variable."
  [classifications :- [Classification]]
  ;; it is easiest to understand this function by starting with the threading
  ;; macro form at the end of the let bindings, then working up from there.
  (let [conflicts->sets (fn [a b]
                          ;; deep-merge-with uses this fn to reduce the vals
                          (into #{} (remove nil? (if (set? a)
                                                   (conj a b)
                                                   (set [a b])))))
        map-entry? #(and (vector? %)
                         (= (count %) 2)
                         (keyword? (first %)))
        conflicting-entry? (fn [[k v]]
                             ;; A key-value entry in a map is a conflicting entry if its value is
                             ;; a set with more than one element, which represents a conflict.
                             ;; However, we don't want to inadvertantly remove all the parents along
                             ;; a path to a conflicting entry, so we preserve these paths by also
                             ;; considering an entry whose value is a map a conflicting one. Since
                             ;; the omit-nonconflicting-keys postwalk transformation removes empty
                             ;; maps (i.e. those containing only non-conflicting entries) _after_ we
                             ;; have had a chance to remove all the entries (due to the `post` part
                             ;; of the `postwalk`), we should just preserve nested maps.
                             (or (map? v)
                                 (and (set? v) (> (count v) 1))))
        omit-nonconflicting-keys (fn [form]
                                   ;; replace with nil any form that is not either a map entry that
                                   ;; leads directly or indirectly,
                                   (if (or (and (map-entry? form) (not (conflicting-entry? form)))
                                           (and (map? form) (empty? form)))
                                     nil
                                     form))
        trump-envs (->> classifications
                     (filter :environment-trumps)
                     (map :environment)
                     set)
        conflicts (->> classifications
                    (map #(dissoc % :environment-trumps))
                    ;; conflicts->sets examines the provided values and produces a set whenever
                    ;; there are multiple distinct values to choose from during the merge (meaning
                    ;; there is a conflict).
                    (apply deep-merge-with conflicts->sets)
                    ;; After the deep merge above, all conflicts have been turned in to sets, so use
                    ;; a postwalk transformation to remove all paths that don't lead to sets.
                    ;; A postwalk is necessary in order to also eliminate empty maps coming up out
                    ;; of the leaves, otherwise classes without any conflicting parameters would
                    ;; still have an empty map in the returned value.
                    (walk/postwalk omit-nonconflicting-keys))]
    (cond
      (not (contains? conflicts :environment))
      conflicts

      (empty? trump-envs) ; nobody has a trump environment, so there are still conflicts
      conflicts

      (> (count trump-envs) 1)
      (assoc conflicts :environment trump-envs)

      :else ; only one trump environment
      (let [without-env (dissoc conflicts :environment)]
        (if (seq without-env)
          without-env)))))

(sc/defn merge-classifications :- (sc/maybe ClassificationOutput)
  "Takes a list of Classifications. If there are no conflicts between the
  classifications, returns a ClassificationOutput map representing the final
  merged classification. If there are conflicts, returns nil."
  [classifications :- [Classification]]
  (if (conflicts classifications)
    nil
    (let [trump-envs (->> classifications
                       (filter :environment-trumps)
                       (map :environment)
                       set)
          envs (->> classifications
                 (remove :environment-trumps)
                 (map :environment)
                 set)
          merged-besides-env (->> classifications
                               (map #(dissoc % :environment :environment-trumps))
                               (apply deep-merge))]
      (if (empty? trump-envs)
        ;; since there are no conflicts, if there are no trump environments
        ;; then all environments must be the same
        (assoc merged-besides-env :environment (first envs))
        ;; again since there aren't any conflicts, there must be only one
        ;; distinct trump environment
        (assoc merged-besides-env :environment (first trump-envs))))))

(defn- inherited-classifications
  [leaves group->ancestors]
  (into {} (for [{id :id, :as group} leaves
                 :let [ancestors (group->ancestors group)
                       class8ns (map group->classification (concat [group] ancestors))]]
             [id (collapse-to-inherited class8ns)])))

(defn classification-steps
  "Takes a node and a map from groups that the node matches to the group's
  ancestors, and returns a map with information on all steps of the
  classification process:
    * :match-explanations - a map from a group id to an explanation of why its
                            rule matched the node.
    * :leaves-by-id - a map between the id and group of the classification
                      leaves (that is, those matched groups that are not the
                      ancestors of any of the other matched groups).
    * :inherited-leaf-classifications - a map from each leaf's id to the
                                        classification it provides, including
                                        inherited values.
    * :conflicts - the conflicts between the leaf classifications. If there are
                   no conflicts, this value is nil.
    * :classification - if there are no conflicts, this is the result of
                        merging all the leaf classifications while properly
                        handling the environment-trumps flags of any of the
                        classifications. If there are conflicts, this value is
                        nil."
  [node matching-group->ancestors]
  (let [leaves (inheritance-maxima matching-group->ancestors)
        leaf-id->classification (inherited-classifications leaves matching-group->ancestors)
        match-explanations (into {} (for [{:keys [id rule]} (keys matching-group->ancestors)]
                                      [id (rules/explain-rule rule node)]))]
    {:match-explanations match-explanations
     :id->leaf (into {} (map (juxt :id identity) leaves))
     :leaf-id->classification leaf-id->classification
     :conflicts (conflicts (vals leaf-id->classification))
     :classification (merge-classifications (vals leaf-id->classification))}))

(defn- conflict-details
  [path values group->ancestors]
  (let [class8n->group (into {} (for [[g ancs] group->ancestors]
                                  (let [class8n (collapse-to-inherited
                                                  (map group->classification (concat [g] ancs)))]
                                    [class8n g])))]
    (set (for [v values
               class8n (filter #(= v (get-in % path)) (keys class8n->group))
               :let [matching-group (class8n->group class8n)
                     defining-group (->> matching-group
                                      group->ancestors
                                      (concat [matching-group])
                                      (filter #(= v (get-in % path)))
                                      first)]]
           {:value v
            :from matching-group
            :defined-by defining-group}))))

(sc/defn explain-conflicts :- ExplainedConflict
  "Takes a ClassificationConflict as produced by conflicts and a map that maps
  between every group that could have contributed to the classification and all
  its ancestors, and creates an ExplainedConflict instance by transforming each
  conflicting value set into a set of ConflictDetail instances, which include
  the value, the matched group whose inherited classification set the value, and
  the group that actually defined the value."
  [conflicts :- ClassificationConflict, group->ancestors :- {Group [Group]}]
  (let [g->as group->ancestors
        env-details (conflict-details [:environment] (:environment conflicts) g->as)
        classes-details (into {} (for [[c params] (:classes conflicts)]
                                   [c (into {} (for [[p vs] params]
                                                 [p (conflict-details [:classes c p]
                                                                      vs, g->as)]))]))
        var-details (into {} (for [[v values] (:variables conflicts)]
                               [v (conflict-details [:variables v] values g->as)]))]
    (merge (if-not (empty? env-details) {:environment env-details})
           (if-not (empty? classes-details) {:classes classes-details})
           (if-not (empty? var-details) {:variables var-details}))))

(sc/defn group-referencing-class :- (sc/maybe Group)
  "Given a class and a list of ancestors, returns the closest ancestral group
  that references the class."
  [class :- sc/Keyword, ancestors :- [Group]]
  (->> ancestors
    (filter #(contains? (:classes %) class))
    first))

(sc/defn group-referencing-parameter :- (sc/maybe Group)
  "Given a class parameter and a list of ancestors, returns the closest
  ancestral group that defines the given class parameter."
  [class :- sc/Keyword, parameter :- sc/Keyword, ancestors :- [Group]]
  (->> ancestors
    (filter #(contains? (get-in % [:classes class]) parameter))
    first))

(sc/defn unknown-parameters
  "Returns a map describing classes and class parameters the group defines which
  are not defined by those classes in `classes` that are in the group's
  environment. The keys of the map are keyworded class names, but the values
  differ depending on whether the class exists but has different parameters, or
  could not be found at all. If the class exists, but the group defines
  extraneous parameters, the value will be a set of the extraneous parameter
  names (as keywords); if the class does not exist in the group's environment,
  then the value will be nil.  Note that it is the caller's responsibility to
  add inherited values if the caller wishes inherited values to be checked."
  [classification :- Classification, classes :- [PuppetClass]]
  (let [class8n-env (-> classification :environment keyword)
        env-classes-by-kw-name (into {} (for [{:keys [environment name] :as c} classes]
                                          (if (= (keyword environment) class8n-env)
                                            [(keyword name) c])))]
    (reduce merge
          (for [[class-key class8n-params] (:classes classification)]
            (if-let [class (get env-classes-by-kw-name class-key)]
              (let [class8n-params-set (-> class8n-params keys set)
                    extant-params-set (-> class :parameters keys set)]
                (if-not (subset? class8n-params-set extant-params-set)
                  {class-key (difference class8n-params-set extant-params-set)}))
              ;; else (class doesn't exist)
              {class-key nil})))))

(sc/defn validation-tree :- ValidationNode
  "Turn a group hierarchy tree into a validation tree, which is composed of
  ValidationNodes rather than HierarchyNodes. The only difference is that
  a ValidationNode contains an :errors key with the group's validation failures
  (if any) as returned by `unknown-parameters`."
  ([tree :- HierarchyNode, classes :- [PuppetClass]]
   (validation-tree tree classes ()))
  ([tree :- HierarchyNode, classes :- [PuppetClass], ancestors :- [Classification]]
   (assert (seq? ancestors))
   (let [{:keys [group children]} tree
         group-classification (group->classification group)
         inherited (collapse-to-inherited group-classification ancestors)
         recurse #(validation-tree % classes (conj ancestors group-classification))]
     {:group group
      :errors (unknown-parameters inherited classes)
      :children (set (map recurse children))})))

(sc/defn valid-tree? :- Boolean
  "Returns true if the given validation tree has no errors."
  [validation-tree :- ValidationNode]
  (let [{:keys [errors children]} validation-tree]
    (if-not (empty? errors)
      false
      (every? identity (map valid-tree? children)))))
