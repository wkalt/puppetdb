(ns puppetlabs.classifier.schema
  (:require [clojure.walk :as walk]
            [clj-time.format :as fmt-time]
            [fast-zip.core :as z]
            [fast-zip.visit :as zv]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.storage :refer [root-group-uuid]]
            [puppetlabs.classifier.util :refer [map-delta uuid?]])
  (:import java.util.UUID))

;; Schemas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ISO8601EncodedDateTime
  (sc/pred (fn [x]
             (let [iso-formatter (fmt-time/formatters :date-time-no-ms)]
               (and (string? x)
                    (not= :errored (try (fmt-time/unparse iso-formatter x)
                                     (catch Throwable _ ::errored))))))))

(def UUIDRepresentation
  (sc/pred #(and (or (string? %) (keyword? %))
                 (uuid? %))))

(def NodeField
  (sc/if #'sequential?
    [String]
    String))

(def NodeValue
  {:path NodeField
   :value sc/Any})

(def Environment {:name String})

(def PuppetClass ; so named to avoid clashing with java.lang.Class
  {:name String
   :parameters {sc/Keyword sc/Any}
   :environment String})

(def RuleCondition
  (sc/either
    [(sc/one (sc/eq "not") "negation") (sc/one (sc/recursive #'RuleCondition) "negated-expression")]

    [(sc/one (sc/eq "and") "conjunction")
     (sc/one (sc/recursive #'RuleCondition) "first-term") (sc/recursive #'RuleCondition)]

    [(sc/one (sc/eq "or") "disjunction")
     (sc/one (sc/recursive #'RuleCondition) "first-term") (sc/recursive #'RuleCondition)]

    [(sc/one (sc/enum "=" "~" "<" "<=" ">" ">=") "operator")
     (sc/one NodeField "field")
     (sc/one String "target-value")]))

(def ExplainedCondition
  {:value boolean
   :form (walk/prewalk
           (fn [form]
             (cond
               (and (= (type form) schema.core.Recursive)
                    (= (:schema-var form) #'RuleCondition))
               (sc/recursive #'ExplainedCondition)

               (= form (sc/one NodeField "field"))
               (sc/one NodeValue "field-with-value")

               :else form))
           RuleCondition)})

(def Rule
  {:when RuleCondition
   :group-id UUID
   (sc/optional-key :id) Number})

(def Classification
  {:environment String
   :environment-trumps boolean
   :classes {sc/Keyword (sc/maybe {sc/Keyword sc/Any})}
   :variables {sc/Keyword sc/Any}})

(def ClassificationOutput
  (dissoc Classification :environment-trumps))

(def ClassificationConflict
  {(sc/optional-key :environment) #{String}
   (sc/optional-key :classes) {sc/Keyword {sc/Keyword #{sc/Any}}}
   (sc/optional-key :variables) {sc/Keyword #{sc/Any}}})

(def CheckIn
  {:node String
   :time org.joda.time.DateTime
   :explanation {UUID ExplainedCondition}
   (sc/optional-key :classification) ClassificationOutput
   (sc/optional-key :transaction-uuid) UUID})

(def ClientCheckIn
  (-> CheckIn
    (dissoc (sc/optional-key :transaction-uuid))
    (assoc :time ISO8601EncodedDateTime
           :explanation {UUIDRepresentation ExplainedCondition}
           (sc/optional-key :transaction_uuid) UUIDRepresentation)))

(def Node
  {:name String
   :check-ins [(dissoc CheckIn :node)]})

(def ClientNode
  {:name String
   :check_ins [(dissoc ClientCheckIn :node)]})

(def SubmittedNode
  {:name String
   :fact {sc/Keyword sc/Any}
   :trusted {sc/Keyword sc/Any}})

(def Group
  (assoc Classification
         :name String
         :id UUID
         :parent UUID
         (sc/optional-key :rule) RuleCondition
         (sc/optional-key :description) String))

(def AnnotatedGroup
  (assoc Group
         (sc/optional-key :deleted) {sc/Keyword {:puppetlabs.classifier/deleted boolean
                                                 sc/Keyword {:puppetlabs.classifier/deleted boolean
                                                             :value String}}}))

(def GroupDelta
  {:id UUID
   (sc/optional-key :name) String
   (sc/optional-key :environment) String
   (sc/optional-key :environment-trumps) boolean
   (sc/optional-key :description) String
   (sc/optional-key :parent) UUID
   (sc/optional-key :rule) (sc/maybe RuleCondition)
   (sc/optional-key :classes) {sc/Keyword (sc/maybe {sc/Keyword sc/Any})}
   (sc/optional-key :variables) {sc/Keyword sc/Any}})

(def ConflictDetails
  {:value sc/Any
   :from Group
   :defined-by Group})

(def ExplainedConflict
  {(sc/optional-key :environment) #{ConflictDetails}
   (sc/optional-key :classes) {sc/Keyword {sc/Keyword #{ConflictDetails}}}
   (sc/optional-key :variables) {sc/Keyword #{ConflictDetails}}})

(def ClientExplainedConflict
  (walk/prewalk (fn [form]
                  (if (= form UUID)
                    UUIDRepresentation
                    form))
                ExplainedConflict))

(def HierarchyNode
  {:group Group
   :children #{(sc/recursive #'HierarchyNode)}})

(def ValidationNode
  (assoc HierarchyNode
         :errors (sc/maybe {sc/Keyword sc/Any})
         :children (sc/maybe #{(sc/recursive #'ValidationNode)})))

;; Utilities for creating & converting maps conforming to the Schemas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(sc/defn group->classification :- Classification
  [group :- Group]
  (select-keys group (keys Classification)))

(defn group->rule
  [group]
  {:when (:rule group) :group-id (:id group)})

(sc/defn group-delta :- GroupDelta
  "Returns a delta that, when applied, turns group `g` into group `h`"
  [g :- Group, h :- Group]
  {:pre [(= (:id g) (:id h))]}
  (assoc (map-delta g h)
         :id (:id g)))

(defn- extract-cycle
  ([id id->parent-id id->group] (extract-cycle id id->parent-id id->group [id]))
  ([id id->parent-id id->group cycle]
   (let [last-id (or (last cycle) id)
         next-parent-id (get id->parent-id last-id)]
     (if (= next-parent-id id)
       (map id->group cycle)
       (recur id id->parent-id id->group (conj cycle next-parent-id))))))

(defn- children-by-id->parent-by-id
  [id->children]
  (reduce (fn [id->parent [id children]]
            (apply assoc id->parent (interleave (map :id children) (repeat id))))
          {}
          id->children))

(defn- groups->tree*
  [{id :id :as group} id->group id->children !marked]
  (when (get @!marked id)
    (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
             :cycle (extract-cycle id (children-by-id->parent-by-id id->children) id->group)}))
  (let [children (if (= id root-group-uuid)
                   (remove #(= (:id %) root-group-uuid) (id->children id))
                   (id->children id))]
    (swap! !marked assoc id true)
    {:group group
     :children (set (map #(groups->tree* % id->group id->children !marked) children))}))

(sc/defn groups->tree :- HierarchyNode
  "Converts a flat collection of groups into a group hierarchy tree. The groups
  must form a complete, valid hierarchy wherein the root group has the expected
  id, all groups have the root as an ancestor, and there are no cycles. If any
  of these conditions are violated then an exception will be thrown."
  [groups :- [Group]]
  (when-not (apply distinct? (map :id groups))
    (let [duplicated-ids (for [[id groups] (group-by :id groups)
                               :when (> (count groups) 1)]
                           id)
          dupe-count (count duplicated-ids)]
      (throw+ {:kind :puppetlabs.classifier.storage.postgres/uniqueness-violation
               :entity-kind "group"
               :constraint "groups_pkey"
               :fields (repeat dupe-count "id")
               :values duplicated-ids})))
  (let [group-maps (loop [gs groups, id->g {}, id->c {}]
                     (if-not (seq gs)
                       [id->g id->c]
                       (let [g (first gs)]
                         (recur (next gs)
                                (assoc id->g (:id g) g)
                                (update-in id->c [(:parent g)] (fnil conj []) g)))))
        [id->group id->children] group-maps
        !marked (atom {})
        tree (groups->tree* (get id->group root-group-uuid) id->group id->children !marked)
        unreachable-groups (remove #(get @!marked (:id %)) groups)]
    (when (seq unreachable-groups)
      (throw+ {:kind :puppetlabs.classifier/unreachable-groups
               :groups unreachable-groups}))
    tree))

(defn- hierarchy-node?
  "Predicate that returns true if `x` conforms to either the HierarchyNode or
  ValidationNode schema."
  [x]
  (or (nil? (sc/check HierarchyNode x))
      (nil? (sc/check ValidationNode x))))

(def hierarchy-zipper
  (partial z/zipper hierarchy-node? (comp seq :children) (fn [n cs] (assoc n :children (set cs)))))

(defn tree->groups
  [root-node]
  (let [zip-root (hierarchy-zipper root-node)
        group-accum (zv/visitor
                      :pre [node groups]
                      {:state (conj groups (:group node))})]
    (:state (zv/visit zip-root [] [group-accum]))))
