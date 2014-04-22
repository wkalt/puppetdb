(ns puppetlabs.classifier.schema
  (:require [schema.core :as sc]
            [puppetlabs.classifier.util :refer [map-delta]]))

;; Schemas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def Node {:name String})

(def SubmittedNode
  {:name String
   :facts {sc/Keyword sc/Any}
   :trusted {sc/Keyword sc/Any}})

(def Environment {:name String})

(def PuppetClass ; so named to avoid clashing with java.lang.Class
  {:name String
   :parameters {sc/Keyword (sc/maybe String)}
   :environment String})

(def RuleCondition
  (sc/either
    [(sc/one (sc/eq "not") "negation") (sc/one (sc/recursive #'RuleCondition) "negated-expression")]

    [(sc/one (sc/eq "and") "conjunction")
     (sc/one (sc/recursive #'RuleCondition) "first-term") (sc/recursive #'RuleCondition)]

    [(sc/one (sc/eq "or") "disjunction")
     (sc/one (sc/recursive #'RuleCondition) "first-term") (sc/recursive #'RuleCondition)]

    [(sc/one (sc/enum "=" "~" "<" "<=" ">" ">=") "operator")
     (sc/one (sc/either String [String]) "field")
     (sc/one String "target-value")]))

(def Rule
  {:when RuleCondition
   :group-id java.util.UUID
   (sc/optional-key :id) Number})

(def Classification
  {:environment String
   :classes {sc/Keyword (sc/maybe {sc/Keyword (sc/maybe String)})}
   :variables {sc/Keyword sc/Any}})

(def ClassificationConflict
  {(sc/optional-key :environment) #{String}
   (sc/optional-key :classes) {sc/Keyword {sc/Keyword #{String}}}
   (sc/optional-key :variables) {sc/Keyword #{sc/Any}}})

(def Group
  (assoc Classification
         :name String
         :id java.util.UUID
         :parent java.util.UUID
         :rule RuleCondition
         (sc/optional-key :description) String))

(def AnnotatedGroup
  (assoc Group
         (sc/optional-key :deleted) {sc/Keyword {:puppetlabs.classifier/deleted boolean
                                                 sc/Keyword {:puppetlabs.classifier/deleted boolean
                                                             :value String}}}))

(def HierarchyNode
  {:group Group
   :children #{(sc/recursive #'HierarchyNode)}})

(def ValidationNode
  (assoc HierarchyNode
         :errors (sc/maybe {sc/Keyword sc/Any})
         :children #{(sc/recursive #'ValidationNode)}))

(def GroupDelta
  {:id java.util.UUID
   (sc/optional-key :name) String
   (sc/optional-key :environment) String
   (sc/optional-key :description) String
   (sc/optional-key :parent) java.util.UUID
   (sc/optional-key :rule) RuleCondition
   (sc/optional-key :classes) {sc/Keyword (sc/maybe {sc/Keyword (sc/maybe String)})}
   (sc/optional-key :variables) {sc/Keyword sc/Any}})

;; Utilities for creating & converting maps conforming to the Schemas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn group->classification
  [group]
  (dissoc group :name :id :description :parent :rule))

(sc/defn group-delta :- GroupDelta
  "Returns a delta that, when applied, turns group `g` into group `h`"
  [g :- Group, h :- Group]
  {:pre [(= (:id g) (:id h))]}
  (assoc (map-delta g h)
         :id (:id g)))
