(ns puppetlabs.classifier.schema
  (:require [clojure.walk :as walk]
            [clj-time.format :as fmt-time]
            [schema.core :as sc]
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
   :facts {sc/Keyword sc/Any}
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
         :children #{(sc/recursive #'ValidationNode)}))

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
