(ns puppetlabs.classifier.schema
  (:require [schema.core :as sc]))

(def Node {:name String})

(def Environment {:name String})

(def puppetlabs.classifier.schema/Class
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
     (sc/one String "field")
     (sc/one String "target-value")]))

(def Rule
  {:when RuleCondition
   (sc/optional-key :group-name) String
   (sc/optional-key :id) Number})

(def Group {:name String
            (sc/optional-key :id) java.util.UUID
            :environment String
            :classes {sc/Keyword {sc/Keyword (sc/maybe String)}}
            :variables {sc/Keyword sc/Any}
            :rule Rule})

(def ^:private GroupDeltaShared
  {(sc/optional-key :environment) String
   (sc/optional-key :rule) Rule
   (sc/optional-key :classes) {sc/Keyword (sc/maybe {sc/Keyword (sc/maybe String)})}
   (sc/optional-key :variables) {sc/Keyword sc/Any}})

(def GroupDelta
  (sc/either
    (assoc GroupDeltaShared :id java.util.UUID)
    (assoc GroupDeltaShared :name String)))
