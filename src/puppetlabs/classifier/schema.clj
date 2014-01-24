(ns puppetlabs.classifier.schema
  (:require [schema.core :as sc]))

(def Node {:name String})

(def Group {:name String
            :classes {sc/Keyword {sc/Keyword (sc/maybe String)}}
            :environment String
            :variables {sc/Keyword sc/Any}})

(def puppetlabs.classifier.schema/Class
  {:name String
   :parameters {sc/Keyword (sc/maybe String)}
   :environment String})

(def Rule
  {:when [String]
   :groups [String]
   (sc/optional-key :id) Number})

(def Environment {:name String})
