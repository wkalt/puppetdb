(ns puppetlabs.classifier.schema
  (:require [schema.core :as sc]))

(def Node {(sc/required-key :name) String})

(def Group {(sc/required-key :name) String
            (sc/required-key :classes) {sc/Keyword
                                        {sc/Keyword (sc/maybe String)}}
            (sc/required-key :environment) String
            (sc/required-key :variables) {sc/Keyword sc/Any}})

(def puppetlabs.classifier.schema/Class
  {(sc/required-key :name) String
   (sc/required-key :parameters) {sc/Keyword (sc/maybe String)}
   (sc/required-key :environment) String})

(def Rule
  {(sc/required-key :when) [String]
   (sc/required-key :groups) [String]
   (sc/optional-key :id) Number})

(def Environment {(sc/required-key :name) String})
