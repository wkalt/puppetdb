(ns puppetlabs.classifier.schema
  (:require [schema.core :as sc]))

(def Node {(sc/required-key :name) String})

(def Group {(sc/required-key :name) String
            (sc/required-key :classes) [String]})

(def puppetlabs.classifier.schema/Class
  {(sc/required-key :name) String
   (sc/required-key :parameters) {sc/Keyword String}})

(def Rule
  {(sc/required-key :when) [String]
   (sc/required-key :groups) [String]})
