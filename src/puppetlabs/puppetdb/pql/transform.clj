(ns puppetlabs.puppetdb.pql.transform
  (:require [clojure.string :as str]
            [puppetlabs.puppetdb.cheshire :as json]))

(defn transform-statement
  [statement]
  (rest statement))

(defn transform-from
  [entity statement]
  (println "transformed" (vec (concat ["from" entity] (transform-statement statement))))
  (vec (concat ["from" entity] (transform-statement statement))))

(defn transform-subquery
  ([entity]
   ["subquery" entity])
  ([entity arg2]
   ["subquery" entity arg2]))

(defn transform-extract
  [& args]
  ["extract" (vec args)])

(defn transform-expr-or
  ;; Single arg? collapse
  ([data] data)
  ;; Multiple args? turn it into an or statement
  ([data & args] (vec (concat ["or" data] args))))

(defn transform-expr-and
  ;; Single arg? collapse
  ([data] data)
  ;; Multiple args? turn it into an and statement
  ([data & args] (vec (concat ["and" data] args))))

(defn transform-expr-not
  ;; Single arg? Just collapse the :expr-not and pass back the data,
  ;; closing the nesting.
  ([data] data)
  ;; Two args? This means :not [data] so convert it into a "not"
  ([_ data] ["not" data]))

(defn transform-function
  [entity args]
  (vec (concat ["function" entity] args)))

(defn transform-condexpression
  [a b c]
  [b a c])

(defn transform-condexpnull
  [entity type]
  ["null?" entity
   (case (first type)
     :condisnull true
     :condisnotnull false)])

(defn transform-groupedlist
  [& args]
  args)

(defn transform-groupedliterallist
  [& args]
  ["array" args])

(defn transform-sqstring
  [s]
  ;; Un-escape any escaped single quotes
  (str/replace s #"\\'" "'"))

(defn transform-dqstring
  [s]
  ;; For now we just parse the contents using the JSON decoder
  (json/parse-string (str "\"" s "\"")))

(defn transform-boolean
  [bool]
  (case (first bool)
    :true true
    :false false))

(defn transform-integer
  ([int]
   (Integer. int))
  ([neg int]
   (- (Integer. int))))

(defn transform-real
  [& args]
  (Double. (apply str args)))

(defn transform-exp
  ([int]
   (str "E" int))
  ([mod int]
   (str "E" mod int)))

(defn transform-groupby
  [& args]
  (vec (concat ["group_by"] args)))

(defn transform-limit
  [arg]
  ["limit" arg])

(defn transform-offset
  [arg]
  ["offset" arg])

(defn transform-orderby
  [arg]
  (println "arg is" arg)
  ["order_by"
   (if (= 2 (count arg))
     [(second arg)]
     (rest arg))])

(def transform-specification
  {:from               transform-from
   :subquery           transform-subquery
   :extract            transform-extract
   :expr-or            transform-expr-or
   :expr-and           transform-expr-and
   :expr-not           transform-expr-not
   :function           transform-function
   :condexpression     transform-condexpression
   :condexpnull        transform-condexpnull
   :groupedarglist     transform-groupedlist
   :groupedfieldlist   transform-groupedlist
   :groupedregexplist  transform-groupedlist
   :groupedliterallist transform-groupedliterallist
   :sqstring           transform-sqstring
   :dqstring           transform-dqstring
   :boolean            transform-boolean
   :integer            transform-integer
   :real               transform-real
   :exp                transform-exp
   :groupby            transform-groupby
   :limit              transform-limit
   :offset             transform-offset
   :orderby            transform-orderby})
