(ns puppetlabs.classifier.util
  (:require [clojure.string :as str]
            [clojure.walk :refer [postwalk prewalk]]
            [schema.utils :refer [named-error-explain validation-error-explain]]
            puppetlabs.trapperkeeper.core))

(defn ini->map
  "Given a path to an ini file, parses the file into a map, returning the map."
  [ini-file-path]
  (#'puppetlabs.trapperkeeper.core/parse-config-file ini-file-path))

(defn map->ini
  "Returns the .ini format representation of the map `m` as a string"
  [m]
  (apply str (for [[k v] m]
               (if (associative? v)
                 (str "[" (name k) "]\r\n"
                      (map->ini v))
                 (str (name k) " = " v "\r\n")))))

(defn write-map-as-ini!
  "Serialize the map `m` to a .ini file at the given path."
  [m output-path]
  (spit output-path (map->ini m)))

(defn ->client-explanation
  "Transforms a schema explanation into one that makes more sense to javascript
  client by removing any java.lang. prefixes and changing any Keyword symbols to
  String."
  [explanation]
  (let [strip-sym-prefix (fn [x]
                           (let [xstr (str x)]
                             (cond
                               (not (symbol? x)) x
                               (not (re-find #"^java\.lang\." xstr)) x
                               :else
                                 (symbol (str/replace xstr "java.lang." "")))))
        keyword->string (fn [x]
                          (if (= x 'Keyword)
                            'String
                            x))
        error-explainer (fn [x]
                          (cond
                            (= (class x) schema.utils.ValidationError)
                            (validation-error-explain x)

                            (= (class x) schema.utils.NamedError)
                            (named-error-explain x)

                            :otherwise x))]
    (postwalk (comp error-explainer keyword->string strip-sym-prefix)
              explanation)))

(defn deep-merge
  "Deeply merges maps so that nested maps are combined rather than replaced.

  For example:
  (deep-merge {:foo {:bar :baz}} {:foo {:fuzz :buzz}})
  ;;=> {:foo {:bar :baz, :fuzz :buzz}}

  ;; contrast with clojure.core/merge
  (merge {:foo {:bar :baz}} {:foo {:fuzz :buzz}})
  ;;=> {:foo {:fuzz :quzz}} ; note how last value for :foo wins"
  [& vs]
  (if (every? map? vs)
    (apply merge-with deep-merge vs)
    (last vs)))

(defn- remove-paths-to-nils
  [m]
  (let [pre-fn #(if-not (= (type %) clojure.lang.MapEntry)
                  %
                  (let [[k v :as kv] %]
                    (cond (nil? v) nil
                          (map? v) (if-let [v' (remove-paths-to-nils v)]
                                     [k v']
                                     nil)
                          :otherwise kv)))]
    (prewalk pre-fn m)))

(defn merge-and-clean
  "First deeply merges the maps with puppetlabs.classifier.util/deep-merge,
  then `cleans` the merged map by deleting any path that leads to a nil value.
  This can be used to remove paths in the first map by setting the value at that
  path to nil in a later map. If the first map has sibling values that are
  non-nil, they will still be present; if there are no siblings, there will be
  an empty map left over.

  Example:
  (unmerge {:foo {:bar :baz}} {:foo {:bar nil}, :quz :qux})
  ;;=> {:foo {}, :quz :qux}

  (unmerge {:foo {:bar :baz, :fuzz :buzz}} {:foo {:bar nil}})
  ;;=> {:foo {:fuzz :buzz}

  (unmerge {:foo {:bar :baz}} {:foo nil})
  ;;=> {}"
  [& maps]
  (->> maps
    (apply deep-merge)
    remove-paths-to-nils))
