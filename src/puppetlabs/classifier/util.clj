(ns puppetlabs.classifier.util
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk prewalk]]
            [cheshire.core :as json]
            [schema.utils :refer [named-error-explain validation-error-explain]]
            [puppetlabs.kitchensink.core :refer [deep-merge]])
  (:import java.util.UUID))

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

(defn map-delta
  "Returns a delta that, when applied to `m` using merge-and-clean, produces `n`."
  [m n]
  (let [m-ks (-> m keys set)
        n-ks (-> n keys set)]
    (merge
      ;; removed keys
      (into {} (for [k (set/difference m-ks n-ks)]
                 [k nil]))
      ;; added keys
      (into {} (for [k (set/difference n-ks m-ks)]
                 [k (get n k)]))
      ;; changed keys
      (into {} (for [k (set/intersection m-ks n-ks)
                     :let [mv (get m k)
                           nv (get n k)]
                     :when (not= mv nv)]
                 (if (and (map? mv) (map? nv))
                   [k (map-delta mv nv)]
                   [k nv]))))))

(defn ->uuid
  "Attempts to convert `x` to a java.util.UUID, returning nil if the conversion
  fails."
  [x]
  (cond
    (instance? UUID x) x
    (keyword? x) (recur (name x))
    (string? x) (try (UUID/fromString x) (catch IllegalArgumentException _ nil))
    :otherwise nil))

(def uuid? (comp boolean ->uuid))

(defn relative-complements-by-key
  "Given two sorted sequences and a key function, give each of the relative
  complements as well as a vector of pairs that compared equally [a not in bs,
  b not in as, [in both by keyfunc]] by comparing with the given key function."
  [keyfunc as bs]
  (loop [as-only [], bs-only [], both [], as as, bs bs]
    (cond
      (and (empty? as) (empty? bs))
      [as-only bs-only both]

      (empty? as)
      [as-only (concat bs-only bs) both]

      (empty? bs)
      [(concat as-only as) bs-only both]

      :else
      (let [[a & rest-as] as
            [b & rest-bs] bs
            diff (compare (keyfunc a) (keyfunc b))]
        (cond
          ;; a < b, therefore a is not in bs
          (< diff 0) (recur (conj as-only a) bs-only both rest-as bs)
          ;; b < a, therefore b is not in as
          (> diff 0) (recur as-only (conj bs-only b) both as rest-bs)
          ;; a == b, add the pair
          (= diff 0) (recur as-only bs-only (conj both [a b]) rest-as rest-bs))))))

(defn dissoc-nil
  "Dissoc a key from a map only if the value is nil"
  [map key]
  (if (nil? (key map))
    (dissoc map key)
    map))

(defn json-key->clj-key
  [k]
  (-> k
    (str/replace \_ \-)
    keyword))

(defn clj-key->json-key
  [k]
  (-> k
    str
    (.substring 1)
    (str/replace \- \_)))

(defn encode
  "Encode clojure data to json with clj-key->json-key as the key-fn."
  [data]
  (json/encode data {:key-fn clj-key->json-key}))

(defn to-sentence
  "Join the given strings as they would be listed in a sentence (using an Oxford
  comma if there are three or more strings).
  Examples:
    [\"apple\"] => \"apple\"
    [\"apple\" \"orange\"] => \"apple and orange\"
    [\"apple\" \"orange\" \"banana\"] => \"apple, orange, and banana\""
  [strings]
  (let [num-strings (count strings)]
    (cond
      (empty? strings)
      ""

      (= num-strings 1)
      (first strings)

      (= num-strings 2)
      (str (first strings) " and " (second strings))

      (= num-strings 3)
      (let [[s0 s1 s2] strings]
        (str s0 ", " s1 ", and " s2))

      (> num-strings 3)
      (str (first strings) ", " (to-sentence (rest strings))))))