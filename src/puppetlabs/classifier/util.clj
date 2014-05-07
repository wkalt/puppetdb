(ns puppetlabs.classifier.util
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk prewalk]]
            [schema.utils :refer [named-error-explain validation-error-explain]]
            [puppetlabs.kitchensink.core :refer [deep-merge]])
  (:import java.util.UUID))

(defn- has-schema-error-classes?
  [x]
  (let [!has? (atom false)
        trip-on-schema-error (fn [x]
                               (let [c (class x)]
                                 (when (or (= c schema.utils.ValidationError)
                                         (= c schema.utils.NamedError))
                                   (reset! !has? true))
                                 x))]
    (prewalk trip-on-schema-error x)
    @!has?))

(defn explain-schema-errors
  [x]
  (let [explainer (fn [x]
                    (condp = (class x)
                      schema.utils.ValidationError (validation-error-explain x)
                      schema.utils.NamedError (named-error-explain x)
                      x))
        explained (postwalk explainer x)]
    (if (has-schema-error-classes? explained)
      (recur explained)
      explained)))

(defn ->client-explanation
  "Transforms a schema explanation into one that makes more sense to javascript
  client by removing any java.lang. prefixes and changing any Keyword symbols to
  String."
  [explanation]
  (let [explained (explain-schema-errors explanation)
        strip-sym-prefix (fn [x]
                           (let [xstr (str x)]
                             (cond
                               (not (symbol? x)) x
                               (not (re-find #"^java\.lang\." xstr)) x
                               :else
                                 (symbol (str/replace xstr "java.lang." "")))))
        keyword->string (fn [x]
                          (if (= x 'Keyword)
                            'String
                            x))]
    (postwalk (comp keyword->string strip-sym-prefix)
              explained)))

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

(defn flatten-tree-with
  "Flattens a HierarchyNode tree or ValidationNode tree by applying f to each
  group and returning a sequence of the results."
  [f tree]
  (let [{:keys [children group]} tree]
    (conj (mapcat (partial flatten-tree-with f) children)
          (f group))))

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
