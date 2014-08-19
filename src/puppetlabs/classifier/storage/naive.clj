(ns puppetlabs.classifier.storage.naive
  (:require [slingshot.slingshot :refer [throw+]]
            [puppetlabs.kitchensink.core :refer [mapvals]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [group->classification]]
            [puppetlabs.classifier.storage :as storage :refer [PrimitiveStorage root-group-uuid]]))

(defn get-ancestors
  "A naive implementation of get-ancestors using only PrimitiveStorage's
  get-group method. This will be used if the application's storage instance
  doesn't satisfy OptimizedStorage, and can also be used as a default
  get-ancestors implementation when implementing OptimizedStorage in order to
  speed up other methods."
  [storage group]
  {:pre [(satisfies? PrimitiveStorage storage)]}
  (let [get-parent #(storage/get-group storage (:parent %))]
    (loop [curr (get-parent group), ancs []]
      (cond
        (= curr (last ancs)) ancs

        (some #(= (:id curr) (:id %)) ancs)
        (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
                 :cycle (drop-while #(not= (:id curr) (:id %)) ancs)})

        :else (recur (get-parent curr) (conj ancs curr))))))

(declare get-all-classes)

(defn annotate-group
  "A naive implementation of get-ancestors using get-all-classes, which
  itself just uses PrimitiveStorage's get-classes method. This will be used if
  the application's storage instance doesn't satisfy OptimizedStorage, and can
  also be used as a default annotate-group implementation when implementing
  OptimizedStorage in order to speed up other methods."
  [storage {env :environment, :as group}]
  {:pre [(satisfies? PrimitiveStorage storage)]}
  (let [extant-classes (->> (get-all-classes storage)
                         (group-by (comp keyword :environment))
                         (mapvals #(into {} (map (juxt (comp keyword :name) identity) %))))
        missing-parameters (fn [g-ps ext-ps]
                             (into {} (for [[p v] g-ps]
                                        (if-not (contains? ext-ps p)
                                          [p {:puppetlabs.classifier/deleted true
                                              :value v}]))))
        deleted (into {} (for [[c ps] (:classes group)
                               :let [extant-class (get-in extant-classes [(keyword env) c])]]
                           (if-not extant-class
                             [c {:puppetlabs.classifier/deleted true}]
                             (let [missing (missing-parameters ps (:parameters extant-class))]
                               (if-not (empty? missing)
                                 [c (assoc missing :puppetlabs.classifier/deleted false)])))))]
    (if-not (empty? deleted)
      (assoc group :deleted deleted)
      group)))

(defn group-validation-failures
  "A naive implementation of group-validation-failures using only
  PrimitiveStorage methods. This will be used if the application's storage
  instance doesn't satisfy OptimizedStorage, and can also be used as a default
  annotate-group implementation when implementing OptimizedStorage in order to
  speed up other methods."
  [storage group]
  {:pre [(satisfies? PrimitiveStorage storage)]}
  (let [parent (storage/get-group storage (:parent group))
        _ (when (nil? parent)
            (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-parent
                     :group group}))
        ancestors (get-ancestors storage group)]
    (when (and (= (:id group) (:parent group))
               (not= (:id group) root-group-uuid))
      (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
               :cycle [group]}))
    (when (not= (:id group) root-group-uuid)
      ;; If the group's parent is being changed, the change is not yet in
      ;; the atom so get-ancestors* will not see it, meaning we have to
      ;; check ourselves for cycles involving the group.
      (when (some #(= (:id group) (:id %)) ancestors)
        (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
                 :cycle (->> ancestors
                          (take-while #(not= (:id group) (:id %)))
                          (concat [group]))})))
    (let [subtree (storage/get-subtree storage group)
          classes (get-all-classes storage)
          vtree (class8n/validation-tree subtree, classes
                                         (map group->classification ancestors))]
      (if-not (class8n/valid-tree? vtree)
        vtree))))

(defn get-all-classes
  "A naive implementation of get-all-classes using only PrimitiveStorage
  methods. This will be used if the application's storage instance doesn't
  satisfy OptimizedStorage, and can also be used as a default annotate-group
  implementation when implementing OptimizedStorage in order to speed up other
  methods."
  [storage]
  {:pre [(satisfies? PrimitiveStorage storage)]}
  (for [env (storage/get-environments storage)
        class (storage/get-classes storage (:name env))]
    class))
