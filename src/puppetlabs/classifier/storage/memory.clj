(ns puppetlabs.classifier.storage.memory
  (:require [clojure.set :as set]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [Environment Group group->classification
                                                  groups->tree GroupDelta Node PuppetClass Rule]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid Storage]]
            [puppetlabs.classifier.util :refer [merge-and-clean]]))

(defn in-memory-storage
  "Create an in-memory Storage protocol instance, given a map with :classes,
  :groups, and :nodes keys that map to lists of those objects."
  [objects]
  {:pre [(set/subset? (keys objects) #{:check-ins :classes :groups})]}
  (let [{:keys [check-ins classes groups]} objects
        ->class-storage-map (fn [classes]
                              (into {} (for [[env cs] (group-by :environment classes)
                                             :let [env-kw (keyword env)]]
                                         [env-kw (zipmap (map (comp keyword :name) cs) cs)])))
        !storage (atom {:groups (zipmap (map :id groups) groups)
                        :check-ins (group-by :node check-ins)
                        :classes (->class-storage-map classes)})]
    (reify Storage
      (store-check-in [_ check-in]
        (swap! !storage update-in [:check-ins (:node check-in)] (fnil conj []) check-in)
        (-> @!storage
          (get-in [:check-ins (:node check-in)])
          last))
      (get-check-ins [_ node-name]
        (get-in @!storage [:check-ins node-name]))
      (get-nodes [_]
        (for [[node-name check-ins] (get @!storage :check-ins)]
          {:name node-name
           :check-ins (map #(dissoc % :node) check-ins)}))

      (create-class [_ {env :environment, name :name :as class}]
        (let [[env-kw name-kw] (map keyword [env name])]
          (swap! !storage assoc-in [:classes env-kw name-kw] (sc/validate PuppetClass class))
          (get-in @!storage [:classes env-kw name-kw])))
      (get-class [_ env name]
        (get-in @!storage [:classes (keyword env) (keyword name)]))
      (get-classes [_ env]
        (vals (get-in @!storage [:classes (keyword env)])))
      (get-all-classes [_]
        (for [[_ env-classes] (:classes @!storage)
              [_ class] env-classes]
          class))
      (synchronize-classes [_ classes]
        (swap! !storage assoc :classes (->class-storage-map classes))
        (get @!storage :classes))
      (delete-class [_ env name]
        (let [[env-kw name-kw] (map keyword [env name])]
          (swap! !storage update-in [:classes env-kw] dissoc name-kw)
          (get-in @!storage [:classes env-kw name-kw])))

      (create-environment [_ env]
        (sc/validate Environment env)
        (swap! !storage assoc-in [:classes (-> env :name keyword)] {})
        env)
      (get-environment [_ env-name]
        (if (get-in @!storage [:classes (keyword env-name)])
          (sc/validate Environment {:name (name env-name)})))
      (get-environments [_]
        (-> @!storage :classes keys))
      (delete-environment [_ env-name]
        (let [env-kw (keyword env-name)]
          (when (empty? (get-in @!storage [:classes env-kw]))
            (swap! !storage update-in [:classes] dissoc env-kw)
            (get-in @!storage [:classes env-kw]))))

      (validate-group [this group]
        (let [parent (storage/get-group this (:parent group))
              _ (when (nil? (:parent group))
                  (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-parent
                           :group group}))
              ancestors (storage/get-ancestors this group)]
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
          (let [subtree (storage/get-subtree this group)
                classes (storage/get-all-classes this)
                vtree (class8n/validation-tree subtree, classes
                                               (map group->classification ancestors))]
            (if-not (class8n/valid-tree? vtree)
              vtree))))
      (create-group [_ group]
        (swap! !storage assoc-in [:groups (:id group)] (sc/validate Group group))
        (get-in @!storage [:groups (:id group)]))
      (get-group [_ id]
        (get-in @!storage [:groups id]))
      (get-group-as-inherited [this id]
        (if-let [group (storage/get-group this id)]
          (let [ancs (storage/get-ancestors this group)
                class8ns (map group->classification (concat [group] ancs))
                inherited (class8n/collapse-to-inherited class8ns)]
            (merge group inherited))))
      (annotate-group [_ group]
        (sc/validate Group group)
        (let [extant-classes (get-in @!storage [:classes (-> group :environment keyword)])
              missing-parameters (fn [g-ps ext-ps]
                                   (into {} (for [[p v] g-ps]
                                              (if-not (contains? ext-ps p)
                                                [p {:puppetlabs.classifier/deleted true
                                                    :value v}]))))
              deleted (into {} (for [[c ps] (:classes group)
                                     :let [extant-class (get extant-classes c)]]
                                 (if-not extant-class
                                   [c {:puppetlabs.classifier/deleted true}]
                                   (let [missing (missing-parameters ps (:parameters extant-class))]
                                     (if-not (empty? missing)
                                       [c (assoc missing :puppetlabs.classifier/deleted false)])))))]
          (if-not (empty? deleted)
            (assoc group :deleted deleted)
            group)))
      (get-groups [_]
        (-> @!storage :groups vals))
      (get-ancestors [this group]
        (let [get-parent #(storage/get-group this (:parent %))]
          (loop [curr (get-parent group), ancs []]
            (cond
              (= curr (last ancs)) ancs

              (some #(= (:id curr) (:id %)) ancs)
              (throw+ {:kind :puppetlabs.classifier/inheritance-cycle
                       :cycle (drop-while #(not= (:id curr) (:id %)) ancs)})

              :else (recur (get-parent group) (conj ancs curr))))))
      (get-subtree [this group]
        (let [get-children (fn [g]
                             (let [groups (storage/get-groups this)]
                               (->> groups
                                 (filter #(= (:parent %) (:id g)))
                                 (remove #(= (:id %) root-group-uuid)))))]
          {:group group
           :children (set (map (partial storage/get-subtree this) (get-children group)))}))
      (update-group [_ delta]
        (sc/validate GroupDelta delta)
        (swap! !storage update-in [:groups (:id delta)] merge-and-clean delta)
        (get-in @!storage [:groups (:id delta)]))
      (delete-group [_ id]
        (let [groups (:groups @!storage)
              children (->> groups
                         vals
                         (filter #(= (:parent %) id)))]
          (when-not (empty? children)
            (throw+ {:kind :puppetlabs.classifier.storage.postgres/children-present
                     :group (get groups id)
                     :children (set children)})))
        (swap! !storage update-in [:groups] dissoc id)
        (get-in @!storage [:groups id]))

      (import-hierarchy [this groups]
        (groups->tree groups) ;; for validation only; throws if hierarchy is malformed
        (swap! !storage assoc :groups (zipmap (map :id groups) groups))
        (storage/get-subtree this (storage/get-group this root-group-uuid)))

      (get-rules [_]
        (let [group->rule (fn [g] {:when (:rule g), :group-id (:id g)})]
          (->> @!storage
            :groups
            vals
            (map group->rule)
            (map (partial sc/validate Rule))))))))
