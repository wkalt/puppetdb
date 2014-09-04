(ns puppetlabs.classifier.storage.memory
  (:require [clojure.set :as set]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [Environment Group group->classification
                                                  group->rule groups->tree GroupDelta Node
                                                  PuppetClass Rule]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid PrimitiveStorage]]
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

    (reify PrimitiveStorage
      (store-check-in [_ check-in]
        (swap! !storage update-in [:check-ins (:node check-in)] (fnil conj []) check-in)
        (-> @!storage
          (get-in [:check-ins (:node check-in)])
          last))
      (get-check-ins [_ node-name]
        (get-in @!storage [:check-ins node-name] []))
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
        (map (fn [env-kw] (sc/validate Environment {:name (name env-kw)}))
             (-> @!storage :classes keys)))
      (delete-environment [_ env-name]
        (let [env-kw (keyword env-name)]
          (when (empty? (get-in @!storage [:classes env-kw]))
            (swap! !storage update-in [:classes] dissoc env-kw)
            (get-in @!storage [:classes env-kw]))))

      (create-group [_ group]
        (swap! !storage assoc-in [:groups (:id group)] (sc/validate Group group))
        (get-in @!storage [:groups (:id group)]))
      (get-group [_ id]
        (get-in @!storage [:groups id]))
      (get-groups [_]
        (-> @!storage :groups vals))
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
        (->> @!storage
          :groups
          vals
          (map group->rule)
          (map (partial sc/validate Rule)))))))
