(ns puppetlabs.classifier.application.default
  (:require [clj-time.core :as time]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.application :refer [Classifier]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [group->classification groups->tree]]
            [puppetlabs.classifier.storage :as storage :refer [get-groups-ancestors OptimizedStorage
                                                               PrimitiveStorage root-group-uuid]]
            [puppetlabs.classifier.storage.naive :as naive]))

(defn matching-groups-and-ancestors
  "Given a storage instance and a node, returns a map from each group that the
  node matched to that group's ancestors."
  [storage node]
  (let [maybe-matching-ids (set (class8n/matching-groups node (storage/get-rules storage)))
        maybe-matching-groups (map (partial storage/get-group storage) maybe-matching-ids)
        mmg->ancs (get-groups-ancestors storage maybe-matching-groups)
        w-full-rules (for [[mmg ancs] mmg->ancs]
                       (assoc mmg :full-rule (class8n/inherited-rule (concat [mmg] ancs))))
        ->rule (fn [g] {:when (:full-rule g), :group-id (:id g)})
        matching-groups (->> w-full-rules
                          (filter #(rules/apply-rule node (->rule %)))
                          (map #(dissoc % :full-rule)))]
    (into {} (map (juxt identity mmg->ancs) matching-groups))))

(defn default-application
  [{storage :db, :as config}]
  {:pre [(satisfies? PrimitiveStorage storage)]}
  (let [optimized? (satisfies? OptimizedStorage storage)
        annotate (if optimized?
                   #(storage/annotate-group storage %)
                   #(naive/annotate-group storage %))]

    (reify Classifier
      (get-config [_] config)
      (get-storage [_] storage)

      (classify-node [_ node transaction-uuid]
        (let [check-in-time (time/now)
              matching-group->ancestors (matching-groups-and-ancestors storage node)
              class8n-info (class8n/classification-steps node matching-group->ancestors)
              {:keys [conflicts classification match-explanations id->leaf]} class8n-info
              leaves (vals id->leaf)
              check-in {:node (:name node)
                        :time check-in-time
                        :explanation match-explanations}
              check-in (if transaction-uuid
                         (assoc check-in :transaction-uuid transaction-uuid)
                         check-in)]
          (storage/store-check-in storage (if (nil? conflicts)
                                            (assoc check-in :classification classification)
                                            check-in))
          (if-not (nil? conflicts)
            (throw+
              {:kind :puppetlabs.classifier.http/classification-conflict
               :group->ancestors (into {} (map (juxt identity matching-group->ancestors) leaves))
               :conflicts conflicts})
            {:name (:name node)
             :environment (:environment classification)
             :groups (keys match-explanations)
             :classes (:classes classification {})
             :parameters (:variables classification {})})))

      (explain-classification [_ node]
        (let [matching-group->ancestors (matching-groups-and-ancestors storage node)
              class8n-info (class8n/classification-steps node matching-group->ancestors)
              {:keys [conflicts classification]} class8n-info
              partial-resp {:node-as-received node
                            :match-explanations (:match-explanations class8n-info)
                            :leaf-groups (:id->leaf class8n-info)
                            :inherited-classifications (:leaf-id->classification class8n-info)}]
          (if (nil? conflicts)
            (assoc partial-resp :final-classification classification)
            (assoc partial-resp
                   :conflicts (class8n/explain-conflicts conflicts matching-group->ancestors)))))

      (get-check-ins [_ node-name] (storage/get-check-ins storage node-name))
      (get-nodes [_] (storage/get-nodes storage))

      (validate-group [_ group]
        (if-let [vtree (if optimized?
                         (storage/group-validation-failures storage group)
                         (naive/group-validation-failures storage group))]
          (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-referents
                   :tree vtree
                   :ancestors (if optimized?
                                (storage/get-ancestors storage group)
                                (naive/get-ancestors storage group))})
          (annotate group)))

      (update-group [_ delta]
        (when (and (= (:id delta) root-group-uuid)
                   (contains? delta :rule))
          (throw+ {:kind :puppetlabs.classifier.storage/root-rule-edit
                   :delta delta}))
        (-> (storage/update-group storage delta) annotate))

      (get-group [_ id]
        (let [g (storage/get-group storage id)]
          (if g
            (annotate g))))

      (get-group-as-inherited [_ id]
        (if-let [group (storage/get-group storage id)]
          (let [ancs (if optimized?
                       (storage/get-ancestors storage group)
                       (naive/get-ancestors storage group))
                chain (concat [group] ancs)
                inherited (class8n/collapse-to-inherited (map group->classification chain))
                inherited-rule (class8n/inherited-rule chain)
                w-inherited (-> (merge group inherited)
                              annotate)]
            (if (contains? group :rule)
              (assoc w-inherited :rule inherited-rule)
              (dissoc w-inherited :rule)))))

      (get-groups [_]
        (let [groups (storage/get-groups storage)]
            (map annotate groups)))

      (create-group [_ group] (storage/create-group storage group))
      (delete-group [_ id] (storage/delete-group storage id))

      (import-hierarchy [_ groups] (storage/import-hierarchy storage groups))

      (get-all-classes [_]
        (if optimized?
          (storage/get-all-classes storage)
          (naive/get-all-classes storage)))

      (create-class [_ class] (storage/create-class storage class))
      (get-class [_ env-name class-name] (storage/get-class storage env-name class-name))
      (get-classes [_ env-name] (storage/get-classes storage env-name))
      (synchronize-classes [_ pcs] (storage/synchronize-classes storage pcs))
      (get-last-sync [_] (storage/get-last-sync storage))
      (delete-class [_ env-name class-name] (storage/delete-class storage env-name class-name))

      (create-environment [_ env] (storage/create-environment storage env))
      (get-environment [_ env-name] (storage/get-environment storage env-name))
      (get-environments [_] (storage/get-environments storage))
      (delete-environment [_ env-name] (storage/delete-environment storage env-name)))))
