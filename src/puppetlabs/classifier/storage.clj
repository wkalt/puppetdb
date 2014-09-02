(ns puppetlabs.classifier.storage)

(def root-group-uuid (java.util.UUID/fromString "00000000-0000-4000-8000-000000000000"))

(defprotocol PrimitiveStorage
  (store-check-in [this check-in] "Store a node check-in.")
  (get-check-ins [this node-name] "Retrieve all check-ins for a node by name.")
  (get-nodes [this] "Retrieve check-ins for all nodes.")

  (create-group [this group] "Creates a new group")
  (get-group [this id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID")
  (get-groups [this] "Retrieves all groups")
  (get-subtree [this group] "Returns the subtree of the group hierarchy rooted at the passed group.")
  (update-group [this delta] "Updates class/parameter and variable fields of a group")
  (delete-group [this id] "Deletes a group given its ID")

  (import-hierarchy [this groups] "Batch import a hierarchy given a flat collection of its groups. Any missing classes & class parameters will be created as needed.")

  (create-class [this class] "Creates a class specification")
  (get-class [this environment-name class-name] "Retrieves a class specification")
  (get-classes [this environment-name] "Retrieves all class specifications in an environment")
  (synchronize-classes [this puppet-classes] "Synchronize database class definitions")
  (get-last-sync [this] "Retrieve the time that classes were last synchronized with puppet")
  (delete-class [this environment-name class-name] "Deletes a class specification")

  (get-rules [this] "Retrieve all rules")

  (create-environment [this environment] "Creates an environment")
  (get-environment [this environment-name] "Retrieves an environment")
  (get-environments [this] "Retrieves all environments")
  (delete-environment [this environment-name] "Deletes an environment"))

(defn get-groups-ancestors
  "Retrieves the ancestors of the given groups up through the root. Returns
  a map from the groups in `groups` to a vector of their ancestors."
  [storage groups]
  {:pre [(satisfies? PrimitiveStorage storage)]}
  (let [id->g (loop [ids-to-fetch (map :parent groups)
                     id->g (into {} (map (juxt :id identity) groups))]
                (if-let [id (first ids-to-fetch)]
                  (if (contains? id->g id)
                    (recur (next ids-to-fetch) id->g)
                    (let [{:keys [parent] :as group} (get-group storage id)]
                      (recur (-> ids-to-fetch
                               rest
                               (conj parent))
                             (assoc id->g id group))))
                  ;; else (no more ids to fetch)
                  id->g))
        get-ancs (fn [{parent-id :parent :as g} ancs]
                   (if (= parent-id (:id g))
                     ancs
                     (let [parent (get id->g parent-id)]
                       (recur parent (conj ancs parent)))))]
    (into {} (map (juxt identity #(get-ancs % [])) groups))))

(defprotocol OptimizedStorage
  (get-group-ids [this] "Returns a sequential collection containing the id of every group.")
  (get-ancestors [this group] "Retrieves the ancestors of the group, up to & including the root group, as a vector starting at the immediate parent and ending with the route.")
  (annotate-group [this group] "Returns an annotated version of the group that shows which classes and parameters are no longer present in Puppet.")
  (group-validation-failures [this group] "Performs validation of references and inherited values for the subtree of the hierarchy rooted at the group. If there are validation failures, returns a ValidationNode corresponding to the group which describes the missing references. If no failures, returns nil.")

  (get-all-classes [this] "Retrieves all class specifications across all environments"))
