(ns puppetlabs.classifier.storage.permissioned
  (:require [clojure.set :as set]
            [fast-zip.visit :as zv]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+ try+]]
            [puppetlabs.kitchensink.core :refer [mapvals]]
            [puppetlabs.classifier.application.default :refer [matching-groups-and-ancestors]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.rules :refer [always-matches]]
            [puppetlabs.classifier.schema :refer [group->classification hierarchy-zipper
                                                  tree->groups]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid OptimizedStorage
                                                               PrimitiveStorage]]
            [puppetlabs.classifier.storage.naive :as naive]
            [puppetlabs.classifier.util :refer [map-delta merge-and-clean]]))

(def redacted-str "puppetlabs.classifier/redacted")

(defprotocol PermissionedStorage
  "This \"wraps\" the Storage protocol with permissions. It has all the same
  methods as the Storage protocol, but they all take the authentication token
  for the user as their first argument (the rest are the same as for the plain
  Storage methods)."

  (store-check-in [this token check-in] "Store a node check-in.")
  (get-check-ins [this token node-name] "Retrieve all check-ins for a node by name.")
  (get-nodes [this token] "Retrieve check-ins for all nodes.")

  (explain-classification [this token node])

  (group-validation-failures [this token group] "Performs validation of references and inherited values for the subtree of the hierarchy rooted at the group, redacting inherited values from the returned errors if the token's subject is not permitted to view the group that the values were inherited from.")
  (create-group [this token group] "Creates a new group if permitted.")
  (get-group [this token id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID if permitted.")
  (get-group-as-inherited [this token id] "Retrieves a group with all its inherited classes, class parameters, and variables, given its ID. Values inherited from groups that the subject is not permitted to view will be replaced with `puppetlabs.classifier/redacted`.")
  (annotate-group [this token group] "Returns an annotated version of the group that shows which classes and parameters are no longer present in Puppet, if permitted to access the original group.")
  (get-groups [this token] "Retrieves all groups if permitted.")
  (get-ancestors [this token group] "Retrieves the ancestors of the group, up to & including the root group, as a vector starting at the immediate parent and ending with the route, if permitted to view all of said groups.")
  (get-subtree [this token group] "Returns the subtree of the group hierarchy rooted at the passed group, if the token's subject is permitted to view the group.")
  (update-group [this token delta] "Updates fields of a group if permitted.")
  (delete-group [this token id] "Deletes a group given its ID if permitted.")

  (create-class [this token class] "Creates a class specification if permitted.")
  (get-class [this token environment-name class-name] "Retrieves a class specification if permitted.")
  (get-classes [this token environment-name] "Retrieves all class specifications in an environment, if permitted.")
  (synchronize-classes [this token puppet-classes] "Synchronize database class definitions if permitted.")
  (delete-class [this token environment-name class-name] "Deletes a class specification if permitted.")

  (get-rules [this token] "Retrieve all rules if permitted.")

  (create-environment [this token environment] "Creates an environment if permitted.")
  (get-environment [this token environment-name] "Retrieves an environment if permitted.")
  (get-environments [this token] "Retrieves all environments if permitted.")
  (delete-environment [this token environment-name] "Deletes an environment if permitted."))

(def Permissions
  "A map that provides the requisite functions for creating
  a PermissionedStorage instance. These functions are mostly permission
  predicates, which take an RBAC API token and, if applicable, a group id and
  list of the group's ancestors' ids, and return a boolean indicating whether
  the subject owning the token has the given permission. The non-predicates are
  :permitted-group-actions, which takes a token, a group id, and a list of the
  group's ancestors' ids, and returns all the actions that the subject owning
  the token has permissions for on the given group  and :viewable-group-ids,
  which takes a token and returns all the ids of groups that the subject is
  allowed to view."
  (let [Function (sc/pred fn?)]
    {:all-group-access? Function
     :group-edit-classification? Function
     :group-edit-environment? Function
     :group-edit-child-rules? Function
     :group-modify-children? Function
     :group-view? Function
     :permitted-group-actions Function
     :viewable-group-ids Function}))

(def permission->description
  "Human-readable descriptions for the action that each permission key in
  a Permissions map represents, mainly to embed in error messages."
  {:all-group-access? "access all groups in the classifier"
   :group-modify-children? "create or delete children"
   :group-edit-classification? "edit the classes and variables"
   :group-edit-environment? "change the environment"
   :group-edit-child-rules? "change the rules"
   :group-view? "view"})

(defn- permission-exception
  "Returns a slingshot exception for a permission denial. `permission` must be
  one of the keys allowed in a Permissions map."
  ([permission token] (permission-exception permission token nil))
  ([permission token group-id]
   {:pre [(contains? Permissions permission)]}
   (let [guaranteed-info {:kind ::permission-denied
                          :permission-to permission
                          :description (permission->description permission)
                          :rbac-token token}
         exc-info (if group-id
                    (assoc guaranteed-info :group-id group-id)
                    guaranteed-info)]
     (throw+ exc-info))))

(defn- split-ancestors-by-viewability
  "Given a group's ancestors (starting at the parent and ending with root) and
  a set of ids that some subject is permitted to view, return a vector
  containing two sequences of groups, the first being the groups that the
  subject is not allowed to view, and the second being those groups that the
  subject can view. One of the sequences may be empty."
  [ancestors viewable-ids]
  (->> ancestors
    reverse
    (split-with #(not (viewable-ids (:id %))))
    (map reverse)))

(defn- redact-inheritable-values
  [{:keys [classes variables] :as group}]
  (assoc group
         :classes (into {} (for [[class-kw params] classes]
                             [class-kw (into {} (for [[param _] params]
                                                  [param redacted-str]))]))
         :variables (into {} (for [[variable _] variables]
                               [variable redacted-str]))))

(defn- redact-invisible-ancestors
  [ancestors viewable-ids]
  (let [[invis vis] (split-ancestors-by-viewability ancestors viewable-ids)]
    (concat vis (map redact-inheritable-values invis))))

(defn- redact-group
  [group]
  (merge group
         (redact-inheritable-values group)
         {:name redacted-str
          :environment redacted-str
          :description redacted-str}))

(defn- redact-group-if-needed
  [group id-viewable?]
  (if (id-viewable? (:id group))
    group
    (redact-group group)))

(defn- redact-tagged-conflict-val-if-needed
  [{:keys [defined-by from value] :as tagged-val} id-viewable?]
  {:value (if (id-viewable? (:id defined-by)) value redacted-str)
   :defined-by (redact-group-if-needed defined-by id-viewable?)
   :from (redact-group-if-needed from id-viewable?)})

(defn- inherited-with-redaction
  [groups viewable-ids]
  (->> (redact-invisible-ancestors groups viewable-ids)
    (map group->classification)
    class8n/collapse-to-inherited
    (merge (first groups))))

(defn- viewable-subtrees
  [root-node viewable-ids]
  (let [zip-root (hierarchy-zipper root-node)
        viewable-visitor (zv/visitor
                           :pre [node subtrees]
                            (if (viewable-ids (-> node :group :id))
                             {:state (conj subtrees node), :cut true}))]
    (:state (zv/visit zip-root [] [viewable-visitor]))))

(defn- check-update-permissions
  "Throw a permission exception if the changes made by the update are not
  allowed by the RBAC permissions for token's subject."
  [update-changes token id parent'-id parent-id ancs' ancs permission-fns]
  (let [{:keys [group-modify-children? permitted-group-actions]} permission-fns
        permitted-actions' (permitted-group-actions token id ancs')
        parent'-permitted-actions (permitted-group-actions token parent'-id (rest ancs'))]

    (when-not (permitted-actions' :view)
      (throw+ (permission-exception :group-view? token id)))

    (when (contains? update-changes :parent)
      (let [modify-parents-children? (group-modify-children? token parent-id (rest ancs))]
        (when-not modify-parents-children?
          (throw+ (permission-exception :group-modify-children? token parent-id)))
        (when-not (parent'-permitted-actions :modify-children)
          (throw+ (permission-exception :group-modify-children? token parent'-id)))))

    (when (and (contains? update-changes :rule)
               (not (or (parent'-permitted-actions :edit-child-rules)
                        (parent'-permitted-actions :modify-children))))
      (throw+ (permission-exception :group-edit-child-rules? token id)))

    (when (and (contains? update-changes :environment)
               (not (permitted-actions' :edit-environment)))
      (throw+ (permission-exception :group-edit-environment? token id)))

    (when (and (some #(contains? update-changes %) [:name :classes :variables])
               (not (or (permitted-actions' :edit-classification)
                        (parent'-permitted-actions :modify-children))))
      (throw+ (permission-exception :group-edit-classification? token id)))))

(sc/defn storage-with-permissions :- (sc/protocol PermissionedStorage)
  [storage :- (sc/protocol PrimitiveStorage), permissions :- Permissions]
  (let [{:keys [all-group-access?
                group-modify-children?
                group-edit-classification?
                group-edit-environment?
                group-edit-child-rules?
                group-view?
                permitted-group-actions
                viewable-group-ids]} permissions
        wrap-all-group-access (fn [f]
                                (fn [this token & args]
                                  (if (all-group-access? token)
                                    (apply f storage args)
                                    (throw+ (permission-exception :all-group-access? token)))))
        wrap-always-allow (fn [f]
                            (fn [this _ & args]
                              (apply f storage args)))
        get-ancestors (if (satisfies? OptimizedStorage storage)
                        storage/get-ancestors
                        naive/get-ancestors)
        get-group-ids (if (satisfies? OptimizedStorage storage)
                        storage/get-group-ids
                        naive/get-group-ids)
        group-validation-failures (if (satisfies? OptimizedStorage storage)
                                    storage/group-validation-failures
                                    naive/group-validation-failures)]

    (reify PermissionedStorage

      ;;
      ;; Group Storage methods
      ;;
      ;; The group methods & explain-classification are the only interesting
      ;; ones; the rest just depend on whether the token's subject has
      ;; classifier access at all.
      ;;

      (group-validation-failures [this token {:keys [id parent] :as group}]
        (let [ancs (get-ancestors storage group)]
          (if-let [group (storage/get-group storage id)]
            (if-not (group-view? token id ancs)
              (throw+ (permission-exception :group-view? token id))
              (group-validation-failures storage group))
            ;; else (group doesn't exist)
            (if-not (group-modify-children? token parent (rest ancs))
              (throw+ (permission-exception :group-modify-children? token parent))
              (group-validation-failures storage group)))))

      (create-group [this token {id :id, parent-id :parent, :as group}]
        (let [ancs (get-ancestors storage group)
              actions (permitted-group-actions token id ancs)
              parent-actions (permitted-group-actions token parent-id (rest ancs))
              parent (storage/get-group storage parent-id)]
          (when-not (contains? parent-actions :modify-children)
            (throw+ (permission-exception :group-modify-children? token parent-id)))
          (when-let [vtree (group-validation-failures storage group)]
            (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-referents
                     :tree vtree
                     :ancestors ancs}))
          (when (and (not (contains? actions :edit-environment))
                     (not= (:environment group) (:environment parent)))
            (throw+ (assoc (permission-exception :group-edit-environment? token id)
                           :description (str "create this group with a different environment"
                                             " than its parent's environment of "
                                             (pr-str (:environment parent)) " because you"
                                             " can't edit the parent's environment"))))
          (storage/create-group storage group)))

      (get-group [this token id]
        (let [group (storage/get-group storage id)
              ancs (get-ancestors storage group)]
          (if-not (group-view? token id ancs)
            (throw+ (permission-exception :group-view? token id))
            group)))

      (get-group-as-inherited [this token id]
        (let [group (storage/get-group storage id)
              ancs (get-ancestors storage group)]
          (if-not (group-view? token id ancs)
            (throw+ (permission-exception :group-view? token id))
            (let [viewable-ids (viewable-group-ids token (get-group-ids storage))]
              (inherited-with-redaction (concat [group] ancs) viewable-ids)))))

      (get-groups [this token]
        (let [viewable-ids (viewable-group-ids token (get-group-ids storage))
              root (storage/get-group storage root-group-uuid)
              root-node (storage/get-subtree storage root)
              subtrees (viewable-subtrees (storage/get-subtree storage root) viewable-ids)]
          (for [subtree subtrees, group (tree->groups subtree)]
            group)))

      (get-ancestors [this token group]
        (let [viewable-ids (viewable-group-ids token (get-group-ids storage))
              ancs (get-ancestors storage group)
              [_ vis] (split-ancestors-by-viewability ancs viewable-ids)]
          vis))

      (get-subtree [this token {id :id :as group}]
        (let [ancs (get-ancestors storage group)]
          (if-not (group-view? token id ancs)
            (throw+ (permission-exception :group-view? token id))
            (storage/get-subtree storage group))))

      (update-group [this token {id :id :as delta}]
        (let [group (storage/get-group storage id)
              group' (merge-and-clean group delta)
              only-changes (map-delta group group')
              ancs' (get-ancestors storage group')
              ancs (if (contains? only-changes :parent)
                     (get-ancestors storage group)
                     ancs')]
          (check-update-permissions
            only-changes token id (:parent group') (:parent group) ancs' ancs permissions)
          (try+
            (storage/update-group storage delta)
            (catch [:kind :puppetlabs.classifier.storage.postgres/missing-referents]
              {:keys [tree ancestors]}
              (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-referents
                       :tree tree
                       :ancestors (redact-invisible-ancestors
                                    ancestors
                                    (viewable-group-ids token (get-group-ids storage)))})))))

      (delete-group [this token id]
        (if-let [{:keys [parent] :as group} (storage/get-group storage id)]
          (let [ancs (get-ancestors storage group)]
            (if (group-modify-children? token parent (rest ancs))
              (storage/delete-group storage id)
              (throw+ (permission-exception :group-modify-children? token parent))))))

      ;;
      ;; Classification Explanation Scrubbing
      ;;

      (explain-classification [_ token node]
        (let [viewable-ids (viewable-group-ids token (get-group-ids storage))
              matching-group->ancestors (matching-groups-and-ancestors storage node)
              class8n-info (class8n/classification-steps node matching-group->ancestors)
              {:keys [conflicts classification id->leaf leaf-id->classification]} class8n-info
              id->ancestor-ids (fn [id]
                                 (->> (matching-group->ancestors (id->leaf id))
                                   (map :id)))
              id->redacted-leaf (mapvals (fn [{:keys [id] :as g}]
                                           (let [ids (-> (id->ancestor-ids id)
                                                       (conj id), set)]
                                             (if (empty? (filter viewable-ids ids))
                                               (redact-group g)
                                               g)))
                                         id->leaf)
              redacted-class8n (fn [id]
                                 (let [leaf (id->leaf id)
                                       ancs (matching-group->ancestors leaf)
                                       groups (concat [leaf] ancs)]
                                   (-> (inherited-with-redaction groups viewable-ids)
                                     group->classification)))
              leaf-id->redacted-class8n (let [ids (keys id->leaf)]
                                          (zipmap ids (map redacted-class8n ids)))
              redacted-conflict-val (fn [{:keys [value from defined-by] :as tagged-val}]
                                      (if (viewable-ids (:id defined-by))
                                        tagged-val
                                        (assoc tagged-val :value redacted-str)))
              explained-conflicts (if conflicts
                                    (let [l->as (into {} (for [[g as] matching-group->ancestors
                                                               :when (contains? id->leaf (:id g))]
                                                           [g as]))]
                                      (class8n/explain-conflicts conflicts l->as)))
              redact-tagged-conflict-val #(redact-tagged-conflict-val-if-needed % viewable-ids)
              r-cs (merge (if-let [conflict-deets (:environment explained-conflicts)]
                            {:environment (set (map redact-tagged-conflict-val conflict-deets))})
                          (if-let [vars (:variables explained-conflicts)]
                            {:variables
                             (into {} (for [[vr conflict-deets] vars]
                                        [vr (set (map redact-tagged-conflict-val
                                                      conflict-deets))]))})
                          (if-let [classes (:classes explained-conflicts)]
                            {:classes
                             (into {}
                                   (for [[c params] classes]
                                     [c (into {} (for [[p conflict-deets] params]
                                                   [p (set (map redact-tagged-conflict-val
                                                                conflict-deets))]))]))}))
              partial-resp {:node-as-received node
                            :match-explanations (:match-explanations class8n-info)
                            :leaf-groups id->redacted-leaf
                            :inherited-classifications leaf-id->redacted-class8n}]
          (if (seq conflicts)
            (assoc partial-resp :conflicts r-cs)
            (assoc partial-resp
                   :final-classification (class8n/merge-classifications
                                           (vals leaf-id->redacted-class8n))))))

      ;;
      ;; Non-Group Storage methods
      ;;
      ;; These all just depend on whether the token's owner has any permissions
      ;; in RBAC for NC at all.
      ;;

      (store-check-in [this token check-in]
        ((wrap-always-allow storage/store-check-in) this token check-in))
      (get-check-ins [this token node-name]
        ((wrap-all-group-access storage/get-check-ins) this token node-name))
      (get-nodes [this token]
        ((wrap-all-group-access storage/get-nodes) this token))

      (create-class [this token class]
        ((wrap-always-allow storage/create-class) this token class))
      (get-class [this token environment-name class-name]
        ((wrap-always-allow storage/get-class) this token environment-name class-name))
      (get-classes [this token environment-name]
        ((wrap-always-allow storage/get-classes) this token environment-name))
      (synchronize-classes [this token puppet-classes]
        ((wrap-always-allow storage/synchronize-classes) this token puppet-classes))
      (delete-class [this token environment-name class-name]
        ((wrap-always-allow storage/delete-class) this token environment-name class-name))

      (get-rules [this token] ((wrap-always-allow storage/get-rules) this token))

      (create-environment [this token environment]
        ((wrap-always-allow storage/create-environment) this token environment))
      (get-environment [this token environment-name]
        ((wrap-always-allow storage/get-environment) this token environment-name))
      (get-environments [this token]
        ((wrap-always-allow storage/get-environments) this token))
      (delete-environment [this token environment-name]
        ((wrap-always-allow storage/delete-environment) this token environment-name)))))
