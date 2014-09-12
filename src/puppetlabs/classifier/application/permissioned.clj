(ns puppetlabs.classifier.application.permissioned
  (:require [clojure.set :as set]
            [fast-zip.visit :as zv]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+ try+]]
            [puppetlabs.kitchensink.core :refer [mapvals]]
            [puppetlabs.classifier.application :as app :refer [Classifier]]
            [puppetlabs.classifier.application.default :refer [matching-groups-and-ancestors]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [group->classification hierarchy-zipper
                                                  tree->groups]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid OptimizedStorage
                                                               PrimitiveStorage]]
            [puppetlabs.classifier.storage.naive :as naive]
            [puppetlabs.classifier.util :refer [map-delta merge-and-clean]]))

;;
;; Permission-related Protocols & Schemas
;;

(defprotocol PermissionedClassifier
  (get-config [this] "Returns a map of the configuration settings for the application.")
  (get-storage [this] "Returns the storage implementation used by the application.")

  (classify-node [this token node transaction-uuid] "Returns the node's classification as a map conforming to the Classification schema, provided that there are no conflicts encountered during classification-time. If conflicts are encountered, an exception is thrown.")
  (explain-classification [this token node] "Returns an explanation of the node's classification as returned by the `puppetlabs.classifier.classification/classification-step` function.")

  (get-check-ins [this token node-name] "Returns the check-in history (with classification) for the node with the given name.")
  (get-nodes [this token] "Returns all nodes, each of which contains its own check-in history.")

  (validate-group [this token group] "Validates the parent link, references, and inherited references for the given group and all of its descendents (if any). If the group would invalidate the group hierarchy's structure or introduce missing referents, a slingshot exception is thrown.")
  (create-group [this token group] "Creates a new group")
  (get-group [this token id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID")
  (get-group-as-inherited [this token id] "Retrieves a group with all its inherited classes, class parameters, and variables, given its ID")
  (get-groups [this token] "Retrieves all groups")
  (update-group [this token delta] "Edits any attribute of a group besides ID, subject to validation for illegal edits, malformed hierarchy structure, and missing references.")
  (delete-group [this token id] "Deletes a group given its ID")

  (import-hierarchy [this token groups] "Batch import a hierarchy given a flat collection of its groups. Any missing classes & class parameters will be created as needed.")

  (create-class [this token class] "Creates a class specification")
  (get-class [this token environment-name class-name] "Retrieves a class specification")
  (get-classes [this token environment-name] "Retrieves all class specifications in an environment")
  (get-all-classes [this token] "Retrieves all class specifications across all environments")
  (synchronize-classes [this token puppet-classes] "Synchronize database class definitions")
  (get-last-sync [this token] "Retrieve the time that classes were last synchronized with puppet")
  (delete-class [this token environment-name class-name] "Deletes a class specification")

  (create-environment [this token environment] "Creates an environment")
  (get-environment [this token environment-name] "Retrieves an environment")
  (get-environments [this token] "Retrieves all environments")
  (delete-environment [this token environment-name] "Deletes an environment"))

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
  {:all-group-access? "access all groups"
   :group-modify-children? "create or delete children"
   :group-edit-classification? "edit the classes and variables"
   :group-edit-environment? "change the environment"
   :group-edit-child-rules? "change the rules"
   :group-view? "view"})

(def Actions
  "These are the actions used by the default PermissionedClassifier
  implementation. The output of the :permitted-group-actions Permission
  function must be a subset of these values."
  #{:edit-classification :edit-environment :edit-child-rules :modify-children :view})

;;
;; Utilities for `app-with-permissions`
;;

(def redacted-str "puppetlabs.classifier/redacted")

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
  [ancestors viewable-group-ids]
  (let [[invis vis] (split-ancestors-by-viewability ancestors viewable-group-ids)]
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
  [groups viewable-group-ids]
  (->> (redact-invisible-ancestors groups viewable-group-ids)
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

;;
;; Permissions Wrapper
;;

(sc/defn app-with-permissions :- (sc/protocol PermissionedClassifier)
  [app :- (sc/protocol Classifier), permissions :- Permissions]
  (let [{:keys [all-group-access?
                group-modify-children?
                group-edit-classification?
                group-edit-environment?
                group-edit-child-rules?
                group-view?
                permitted-group-actions
                viewable-group-ids]} permissions
        wrap-all-group-access (fn [f]
                                  (fn [token & args]
                                    (if (all-group-access? token)
                                      (apply f app args)
                                      (throw+ (permission-exception :all-group-access? token)))))
        storage (app/get-storage app)
        annotate (if (satisfies? OptimizedStorage storage)
                   #(storage/annotate-group storage %)
                   #(naive/annotate-group storage %))
        get-ancestors (if (satisfies? OptimizedStorage storage)
                        storage/get-ancestors
                        naive/get-ancestors)
        get-group-ids (if (satisfies? OptimizedStorage storage)
                        storage/get-group-ids
                        naive/get-group-ids)
        group-validation-failures (if (satisfies? OptimizedStorage storage)
                                    storage/group-validation-failures
                                    naive/group-validation-failures)]

    (reify PermissionedClassifier
      (get-config [_] (app/get-config app))
      (get-storage [_] storage)

      ;;
      ;; Group Methods
      ;;

      (validate-group [_ token {:keys [id parent] :as group}]
        (let [ancs (get-ancestors storage group)]
          (if (storage/get-group storage id)
            (if-not (group-view? token id ancs)
              (throw+ (permission-exception :group-view? token id))
              (app/validate-group app group))
            ;; else (group doesn't exist)
            (if-not (group-modify-children? token parent (rest ancs))
              (throw+ (permission-exception :group-modify-children? token parent))
              (app/validate-group app group)))))

      (get-group [_ token id]
        (if-let [group (app/get-group app id)]
          (let [ancs (get-ancestors storage group)]
            (if-not (group-view? token id ancs)
              (throw+ (permission-exception :group-view? token id))
              group))))

      (get-group-as-inherited [_ token id]
        (if-let [group (storage/get-group storage id)]
          (let [ancs (get-ancestors storage group)]
            (if-not (group-view? token id ancs)
              (throw+ (permission-exception :group-view? token id))
              (let [viewable-ids (viewable-group-ids token (map :id (conj ancs group)))]
                (annotate (inherited-with-redaction (concat [group] ancs) viewable-ids)))))))

      (get-groups [_ token]
        (let [viewable-ids (viewable-group-ids token (get-group-ids storage))
              root (storage/get-group storage root-group-uuid)
              root-node (storage/get-subtree storage root)
              subtrees (viewable-subtrees (storage/get-subtree storage root) viewable-ids)]
          (for [subtree subtrees, group (tree->groups subtree)]
            (annotate group))))

      (create-group [_ token {id :id, parent-id :parent, :as group}]
        (let [ancs (get-ancestors storage group)
              actions (permitted-group-actions token id ancs)
              parent-actions (permitted-group-actions token parent-id (rest ancs))
              parent (storage/get-group storage parent-id)]
          (when-not (contains? parent-actions :modify-children)
            (throw+ (permission-exception :group-modify-children? token parent-id)))
          (when-let [vtree (group-validation-failures storage group)]
            (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-referents
                     :tree vtree
                     :ancestors (let [viewable-ids (viewable-group-ids token (map :id ancs))]
                                  (redact-invisible-ancestors ancs viewable-ids))}))
          (when (and (not (contains? actions :edit-environment))
                     (not= (:environment group) (:environment parent)))
            (throw+ (assoc (permission-exception :group-edit-environment? token id)
                           :description (str "create this group with a different environment"
                                             " than its parent's environment of "
                                             (pr-str (:environment parent)) " because you"
                                             " can't edit the parent's environment"))))
          (storage/create-group storage group)))

      (update-group [_ token {id :id :as delta}]
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
            (app/update-group app delta)
            (catch [:kind :puppetlabs.classifier.storage.postgres/missing-referents]
              {:keys [tree ancestors]}
              (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-referents
                       :tree tree
                       :ancestors (redact-invisible-ancestors
                                    ancestors
                                    (viewable-group-ids token (get-group-ids storage)))})))))

      (delete-group [_ token id]
        (if-let [{:keys [parent] :as group} (storage/get-group storage id)]
          (let [ancs (get-ancestors storage group)]
            (if (group-modify-children? token parent (rest ancs))
              (storage/delete-group storage id)
              (throw+ (permission-exception :group-modify-children? token parent))))))

      (import-hierarchy [_ token groups]
        ;; in order to import a new hierarchy, the subject needs to have
        ;;   1) edit-classification & edit-environment on the root group
        ;;   2) modify-children on the root group
        ;;   3) edit-child-rules on the root group
        (let [root-actions (permitted-group-actions token root-group-uuid [])
              required-actions #{:edit-classification :edit-environment :edit-child-rules
                                 :modify-children}]
          (when-not (= (set/intersection required-actions root-actions) required-actions)
            (let [missing-actions (set/difference required-actions root-actions)]
              (throw+ (permission-exception (first missing-actions)))))
          (storage/import-hierarchy storage groups)))

      ;;
      ;; Node Check-In methods
      ;;
      ;; Node check-ins contain the node's classification, which could contain
      ;; values from any group, so we only allow users that can view all groups
      ;; to see the check-ins.
      ;;

      (get-check-ins [_ token node-name]
        ((wrap-all-group-access app/get-check-ins) token node-name))
      (get-nodes [_ token]
        ((wrap-all-group-access app/get-nodes) token))

      ;;
      ;; Classification
      ;;

      (classify-node [_ token node transaction-uuid]
        ((wrap-all-group-access app/classify-node) token node transaction-uuid))

      ;;
      ;; Classification Explanation Scrubbing
      ;;

      (explain-classification [_ token node]
        (let [matching-group->ancestors (matching-groups-and-ancestors storage node)
              relevant-ids (into #{} (mapcat (fn [[group ancs]] (conj (map :id ancs) (:id group)))
                                             matching-group->ancestors))
              viewable-ids (viewable-group-ids token relevant-ids)
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
      ;; Other Methods
      ;;
      ;; These methods don't have any permissions associated with them.
      ;;

      (create-class [_ _ class]
        (app/create-class app class))
      (get-class [_ _ environment-name class-name]
        (app/get-class app environment-name class-name))
      (get-classes [_ _ environment-name]
        (app/get-classes app environment-name))
      (get-all-classes [_ _]
        (app/get-all-classes app)
      (synchronize-classes [_ _ puppet-classes]
        (app/synchronize-classes app puppet-classes))
      (get-last-sync [_ _]
        (app/get-last-sync app))
      (delete-class [_ _ environment-name class-name]
        (app/delete-class app environment-name class-name))

      (create-environment [_ _ environment]
        (app/create-environment app environment))
      (get-environment [_ _ environment-name]
        (app/get-environment app environment-name))
      (get-environments [_ _]
        (app/get-environments app))
      (delete-environment [_ _ environment-name]
        (app/delete-environment app environment-name)))))
