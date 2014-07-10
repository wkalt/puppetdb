(ns puppetlabs.classifier.storage.permissioned
  (:require [clojure.set :as set]
            [clojure.walk :refer [prewalk]]
            [fast-zip.visit :as zv]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.rules :refer [always-matches]]
            [puppetlabs.classifier.schema :refer [group->classification hierarchy-zipper
                                                  tree->groups]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid Storage]]
            [puppetlabs.classifier.util :refer [map-delta merge-and-clean]]))

(defprotocol PermissionedStorage
  "This \"wraps\" the Storage protocol with permissions. It has all the same
  methods as the Storage protocol, but they all take the authentication token
  for the user as their first argument (the rest are the same as for the plain
  Storage methods)."

  (store-check-in [this token check-in] "Store a node check-in.")
  (get-check-ins [this token node-name] "Retrieve all check-ins for a node by name.")
  (get-nodes [this token] "Retrieve check-ins for all nodes.")

  (validate-group [this token group] "Performs validation of references and inherited values for the subtree of the hierarchy rooted at the group, scrubbing inherited values from the returned errors if the token's subject is not permitted to view the group that the values were inherited from.")
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
    {:classifier-access? Function
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
  {:classifier-access? "access the classifier"
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

(defn- scrub-inheritable-values
  [{:keys [classes variables] :as group}]
  (assoc group
         :classes (into {} (for [[class-kw params] classes]
                             [class-kw (into {} (for [[param _] params]
                                                  [param "puppetlabs.classifier/redacted"]))]))
         :variables (into {} (for [[variable _] variables]
                               [variable "puppetlabs.classifier/redacted"]))))

(defn- scrub-invisible-ancestors
  [ancestors viewable-group-ids]
  (let [[invis vis] (split-ancestors-by-viewability ancestors viewable-group-ids)]
    (concat vis (map scrub-inheritable-values invis))))

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
  [storage :- (sc/protocol Storage), permissions :- Permissions]
  (let [{:keys [classifier-access?
                group-modify-children?
                group-edit-classification?
                group-edit-environment?
                group-edit-child-rules?
                group-view?
                permitted-group-actions
                viewable-group-ids]} permissions
        wrap-access-permissions (fn [f]
                                  (fn [this token & args]
                                    (if (classifier-access? token)
                                      (apply f storage args)
                                      (throw+ (permission-exception :classifier-access? token)))))]

    (reify PermissionedStorage
      ;;
      ;; Group Storage methods
      ;;
      ;; The group methods are the only interesting ones; the rest just depend
      ;; on whether the token's subject has classifier access at all.
      ;;

      (validate-group [this token {:keys [id parent] :as group}]
        (let [ancs (storage/get-ancestors storage group)]
          (if-let [group (storage/get-group storage id)]
            (if-not (group-view? token id ancs)
              (throw+ (permission-exception :group-view? token id))
              (storage/validate-group storage group))
            ;; else (group doesn't exist)
            (if-not (group-modify-children? token parent (rest ancs))
              (throw+ (permission-exception :group-modify-children? token parent))
              (storage/validate-group storage group)))))

      (create-group [this token {id :id, parent-id :parent, :as group}]
        (let [ancs (storage/get-ancestors storage group)
              actions (permitted-group-actions token id ancs)
              parent-actions (permitted-group-actions token parent-id (rest ancs))
              parent (storage/get-group storage parent-id)]
          (when-not (contains? parent-actions :modify-children)
            (throw+ (permission-exception :group-modify-children? token parent-id)))
          (when-let [vtree (storage/validate-group storage group)]
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
              ancs (storage/get-ancestors storage group)]
          (if-not (group-view? token id ancs)
            (throw+ (permission-exception :group-view? token id))
            group)))

      (get-group-as-inherited [this token id]
        (let [group (storage/get-group storage id)
              ancs (storage/get-ancestors storage group)]
          (if-not (group-view? token id ancs)
            (throw+ (permission-exception :group-view? token id))
            (let [viewable-ids (viewable-group-ids token)
                  [invis vis] (split-ancestors-by-viewability ancs viewable-ids)
                  scrubbed (map scrub-inheritable-values invis)
                  class8ns (map group->classification (concat [group] vis scrubbed))
                  inherited (class8n/collapse-to-inherited class8ns)]
              (merge group inherited)))))

      (get-groups [this token]
        (let [viewable-ids (viewable-group-ids token)
              root (storage/get-group storage root-group-uuid)
              root-node (storage/get-subtree storage root)
              subtrees (viewable-subtrees (storage/get-subtree storage root) viewable-ids)]
          (for [subtree subtrees, group (tree->groups subtree)]
            group)))

      (get-ancestors [this token group]
        (let [viewable-ids (viewable-group-ids token)
              ancs (storage/get-ancestors storage group)
              [_ vis] (split-ancestors-by-viewability ancs viewable-ids)]
          vis))

      (get-subtree [this token {id :id :as group}]
        (let [ancs (storage/get-ancestors storage group)]
          (if-not (group-view? token id ancs)
            (throw+ (permission-exception :group-view? token id))
            (storage/get-subtree storage group))))

      (update-group [this token {id :id :as delta}]
        (let [group (storage/get-group storage id)
              group' (merge-and-clean group delta)
              only-changes (map-delta group group')
              ancs' (storage/get-ancestors storage group')
              ancs (if (contains? only-changes :parent)
                     (storage/get-ancestors storage group)
                     ancs')]
          (check-update-permissions
            only-changes token id (:parent group') (:parent group) ancs' ancs permissions)
          (when-let [vtree (storage/validate-delta storage delta group)]
            (throw+ {:kind :puppetlabs.classifier.storage.postgres/missing-referents
                     :tree vtree
                     :ancestors (scrub-invisible-ancestors ancs (viewable-group-ids token))}))
          (storage/update-group storage delta)))

      (delete-group [this token id]
        (if-let [{:keys [parent] :as group} (storage/get-group storage id)]
          (let [ancs (storage/get-ancestors storage group)]
            (if (group-modify-children? token parent (rest ancs))
              (storage/delete-group storage id)
              (throw+ (permission-exception :group-modify-children? token parent))))))

      ;;
      ;; Non-Group Storage methods
      ;;
      ;; These all just depend on whether the token's owner has any permissions
      ;; in RBAC for NC at all.
      ;;

      (store-check-in [this token check-in]
        ((wrap-access-permissions storage/store-check-in) this token check-in))
      (get-check-ins [this token node-name]
        ((wrap-access-permissions storage/get-check-ins) this token node-name))
      (get-nodes [this token]
        ((wrap-access-permissions storage/get-nodes) this token))

      (create-class [this token class]
        ((wrap-access-permissions storage/create-class) this token class))
      (get-class [this token environment-name class-name]
        ((wrap-access-permissions storage/get-class) this token environment-name class-name))
      (get-classes [this token environment-name]
        ((wrap-access-permissions storage/get-classes) this token environment-name))
      (synchronize-classes [this token puppet-classes]
        ((wrap-access-permissions storage/synchronize-classes) this token puppet-classes))
      (delete-class [this token environment-name class-name]
        ((wrap-access-permissions storage/delete-class) this token environment-name class-name))

      (get-rules [this token] ((wrap-access-permissions storage/get-rules) this token))

      (create-environment [this token environment]
        ((wrap-access-permissions storage/create-environment) this token environment))
      (get-environment [this token environment-name]
        ((wrap-access-permissions storage/get-environment) this token environment-name))
      (get-environments [this token]
        ((wrap-access-permissions storage/get-environments) this token))
      (delete-environment [this token environment-name]
        ((wrap-access-permissions storage/delete-environment) this token environment-name)))))
