(ns puppetlabs.classifier.storage.permissioned
  (:require [clojure.walk :refer [prewalk]]
            [fast-zip.visit :as zv]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.schema :refer [hierarchy-zipper]]
            [puppetlabs.classifier.storage :as storage :refer [Storage]]))

(defprotocol PermissionedStorage
  "This \"wraps\" the Storage protocol with permissions. It has all the same
  methods as the Storage protocol, but they all take the authentication token
  for the user as their first argument (the rest are the same as for the plain
  Storage methods)."

  (store-check-in [this token check-in] "Store a node check-in.")
  (get-check-ins [this token node-name] "Retrieve all check-ins for a node by name.")
  (get-nodes [this token] "Retrieve check-ins for all nodes.")

  (validate-group [this token group] "Performs validation of references and inherited values for the subtree of the hierarchy rooted at the group.")
  (create-group [this token group] "Creates a new group if permitted.")
  (get-group [this token id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID if permitted.")
  (annotate-group [this token group] "Returns an annotated version of the group that shows which classes and parameters are no longer present in Puppet, if permitted to access the original group.")
  (get-groups [this token] "Retrieves all groups if permitted.")
  (get-ancestors [this token group] "Retrieves the ancestors of the group, up to & including the root group, as a vector starting at the immediate parent and ending with the route, if permitted to view all of said groups.")
  (get-subtree [this token group] "Returns the subtree of the group hierarchy rooted at the passed group, if permitted to view all of said groups.")
  (update-group [this token delta] "Updates class/parameter and variable fields of a group if permitted.")
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
  predicates, which take an RBAC API token and group id (if applicable) and
  return a boolean indicating whether the subject owning the token has
  the given permission. The non-predicates are :permitted-group-actions and
  :viewable-group-ids, which take a token and return either the actions the
  subject is allowed to perform on the group, or a list of the ids of groups the
  subject as allowed to view."
  (let [Function (sc/pred fn?)]
    {:classifier-access? Function
     :group-create? Function
     :group-delete? Function
     :group-edit-classification? Function
     :group-edit-environment? Function
     :group-edit-parent? Function
     :group-edit-rules? Function
     :group-view? Function
     :permitted-group-actions Function
     :viewable-group-ids Function}))

(def permission->description
  "Human-readable descriptions for the action that each permission key in
  a Permissions map represents, mainly to embed in error messages."
  {:classifier-access? "access the classifier"
   :group-create? "create a group"
   :group-delete? "delete a group"
   :group-edit-classification? "edit the group's classification values"
   :group-edit-environment? "change the group's environment"
   :group-edit-parent? "change a group's parent"
   :group-edit-rules? "change the group's rules"
   :group-view? "view the group"})

(defn- throw-permission-exception
  "Throws a slingshot exception for a permission denial. `permission` must be
  one of the keys allowed in a Permissions map."
  ([permission token] (throw-permission-exception permission token nil))
  ([permission token group-id]
   {:pre [(contains? Permissions permission)]}
   (let [guaranteed-info {:kind ::permission-denied
                          :permission-to permission
                          :permission-description (permission->description permission)
                          :rbac-token token}
         exc-info (if group-id
                    (assoc guaranteed-info :group-id group-id)
                    guaranteed-info)]
     (throw+ exc-info))))

(defn- viewable-subtrees
  [root-node viewable-ids]
  (let [zip-root (hierarchy-zipper root-node)
        viewable-visitor (zv/visitor
                           :pre [node subtrees]
                            (if (viewable-ids (-> node :group :id))
                             {:state (conj subtrees node), :cut true}))]
    (:state (zv/visit zip-root [] [viewable-visitor]))))

(sc/defn storage-with-permissions :- (sc/protocol PermissionedStorage)
  [storage :- (sc/protocol Storage), permissions :- Permissions]
  (let [{:keys [classifier-access?
                group-create?
                group-delete?
                group-edit-classification?
                group-edit-environment?
                group-edit-parent?
                group-edit-rules?
                group-view?
                permitted-group-actions
                viewable-group-ids]} permissions
        wrap-access-permissions (fn [f]
                                  (fn [this token & args]
                                    (if (classifier-access? token)
                                      (apply f storage args)
                                      (throw-permission-exception :classifier-access? token))))]

    (reify PermissionedStorage
      ;; the group methods are the only interesting ones; the rest just depend
      ;; on whether the token's subject has classifier access at all
      (validate-group [this token {id :id :as group}]
        (if-let [group (storage/get-group storage id)]
          (if (group-view? token id)
            (storage/validate-group storage group)
            (throw-permission-exception :group-view? token id))
          (if (group-create? token)
            (storage/validate-group storage group)
            (throw-permission-exception :group-create? token))))
      (create-group [this token group]
        (if (group-create? token)
          (storage/create-group storage group)
          (throw-permission-exception :group-create? token)))
      (get-group [this token id]
        (if (group-view? token id)
          (storage/get-group storage id)
          (throw-permission-exception :group-view? token id)))
      (get-groups [this token]
        (let [viewable-ids (set (viewable-group-ids token))]
          (filter #(contains? viewable-ids (:id %)) (storage/get-groups storage))))
      (get-ancestors [this token group]
        (let [viewable-ids (set (viewable-group-ids token))]
          (filter #(contains? viewable-ids (:id %)) (storage/get-ancestors storage group))))
      (get-subtree [this token group]
        (let [viewable-ids (set (viewable-group-ids token))
              subtree (storage/get-subtree storage group)]
          (prewalk (fn [form]
                     (if (and (map? form)
                              (contains? form :group)
                              (contains? form :children)
                              (not (contains? viewable-ids (get-in form [:group :id]))))
                       (assoc form :group nil)
                       form))
                   subtree)))
      (update-group [this token delta]
        (let [id (:id delta)
              permitted-actions (->> (permitted-group-actions token id)
                                  (map keyword)
                                  set)]
          (when-not (permitted-actions :view)
            (throw-permission-exception :group-view? token id))
          (when (and (contains? delta :parent) (not (permitted-actions :edit_parent)))
            (throw-permission-exception :group-edit-parent? token))
          (when (and (contains? delta :rule) (not (permitted-actions :edit_rules)))
            (throw-permission-exception :group-edit-rules? token id))
          (when (and (contains? delta :environment) (not (permitted-actions :edit_env)))
            (throw-permission-exception :group-edit-environment? token id))
          (when (and (some #(contains? delta %) [:name :classes :variables])
                     (not (contains? permitted-actions :configure)))
            (throw-permission-exception :group-edit-classification? token id))
          (storage/update-group storage delta)))
      (delete-group [this token id]
        (if (group-delete? token id)
          (storage/delete-group storage id)
          (throw-permission-exception :group-delete? token)))

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
