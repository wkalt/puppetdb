(ns puppetlabs.classifier.application.permissioned.rbac
  (:require [schema.core :as sc]
            [puppetlabs.rbac.services.authz :as authz]
            [puppetlabs.classifier.application.permissioned :refer [Permissions]]))
(def rbac-object-type "node_groups")

(def rbac-actions {:edit-classification "edit_classification"
                   :edit-environment "set_environment"
                   :edit-child-rules "edit_child_rules"
                   :modify-children "modify_children"
                   :view "view"})

(defn rbac-permission
  [action id]
  {:object_type rbac-object-type, :action (get rbac-actions action), :instance (str id)})

(defn rbac-permission-str
  [action id]
  (str rbac-object-type ":" (get rbac-actions action) ":" id))

(sc/defn rbac-service-permissions :- Permissions
  [authz-service]
  (let [svc authz-service
        perm-str rbac-permission-str
        any-perm? (fn [subj perm-strs]
                    (->> (authz/are-permitted? svc subj perm-strs)
                      (some identity)
                      boolean))
        action-for-any-id? (fn [subj action ids]
                             (any-perm? subj (map (partial perm-str action) (conj ids "*"))))]
    {:all-group-access? (fn [subj]
                          (authz/is-permitted? svc subj (perm-str :view "*")))
     :group-edit-classification? (fn [subj id ancs]
                                   (action-for-any-id? subj :edit-classification (conj ancs id)))
     :group-edit-environment? (fn [subj id ancs]
                                (action-for-any-id? subj :edit-environment (conj ancs id)))
     :group-edit-child-rules? (fn [subj id ancs]
                                (action-for-any-id? subj :edit-child-rules (conj ancs id)))
     :group-modify-children? (fn [subj id ancs]
                               (action-for-any-id? subj :modify-children (conj ancs id)))
     :group-view? (fn [subj id ancs]
                    (action-for-any-id? subj :view (conj ancs id)))
     :permitted-group-actions (fn [subj id ancs]
                                (let [ids (conj ancs id)]
                                  (into #{} (for [action (keys rbac-actions)
                                                :when (action-for-any-id? subj action ids)]
                                              action))))
     :viewable-group-ids (fn [subj all-the-ids]
                           (if (authz/is-permitted? svc subj (perm-str :view "*"))
                             (set all-the-ids)
                             (into #{} (for [id all-the-ids
                                             :when (authz/is-permitted? svc subj
                                                                        (perm-str :view id))]
                                         id))))}))
