(ns puppetlabs.classifier.storage.permissioned)

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
