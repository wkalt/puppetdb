(ns puppetlabs.classifier.storage)

(defprotocol Storage
  (create-node [this node] "Creates a new node object")
  (get-node [this node] "Retrieves a node")
  (get-nodes [this] "Retrieves all nodes")
  (delete-node [this node] "Deletes a node")
  (store-check-in [this check-in] "Store a node check-in.")
  (get-check-ins [this node-name] "Retrieve all check-ins for a node.")

  (validate-group [this group] "Performs validation of references and inherited values for the subtree of the hierarchy rooted at the group.")
  (create-group [this group] "Creates a new group")
  (get-group [this id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID")
  (annotate-group [this group] "Returns an annotated version of the group that shows which classes and parameters are no longer present in Puppet.")
  (get-groups [this] "Retrieves all groups")
  (get-ancestors [this group] "Retrieves the ancestors of the group, up to & including the root group, as a vector starting at the immediate parent and ending with the route.")
  (get-subtree [this group] "Returns the subtree of the group hierarchy rooted at the passed group.")
  (update-group [this delta] "Updates class/parameter and variable fields of a group")
  (delete-group [this id] "Deletes a group given its ID")

  (create-class [this class] "Creates a class specification")
  (get-class [this environment-name class-name] "Retrieves a class specification")
  (get-classes [this environment-name] "Retrieves all class specifications in an environment")
  (synchronize-classes [this puppet-classes] "Synchronize database class definitions")
  (delete-class [this environment-name class-name] "Deletes a class specification")

  (get-rules [this] "Retrieve all rules")

  (create-environment [this environment] "Creates an environment")
  (get-environment [this environment-name] "Retrieves an environment")
  (get-environments [this] "Retrieves all environments")
  (delete-environment [this environment-name] "Deletes an environment"))
