(ns puppetlabs.classifier.storage)

(defprotocol Storage
  (create-node [this node] "Creates a new node object")
  (get-node [this node] "Retrieves a node")
  (get-nodes [this] "Retrieves all nodes")
  (delete-node [this node] "Deletes a node")

  (create-group [this group] "Creates a new group")
  (get-group-by-id [this id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID")
  (get-group-by-name [this group-name] "Retrieves a group given its name")
  (get-groups [this] "Retrieves all groups")
  (update-group [this delta] "Updates class/parameter and variable fields of a group")
  (delete-group-by-id [this id] "Deletes a group given its ID")
  (delete-group-by-name [this group-name] "Deletes a group given its name")

  (create-class [this class] "Creates a class specification")
  (get-class [this class-name] "Retrieves a class specification")
  (get-classes [this] "Retrieves all class specifications")
  (delete-class [this class-name] "Deletes a class specification")

  (create-rule [this rule] "Creates a rule")
  (get-rules [this] "Retrieve all rules")

  (create-environment [this environment] "Creates an environment")
  (get-environment [this environment-name] "Retrieves an environment")
  (get-environments [this] "Retrieves all environments")
  (delete-environment [this environment-name] "Deletes an environment"))
