(ns puppetlabs.classifier.application)

(defprotocol Classifier
  (get-config [this] "Returns a map of the configuration settings for the application.")

  (classify-node [this node transaction-uuid] "Returns the node's classification as a map conforming to the Classification schema, provided that there are no conflicts encountered during classification-time. If conflicts are encountered, an exception is thrown.")
  (explain-classification [this node] "Returns an explanation of the node's classification as returned by the `puppetlabs.classifier.classification/classification-step` function.")

  (get-check-ins [this node-name] "Returns the check-in history (with classification) for the node with the given name.")
  (get-nodes [this] "Returns all nodes, each of which contains its own check-in history.")

  (validate-group [this group] "Validates the parent link, references, and inherited references for the given group and all of its descendents (if any). If the group would invalidate the group hierarchy's structure or introduce missing referents, a slingshot exception is thrown.")
  (create-group [this group] "Creates a new group")
  (get-group [this id] "Retrieves a group given its ID, a type-4 (i.e. random) UUID")
  (get-group-as-inherited [this id] "Retrieves a group with all its inherited classes, class parameters, and variables, given its ID")
  (get-groups [this] "Retrieves all groups")
  (update-group [this delta] "Edits any attribute of a group besides ID, subject to validation for illegal edits, malformed hierarchy structure, and missing references.")
  (delete-group [this id] "Deletes a group given its ID")

  (import-hierarchy [this groups] "Batch import a hierarchy given a flat collection of its groups. Any missing classes & class parameters will be created as needed.")

  (create-class [this class] "Creates a class specification")
  (get-class [this environment-name class-name] "Retrieves a class specification")
  (get-classes [this environment-name] "Retrieves all class specifications in an environment")
  (get-all-classes [this] "Retrieves all class specifications across all environments")
  (synchronize-classes [this puppet-classes] "Synchronize database class definitions")
  (get-last-sync [this] "Retrieve the time that classes were last synchronized with puppet")
  (delete-class [this environment-name class-name] "Deletes a class specification")

  (create-environment [this environment] "Creates an environment")
  (get-environment [this environment-name] "Retrieves an environment")
  (get-environments [this] "Retrieves all environments")
  (delete-environment [this environment-name] "Deletes an environment"))
