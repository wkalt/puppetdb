(ns puppetlabs.classifier.storage)

(defprotocol Storage
  (create-node [this node] "Creates a new node object")
  (get-node [this node] "Retrieves a node")
  (delete-node [this node] "Deletes a node")
  (create-group [this group] "Creates a new group")
  (get-group [this group] "Retrieves a group")
  (delete-group [this group] "Deletes a group")
  (get-class [this class-name] "Retrieves a class specification")
  (create-class [this class] "Creates a class specification")
  (delete-class [this class-name] "Deletes a class specification"))
