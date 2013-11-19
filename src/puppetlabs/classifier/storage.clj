(ns puppetlabs.classifier.storage)

(defprotocol Storage
  (create-node [this node] "Creates a new node object")
  (get-node [this node] "Retrieves a node")
  (create-group [this group] "Creates a new group")
  (get-group [this group] "Retrieves a group"))
