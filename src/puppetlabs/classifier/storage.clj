(ns puppetlabs.classifier.storage)

(defprotocol Storage
  (create-node [this node] "Creates a new node object")
  (get-node [this node] "Retrieves a node"))

(defn memory []
  (let [node-storage (ref #{})]
    (reify
      Storage

      (create-node [this node] (dosync (alter node-storage conj node)))
      (get-node [this node] (if (contains? (deref node-storage) node) node nil))
      )))
