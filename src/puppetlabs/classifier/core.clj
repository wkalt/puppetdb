(ns puppetlabs.classifier.core
  (:require [ring.adapter.jetty :as jetty]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage :as storage])
  (:gen-class))


(def nodb
  (reify
    storage/Storage

    (create-node [this node] (if (= "addone" node) "addone" nil))
    (get-node [this node] (if (= "addone" node) "addone" nil))
    )
  )

(defn -main
  "I don't do a whole lot."
  [& args]
    (jetty/run-jetty (http/app (storage/memory)) {:port 8080 :join? true}))
