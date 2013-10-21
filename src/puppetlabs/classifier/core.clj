(ns puppetlabs.classifier.core
  (:require [ring.adapter.jetty :as jetty]
            [puppetlabs.classifier.http :as http]))


(defn -main
  "I don't do a whole lot."
  [& args]
  (jetty/run-jetty http/app {:port 8080 :join? true}))
