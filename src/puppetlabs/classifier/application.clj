(ns puppetlabs.classifier.application
  (:require [ring.adapter.jetty :as jetty]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage.postgres :as postgres]))

(def db-spec {:subprotocol "postgresql"
              :subname "classifier"
              :user "classifier"
              :passwd "classifier"})

(defn run
  [& args]
  (let [database (postgres/new-db db-spec)]
    ; (postgres/init-schema db-spec)
    (jetty/run-jetty (http/app database) {:port 8080 :join? true})) 
  )
