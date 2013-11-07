(ns puppetlabs.classifier.core
  (:require [ring.adapter.jetty :as jetty]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage.postgres :refer :all])
  (:gen-class))

(def db-spec {:subprotocol "postgresql"
              :subname "classifier"
              :user "classifier"
              :passwd "classifier"})

(defn -main
  "I don't do a whole lot."
  [& args]
  (let [database (puppetlabs.classifier.storage.postgres.Postgres. db-spec)]
    ; (init-schema db-spec)
    (jetty/run-jetty (http/app database) {:port 8080 :join? true})))
