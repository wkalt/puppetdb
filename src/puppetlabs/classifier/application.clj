(ns puppetlabs.classifier.application
  (:require [compojure.core :refer [context]]
            [puppetlabs.classifier.http :as http]
            [puppetlabs.classifier.storage.postgres :as postgres]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [clojure.tools.logging :as log]))

(def db-spec {:subprotocol "postgresql"
              :subname "classifier"
              :user "classifier"
              :passwd "classifier"})
(defn on-shutdown
  []
  (log/info "Classifier service shutting down."))

(defservice classifier-service
   {:depends [[:config-service get-config]
              [:webserver-service add-ring-handler]]
    :provides [shutdown]}
   (let [database     (postgres/new-db db-spec)
         context-path "/classifier"
         ;; TODO: wondering if we should add a function "add-compojure-app"
         ;;  that deals with the wrapping with compojure.core/context here
         context-app  (context context-path [] (http/app database))]
     (add-ring-handler context-app context-path))
   {:shutdown on-shutdown})
