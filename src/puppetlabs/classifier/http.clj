(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [defroutes GET]]
            [compojure.route :as route]
            [cheshire.core :refer [generate-string]]))

(defroutes app
  (GET "/v1/classified/nodes/:node" [node]
    {:status 200
     :headers {"content-type" "application/json"}
     :body (generate-string {:name node
                             :classes ["foo"]
                             :environment "production"
                             :parameters {}})})
  )
