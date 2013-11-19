(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [routes GET PUT]]
            [compojure.route :as route]
            [cheshire.core :refer [generate-string]]
            [puppetlabs.classifier.storage :as storage]))

(defn app [db]
  (routes
    (PUT "/v1/nodes/:node" [node]
      (storage/create-node db node)
      {:status 201
       :body (generate-string {:name node})})

    (GET "/v1/nodes/:node" [node]
      (let [node (storage/get-node db node)]
        (if (nil? node)
          {:status 404
           :body "Node not found"}
          {:status 200
           :body (generate-string {:name node})})))

    (PUT "/v1/groups/:group" [group]
      (storage/create-group db group)
      {:status 201
       :body (generate-string {:name group})})

    (GET "/v1/groups/:group" [group]
      (if-let [group (storage/get-group db group)]
        {:status 200
         :body (generate-string {:name group})}))

    (GET "/v1/classified/nodes/:node" [node]
      {:status 200
       :headers {"content-type" "application/json"}
       :body (generate-string {:name node
                               :classes ["foo"]
                               :environment "production"
                               :parameters {}})})

    (route/not-found "Not found")))
