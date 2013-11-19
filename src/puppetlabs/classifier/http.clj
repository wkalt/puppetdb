(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [routes GET PUT ANY]]
            [compojure.route :as route]
            [cheshire.core :refer [generate-string]]
            [liberator.core :refer [resource]]
            [puppetlabs.classifier.storage :as storage]))

(defn app [db]
  (routes
    (ANY "/v1/nodes/:node" [node]
         (resource
           :allowed-methods [:put :get]
           :available-media-types ["application/json"]
           :exists? (fn [ctx]
                      (if-let [node (storage/get-node db node)]
                        {:node node}))
           :handle-ok (fn [ctx] {:name (get ctx :node)})
           :put! (fn [ctx]
                   (storage/create-node db node))
           :handle-created (fn [ctx] {:name node})))

    (ANY "/v1/groups/:group" [group]
         (resource
           :allowed-methods [:put :get]
           :available-media-types ["application/json"]
           :exists? (fn [ctx]
                      (if-let [group (storage/get-group db group)]
                        {:group group}))
           :handle-ok (fn [ctx] {:name (get ctx :group)})
           :put! (fn [ctx]
                   (storage/create-group db group)
                   )
           :handle-created (fn [ctx] {:name group})))

    (GET "/v1/classified/nodes/:node" [node]
      {:status 200
       :headers {"content-type" "application/json"}
       :body (generate-string {:name node
                               :classes ["foo"]
                               :environment "production"
                               :parameters {}})})

    (route/not-found "Not found")))
