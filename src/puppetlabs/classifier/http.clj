(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [routes GET PUT ANY]]
            [compojure.route :as route]
            [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :refer [generate-string]]
            [liberator.core :refer [resource]]
            [puppetlabs.classifier.storage :as storage]
            [cheshire.core :refer [parse-string]]))

(defn malformed-or-parse
  "Returns true for well-formed json along with a map storing the parse result
  in :data. Returns false for malformed json."
  [body]
  (try 
    (if-let [data (keywordize-keys (parse-string (slurp body)))]
      [false {:data data}]
      true)
    (catch Exception e
      [true {:error e}])))

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
           :handle-created (fn [ctx] {:name node})
           :handle-delete (fn [ctx]
                            (storage/delete-node db node))))

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
           :handle-created (fn [ctx] {:name group})
           :handle-delete (fn [ctx] (storage/delete-group db group))))

    (ANY "/v1/classes/:class-name" [class-name]
         (resource
           :allowed-methods [:put :get]
           :available-media-types ["application/json"]
           :exists? (fn [_]
                      (if-let [class (storage/get-class db class-name)]
                        {:class class}))
           :handle-ok :class
           :malformed? (fn [ctx]
                         (if-let [body (get-in ctx [:request :body])]
                           (malformed-or-parse body)
                           false))
           :handle-malformed (fn [ctx] (format "Body not valid JSON: %s" (get ctx :body)))
           :put! (fn [ctx]
                   (storage/create-class db (get ctx :data)))
           :handle-created :data))

    (GET "/v1/classified/nodes/:node" [node]
      {:status 200
       :headers {"content-type" "application/json"}
       :body (generate-string {:name node
                               :classes ["foo"]
                               :environment "production"
                               :parameters {}})})

    (route/not-found "Not found")))
