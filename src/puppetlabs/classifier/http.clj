(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [routes GET PUT ANY]]
            [compojure.route :as route]
            [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :refer [generate-string]]
            [liberator.core :refer [resource]]
            [cheshire.core :refer [parse-string]]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.rules :as rules]))

(defn malformed-or-parse
  "Returns true for well-formed json along with a map storing the parse result
  in ::data. Returns false for malformed json."
  [body]
  (try
    (if-let [data (keywordize-keys (parse-string (slurp body)))]
      [false {::data data}]
      true)
    (catch Exception e
      [true {::error e}])))

(defn app [db]
  (routes
    (ANY "/v1/nodes/:node-name" [node-name]
         (resource
           :allowed-methods [:put :get]
           :available-media-types ["application/json"]
           :exists? (fn [ctx]
                      (if-let [node (storage/get-node db node-name)]
                        {::node node}))
           :handle-ok ::node
           :put! (fn [ctx] (let [node {:name node-name}]
                             (storage/create-node db node)
                             {::node node}))
           :handle-created ::node
           :handle-delete (fn [ctx]
                            (storage/delete-node db node-name))))

    (ANY "/v1/groups/:group-name" [group-name]
         (resource
           :allowed-methods [:put :get]
           :available-media-types ["application/json"]
           :exists? (fn [ctx]
                      (if-let [group (storage/get-group db group-name)]
                        {::group group}))
           :handle-ok ::group
           :put! (fn [ctx] (let [group {:name group-name}]
                             (storage/create-group db group)
                             {::group group}))
           :handle-created ::group
           :handle-delete (fn [ctx] (storage/delete-group db group-name))))

    (ANY "/v1/classes/:class-name" [class-name]
         (resource
           :allowed-methods [:put :get]
           :available-media-types ["application/json"]
           :exists? (fn [_]
                      (if-let [class (storage/get-class db class-name)]
                        {::class class}))
           :handle-ok ::class
           :malformed? (fn [ctx]
                         (if-let [body (get-in ctx [:request :body])]
                           (malformed-or-parse body)
                           false))
           :handle-malformed (fn [ctx] (format "Body not valid JSON: %s" (get ctx :body)))
           :put! (fn [ctx]
                   (storage/create-class db (get ctx ::data)))
           :handle-created ::data
           :handle-delete (fn [ctx] (storage/delete-class db class-name))))

    (ANY "/v1/rules" []
         (resource
           :allowed-methods [:post :get]
           :available-media-types ["application/json"]
           :handle-ok ::data
           :malformed? (fn [ctx]
                         (if-let [body (get-in ctx [:request :body])]
                           (malformed-or-parse body)
                           false))
           :handle-malformed (fn [ctx] (format "Body not valid JSON: %s" (get ctx :body)))
           :post! (fn [ctx]
                   (let [rule-id (storage/create-rule db (get ctx ::data))]
                     {::location (str "/v1/rules/" rule-id)}))
           :location ::location))

    (ANY "/v1/classified/nodes/:node-name" [node-name]
         (resource
           :allowed-methods [:get]
           :available-media-types ["application/json"]
           :exists? true
           :handle-ok (fn [ctx]
                        (let [node (merge {:name node-name} (storage/get-node db node-name))
                              rules (storage/get-rules db)
                              groups (mapcat (partial rules/apply-rule node) rules)]
                          (merge node {:groups groups})))))

    (route/not-found "Not found")))
