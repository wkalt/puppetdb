(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [routes GET PUT ANY]]
            [compojure.route :as route]
            [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :refer [generate-string]]
            [liberator.core :refer [defresource resource]]
            [cheshire.core :refer [parse-string]]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.rules :as rules]))

(defn malformed-or-parse
  "Returns false (i.e. not malformed) for well-formed json string `body`, along
  with a map storing the parse result in ::data. Returns true (i.e. malformed)
  for `body` string that causes an exception to be thrown during parsing, along
  with a map storing the exception in ::error and the body as a string in
  ::request-body."
  [body]
  (try
    (if-let [data (keywordize-keys (parse-string body))]
      [false {::data data}]
      true)
    (catch Exception e
      [true {::error e, ::request-body body}])))

(defn parse-if-body
  "If the request in ctx has a non-empty body, tries to parse it with
  malformed-or-parse; otherwise, returns false."
  [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (let [body-string (slurp body)]
      (if-not (empty? body-string)
        (malformed-or-parse body-string)
        false))
    false))

(defresource classifier-resource
  ;; action-fns is a map that mush have :get, :put, and :delete keys.
  ;; The values of those keys should be functions of the context, and if they
  ;; put a resource in the context, the key they put it in under should be
  ;; `:puppetlabs.classifier.http/resource` (i.e. ::resource in this namespace).
  [action-fns]
  :allowed-methods [:put :get]
  :available-media-types ["application/json"]
  :exists? (:get action-fns)
  :handle-ok ::resource
  :put! (:put action-fns)
  :handle-created ::resource
  :delete! (:delete action-fns)
  :malformed? parse-if-body
  :handle-malformed (fn [ctx] (format "Body is not valid JSON: %s"
                                      (::request-body ctx))))

(defn app [db]
  (routes
    (ANY "/v1/nodes/:node-name" [node-name]
         (classifier-resource
           {:get (fn [_]
                   (if-let [node (storage/get-node db node-name)]
                     {::resource node}))
            :put (fn [_]
                   (let [node {:name node-name}]
                     (storage/create-node db node)
                     {::resource node}))
            :delete (fn [_] (storage/delete-node db node-name))}))

    (ANY "/v1/groups/:group-name" [group-name]
         (classifier-resource
           {:get (fn [_]
                   (if-let [group (storage/get-group db group-name)]
                     {::resource group}))
            :put (fn [ctx] (let [group (or (::data ctx) {})]
                             (storage/create-group db (assoc group :name group-name))
                             {::resource group}))
            :delete (fn [_] (storage/delete-group db group-name))}))

    (ANY "/v1/classes/:class-name" [class-name]
         (classifier-resource
           {:get (fn [_]
                   (if-let [class (storage/get-class db class-name)]
                     {::resource class}))
            :put (fn [ctx] (let [class (or (::data ctx) {})]
                             (storage/create-class db (assoc class :name class-name))
                             {::resource class}))
            :delete (fn [_] (storage/delete-class db class-name))}))

    (ANY "/v1/rules" []
         (resource
           :allowed-methods [:post :get]
           :available-media-types ["application/json"]
           :handle-ok ::data
           :malformed? parse-if-body
           :handle-malformed (fn [ctx] (format "Body is not valid JSON: %s"
                                               (::request-body ctx)))
           :post! (fn [ctx]
                    (let [rule-id (storage/create-rule db (::data ctx))]
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
                              groups (mapcat (partial rules/apply-rule node) rules)
                              classes (mapcat :classes (map (partial storage/get-group db) groups))]
                          (assoc node :groups groups :classes classes)))))

    (route/not-found "Not found")))
