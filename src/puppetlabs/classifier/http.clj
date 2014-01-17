(ns puppetlabs.classifier.http
  (:require [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :refer [generate-string parse-string]]
            [compojure.core :refer [routes GET PUT ANY]]
            [compojure.route :as route]
            [liberator.core :refer [resource run-resource]]
            [schema.core :as sc]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Group Node Rule Environment]])
  (:import com.fasterxml.jackson.core.JsonParseException))

(def ^:private PuppetClass puppetlabs.classifier.schema/Class)

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
    (catch JsonParseException e
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

(defn wrap-schema-fail
  "This wraps a ring handler that could throw a schema validation error. If such
  an error is thrown, this function catches it and produces a 400 response whose
  body is the full error message."
  [handler]
  (fn [request]
    (try (handler request)
      (catch clojure.lang.ExceptionInfo e
        ;; re-throw things that aren't schema validation errors
        (when-not (re-find #"Value does not match schema" (.getMessage e))
          (throw e))
        {:status 400
         :headers {"Content-Type" "text/plain"}
         ;; TODO: should we process this error message to disguise its clojure
         ;; origins by e.g. converting EDN representations to JSON?
         :body (.toString e)}))))

(def ResourceImplementation
  {(sc/required-key :get) (sc/->FnSchema sc/Any sc/Any)
   (sc/required-key :put) (sc/->FnSchema sc/Any sc/Any)
   (sc/required-key :delete) (sc/->FnSchema sc/Any sc/Any)})

(defn classifier-resource
  "The `implementation` is a map with all the necessary action fns for this
  resource. See the ResourceImplementation schema defined above for more
  details."
  [implementation]
  {:pre [(sc/validate ResourceImplementation implementation)]}
  (fn [request]
    (run-resource
      request
      {:allowed-methods [:put :get]
       :available-media-types ["application/json"]
       :exists? (:get implementation)
       :handle-ok ::retrieved
       :put! (:put implementation)
       :handle-created ::created
       :delete! (:delete implementation)
       :malformed? parse-if-body
       :handle-malformed (fn [ctx]
                           (format "Body is not valid JSON: %s"
                                   (::request-body ctx)))})))

(sc/defn crud-resource
  "Create a basic CRUD endpoint for a resource, given a storage object and a
  map of functions to store the resource."
  [resource-name :- String
   schema :- sc/Schema
   storage :- (sc/protocol storage/Storage)
   defaults :- {sc/Keyword sc/Any}
   storage-fns :- {:get (sc/->FnSchema sc/Any sc/Any)
                   :create (sc/->FnSchema sc/Any sc/Any)
                   :delete (sc/->FnSchema sc/Any sc/Any)}]
  (classifier-resource
    {:get (fn [_]
            (if-let [resource ((:get storage-fns) storage resource-name)]
              {::retrieved resource}))
     :put (fn [ctx]
            (let [resource (-> (merge defaults (::data ctx {}))
                               (assoc :name resource-name))]
              (sc/validate schema resource)
              ((:create storage-fns) storage resource)
              {::created resource}))
     :delete (fn [_]
               ((:delete storage-fns) storage resource-name))}))


(defn app [db]
  (wrap-schema-fail
    (routes
      (ANY "/v1/nodes/:node-name" [node-name]
           (crud-resource node-name Node db {}
             {:get storage/get-node
              :create storage/create-node
              :delete storage/delete-node}))

      (ANY "/v1/groups/:group-name" [group-name]
           (crud-resource group-name Group db
             {:environment "production"}
             {:get storage/get-group
              :create storage/create-group
              :delete storage/delete-group}))

      (ANY "/v1/classes/:class-name" [class-name]
           (crud-resource class-name PuppetClass db
             {:environment "production"}
             {:get storage/get-class
              :create storage/create-class
              :delete storage/delete-class}))

      (ANY "/v1/environments/:environment-name" [environment-name]
           (crud-resource environment-name Environment db {}
             {:get storage/get-environment
              :create storage/create-environment
              :delete storage/delete-environment}))

      (ANY "/v1/rules" []
           (resource
             :allowed-methods [:post :get]
             :available-media-types ["application/json"]
             :handle-ok ::data
             :malformed? parse-if-body
             :handle-malformed (fn [ctx]
                                 (format "Body is not valid JSON: %s"
                                         (::request-body ctx)))
             :post! (fn [ctx]
                      (let [rule (sc/validate Rule (::data ctx))
                            rule-id (storage/create-rule db rule)]
                        {::location (str "/v1/rules/" rule-id)}))
             :location ::location))

      (ANY "/v1/classified/nodes/:node-name" [node-name]
           (resource
             :allowed-methods [:get]
             :available-media-types ["application/json"]
             :exists? true
             :handle-ok (fn [ctx]
                          (let [node (merge {:name node-name}
                                            (storage/get-node db node-name))
                                rules (storage/get-rules db)
                                groups (mapcat (partial rules/apply-rule node) rules)
                                classes (->> groups
                                          (map (partial storage/get-group db))
                                          (mapcat :classes)
                                          (into {}))
                                parameters {}
                                environment "production"]
                            (assoc node
                                   :groups groups
                                   :classes classes
                                   :parameters parameters
                                   :environment environment)))))

      (route/not-found "Not found"))))
