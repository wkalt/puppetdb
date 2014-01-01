(ns puppetlabs.classifier.http
  (:require [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :refer [generate-string parse-string]]
            [compojure.core :refer [routes GET PUT ANY]]
            [compojure.route :as route]
            [liberator.core :refer [resource run-resource]]
            [schema.core :as sc]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Group Node Rule]]))

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
    (catch Exception e ; FIXME: exception event horizon
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
       :handle-ok ::resource
       :put! (:put implementation)
       :handle-created ::resource
       :delete! (:delete implementation)
       :malformed? parse-if-body
       :handle-malformed (fn [ctx]
                           (format "Body is not valid JSON: %s"
                                   (::request-body ctx)))})))

(defn app [db]
  (wrap-schema-fail
    (routes
      (ANY "/v1/nodes/:node-name" [node-name]
           (classifier-resource
             {:get (fn [_]
                     (if-let [node (storage/get-node db node-name)]
                       {::resource node}))
              :put (fn [_]
                     (let [node (sc/validate Node {:name node-name})]
                       (storage/create-node db node)
                       {::resource node}))
              :delete (fn [_] (storage/delete-node db node-name))}))

      (ANY "/v1/groups/:group-name" [group-name]
           (classifier-resource
             {:get (fn [_]
                     (if-let [group (storage/get-group db group-name)]
                       {::resource group}))
              :put (fn [ctx] (let [group (-> (::data ctx {})
                                             (assoc :name group-name))]
                               (sc/validate Group group)
                               (storage/create-group db group)
                               {::resource group}))
              :delete (fn [_] (storage/delete-group db group-name))}))

      (ANY "/v1/classes/:class-name" [class-name]
           (classifier-resource
             {:get (fn [_]
                     (if-let [class (storage/get-class db class-name)]
                       {::resource class}))
              :put (fn [ctx] (let [class (-> (::data ctx {})
                                             (assoc :name class-name))]
                               (sc/validate PuppetClass class)
                               (storage/create-class db class)
                               {::resource class}))
              :delete (fn [_] (storage/delete-class db class-name))}))

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
                                             (mapcat :classes))]
                            (assoc node :groups groups :classes classes)))))

      (route/not-found "Not found"))))
