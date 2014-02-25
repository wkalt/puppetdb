(ns puppetlabs.classifier.http
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [compojure.core :refer [routes context GET POST PUT ANY]]
            [compojure.route :as route]
            [liberator.core :refer [resource run-resource]]
            [liberator.representation :as liberator-representation]
            [schema.core :as sc]
            [puppetlabs.classifier.class-updater :as class-updater]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.storage.postgres :refer [foreign-key-violation-code]]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Group Node Rule Environment]]
            [puppetlabs.classifier.util :refer [->client-explanation merge-and-clean uuid?]])
  (:import com.fasterxml.jackson.core.JsonParseException
           java.util.UUID
           org.postgresql.util.PSQLException))

(def ^:private PuppetClass puppetlabs.classifier.schema/Class)


;; Exception-Catching Middleware
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn wrap-schema-fail-explanations
  "This wraps a ring handler that could throw a schema validation error. If such
  an error is thrown, this function catches it and produces a 400 response whose
  body is a JSON object describing the submitted object, the schema it failed to
  validate against, and a description of the validation error."
  [handler]
  (fn [request]
    (try (handler request)
      (catch clojure.lang.ExceptionInfo e
        ;; re-throw things that aren't schema validation errors
        (when-not (re-find #"does not match schema" (.getMessage e))
          (throw e))
        (let [{:keys [schema value error]} (.getData e)]
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (json/encode
                   {:submitted value
                    :schema (-> schema sc/explain ->client-explanation)
                    :error (->client-explanation error)})})))))

(def ^:private sql-foreign-key-violation-regex
  #"insert or update on table \"(\w+)\" violates foreign key constraint \"\w+\"\n\s*Detail: Key \([\w\s,]+\)=\(([\w\s,]+)\) is not present in table \"(\w+)\"")

(defn wrap-sql-foreign-key-fail-explanations
  [handler]
  (fn [request]
    (try (handler request)
      (catch PSQLException e
        (let [code (.getSQLState e)
              msg (.getMessage e)
              [_ resource-kind] (re-find #"^/v1/(class|group)" (:uri request))
              resp-msg (if-let [[match & groups] (re-find sql-foreign-key-violation-regex msg)]
                         (let [[into-table pkey foreign-table] groups
                               foreign-resource (-> foreign-table
                                                  (str/replace #"e?s$" "")
                                                  (str/replace "_" " "))]
                           (str "The " resource-kind " you tried to create refers to a "
                                foreign-resource " with primary key values (" pkey "), but no such "
                                foreign-resource " could be found."))
                         (str "The " resource-kind " you tried to create refers to a class or"
                              " parameter that could not be found."))]
          (when-not (= code foreign-key-violation-code)
            (throw e))
          {:status 412
           :headers {"Content-Type" "text/plain"}
           :body resp-msg})))))


;; Liberator Resources
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmethod liberator-representation/render-map-generic "application/json"
  [data _]
  (json/encode data))

(defmethod liberator-representation/render-seq-generic "application/json"
  [data _]
  (json/encode data))

(defn malformed-or-parse
  "Returns false (i.e. not malformed) for well-formed json string `body`, along
  with a map storing the parse result in ::data. Returns true (i.e. malformed)
  for `body` string that causes an exception to be thrown during parsing, along
  with a map storing the exception in ::error and the body as a string in
  ::request-body."
  [body]
  (try
    (if-let [data (json/decode body true)]
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

(sc/defn crd-resource
  "Create a basic CRD endpoint for a resource, given a storage object and a
  map of functions to create/retrieve/delete the resource."
  [resource-name :- String
   storage :- (sc/protocol storage/Storage)
   defaults :- {sc/Keyword sc/Any}
   {:keys [get create delete]} :- {:get (sc/pred fn?)
                                   :create (sc/pred fn?)
                                   :delete (sc/pred fn?)}]
  (let [exists? (fn [_]
                  (if-let [resource (get storage resource-name)]
                    {::retrieved resource}))
        put! (fn [ctx]
               (let [submitted (assoc (::data ctx {}) :name resource-name)
                     resource (merge defaults submitted)
                     inserted-resource (create storage resource)]
                 {::created inserted-resource}))
        delete! (fn [_] (delete storage resource-name))]
    (fn [request]
      (run-resource
        request
        {:allowed-methods [:get :put :delete]
         :respond-with-entity? (not= (:request-method request) :delete)
         :available-media-types ["application/json"]
         :exists? exists?
         :handle-ok ::retrieved
         :put! put!
         :new? ::created
         :handle-created ::created
         :delete! delete!
         :malformed? parse-if-body
         :handle-malformed (fn [ctx]
                             (format "Body is not valid JSON: %s"
                                     (::request-body ctx)))}))))

(sc/defn listing-resource
  [storage :- (sc/protocol storage/Storage)
   get-all :- (sc/pred fn?)]
  (resource
    :allowed-methods [:get]
    :available-media-types ["application/json"]
    :exists? (fn [_] {::retrieved (get-all storage)})
    :handle-ok ::retrieved))


;; Ring Handler
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn app [{:keys [db] :as config}]
  (-> (routes
        (context
          "/v1" []

          (GET "/nodes" []
               (listing-resource db storage/get-nodes))

          (ANY "/nodes/:node-name" [node-name]
               (crd-resource node-name db {}
                             {:get storage/get-node
                              :create storage/create-node
                              :delete storage/delete-node}))

          (GET "/environments" []
               (listing-resource db storage/get-environments))

          (context "/environments/:environment-name" [environment-name]
                   (ANY "/" []
                        (crd-resource environment-name db {}
                                      {:get storage/get-environment
                                       :create storage/create-environment
                                       :delete storage/delete-environment}))

                   (GET "/classes" []
                        (listing-resource db #(storage/get-classes % environment-name)))

                   (ANY "/classes/:class-name" [class-name]
                        (crd-resource class-name db
                                      {:environment environment-name}
                                      {:get #(storage/get-class %1 environment-name %2)
                                       :create storage/create-class
                                       :delete #(storage/delete-class %1 environment-name %2)})))

          (GET "/groups" []
               (listing-resource db storage/get-groups))

          (ANY "/groups/:group-name-or-uuid" [group-name-or-uuid]
               (let [uuid (if (uuid? group-name-or-uuid) (UUID/fromString group-name-or-uuid))
                     group-name (if-not uuid group-name-or-uuid)
                     exists? (fn [_]
                               {::retrieved (if uuid
                                              (storage/get-group-by-id db uuid)
                                              (storage/get-group-by-name db group-name))})
                     put! (fn [ctx]
                            (let [submitted (assoc (::data ctx {}) :name group-name)
                                  resource (merge {:environment "production",
                                                   :variables {}}
                                                  submitted)
                                  inserted-resource (storage/create-group db resource)]
                              {::created inserted-resource}))
                     post! (fn [ctx]
                             (let [submitted (if uuid
                                               (assoc (::data ctx {}) :id uuid)
                                               (assoc (::data ctx {}) :name group-name))]
                               {::updated (storage/update-group db submitted)}))
                     delete! (fn [_] (if uuid
                                       (storage/delete-group-by-id db uuid)
                                       (storage/delete-group-by-name db group-name)))]
                 (fn [req]
                   (run-resource
                     req
                     {:allowed-methods (if uuid
                                         [:post :get :delete]
                                         [:put :post :get :delete])
                      :respond-with-entity? (not= (:request-method req) :delete)
                      :available-media-types ["application/json"]
                      :exists? exists?
                      :handle-ok #(or (::updated %) (::retrieved %))
                      :put! put!
                      :post! post!
                      :new? ::created
                      :handle-created ::created
                      :delete! delete!
                      :malformed? parse-if-body
                      :handle-malformed (fn [ctx]
                                          (format "Body is not valid JSON: %s"
                                                  (::request-body ctx)))}))))

          (ANY "/rules" []
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

          (ANY "/classified/nodes/:node-name" [node-name]
               (resource
                 :allowed-methods [:get]
                 :available-media-types ["application/json"]
                 :exists? true
                 :handle-ok (fn [ctx]
                              (let [node {:name node-name}
                                    rules (storage/get-rules db)
                                    group-names (mapcat (partial rules/apply-rule node) rules)
                                    groups (map (partial storage/get-group-by-name db) group-names)
                                    classes (->> groups
                                                 (mapcat #(-> % :classes keys))
                                                 set)
                                    parameters (->> groups
                                                    (map :classes)
                                                    (apply merge))
                                    environments (set (map :environment groups))
                                    variables (apply merge (map :variables groups))]
                                (when-not (= (count environments) 1)
                                  (log/warn "Node" node-name "is classified into groups" group-names
                                            "with inconsistent environments" environments))
                                (assoc node
                                       :groups group-names
                                       :classes classes
                                       :parameters parameters
                                       :environment (first environments)
                                       :variables variables)))))
          (ANY "/update-classes" []
               (resource
                 :allowed-methods [:post]
                 :available-media-types ["application/json"]
                 :post! (fn [_]
                          (class-updater/update-classes! (:puppet-master config) db)))))

        (route/not-found "Not found"))

    wrap-schema-fail-explanations
    wrap-sql-foreign-key-fail-explanations))
