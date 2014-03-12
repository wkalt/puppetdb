(ns puppetlabs.classifier.http
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [cheshire.generate :as gen]
            [compojure.core :refer [routes context GET POST PUT ANY]]
            [compojure.route :as route]
            [liberator.core :refer [resource run-resource]]
            [liberator.representation :as liberator-representation]
            [schema.core :as sc]
            [schema.utils]
            [slingshot.slingshot :refer [try+]]
            [puppetlabs.kitchensink.core :refer [deep-merge]]
            [puppetlabs.classifier.class-updater :as class-updater]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.storage.postgres :refer [foreign-key-violation-code]]
            [puppetlabs.classifier.schema :refer [Environment Group GroupDelta group->classification
                                                  Node PuppetClass Rule]]
            [puppetlabs.classifier.util :refer [->client-explanation merge-and-clean uuid?]])
  (:import com.fasterxml.jackson.core.JsonParseException
           java.util.UUID
           org.postgresql.util.PSQLException))

;; Exception-Catching Middleware
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn wrap-schema-fail-explanations!
  "This wraps a ring handler that could throw a schema validation error. If such
  an error is thrown, this function catches it and produces a 400 response whose
  body is a JSON object describing the submitted object, the schema it failed to
  validate against, and a description of the validation error.
  Note that this middleware has the side effect of registering a cheshire
  encoder for java.lang.Class"
  [handler]
  (gen/add-encoder java.lang.Class
                   (fn [c generator]
                     (.writeString generator (str c))))
  (fn [request]
    (try (handler request)
      (catch clojure.lang.ExceptionInfo e
        ;; re-throw things that aren't schema validation errors
        (when-not (re-find #"does not match schema" (.getMessage e))
          (throw e))
        (let [{:keys [schema value error]} (.getData e)
              explained-error (->client-explanation error)]
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (json/encode
                   {:kind "schema-violation"
                    :msg (str "The object you submitted does not conform to the schema. The problem"
                              " is: " explained-error)
                    :details {:submitted value
                              :schema (-> schema sc/explain ->client-explanation)
                              :error explained-error}})})))))

(defn- referent-error-message
  [total-error-count group-error-count child-error-count]
  (let [prelude (fn [error-count]
                  (if (= error-count 1)
                    "A class or class parameter"
                    (str error-count " classes or class parameters")))
        do-or-does (fn [plurality]
                         (if (= plurality 1)
                           "does"
                           "do"))]
    (str
      (cond
        (zero? child-error-count)
        (str (prelude group-error-count)
             " that the group defines or inherits " (do-or-does group-error-count) " not exist in"
             " the group's environment.")

        (zero? group-error-count)
        (str (prelude child-error-count)
             " defined or inherited by the group's children " (do-or-does child-error-count)
             " not exist in the appropriate child's environment.")

        :otherwise
        (str (prelude group-error-count)
             " defined or inherited by the group and "
             (str/lower-case (prelude child-error-count))
             " defined or inherited by the group's children do not exist the appropriate"
             " environments."))
      " See the `details` key for a list of the specific errors.")))

(defn- flatten-errors
  [{:keys [group errors children]} ancestors]
  (let [full-chain (conj ancestors group)
        node-errors (for [[class missing-params] errors]
                      (if (nil? missing-params)
                        (let [definer (class8n/group-referencing-class class full-chain)]
                          {:kind "missing-class"
                           :group (:name group)
                           :missing class
                           :environment (:environment group)
                           :defined-by (:name definer)})
                        ;; else (class is there, but specific param missing)
                        (for [param missing-params]
                          (let [definer (class8n/group-referencing-parameter
                                          class param full-chain)]
                            {:kind "missing-parameter"
                             :group (:name group)
                             :missing [class param]
                             :environment (:environment group)
                             :defined-by (:name definer)}))))]
    (concat (flatten node-errors)
            (mapcat #(flatten-errors % (conj ancestors group)) children))))

(defn wrap-hierarchy-validation-fail-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/missing-referents]
        {:keys [tree ancestors classes]}
        (let [errors (flatten-errors tree (seq ancestors))
              error-count (count errors)
              group-error-count (->> errors
                                  (filter #(= (:group %) (get-in tree [:group :name])))
                                  count)
              child-error-count (- error-count group-error-count)
              inherited-errors (filter #(not= (:group %) (:defined-by %)) errors)]
          {:status (if (pos? (count inherited-errors)) 409 412)
           :headers {"Content-Type" "application/json"}
           :body (json/encode {:details errors
                               :kind "missing-referents"
                               :msg (referent-error-message error-count
                                                            group-error-count
                                                            child-error-count)})})))))

;; Liberator Resources
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmethod liberator-representation/render-map-generic "application/json"
  [data _]
  (json/encode data))

(defmethod liberator-representation/render-seq-generic "application/json"
  [data _]
  (json/encode data))

(defn handle-malformed
  [ctx]
  {:kind "malformed-request"
   :msg "The body of your request is not valid JSON."
   :details {:body (::request-body ctx)
             :error (.toString (::error ctx))}})

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
  [storage :- (sc/protocol storage/Storage)
   schema :- (sc/protocol sc/Schema)
   resource-path :- [String]
   attributes :- {sc/Keyword sc/Any}
   {:keys [get create delete]} :- {:get (sc/pred fn?)
                                   :create (sc/pred fn?)
                                   :delete (sc/pred fn?)}]
  (let [exists? (fn [_]
                  (if-let [resource (apply get storage resource-path)]
                    {::retrieved resource}))
        put! (fn [ctx]
               (let [resource (merge (::data ctx {}) attributes)
                     inserted-resource (create storage (sc/validate schema resource))]
                 {::created inserted-resource}))
        delete! (fn [_] (apply delete storage resource-path))]
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
         :handle-malformed handle-malformed}))))

(sc/defn listing-resource
  [storage :- (sc/protocol storage/Storage)
   get-all :- (sc/pred fn?)]
  (resource
    :allowed-methods [:get]
    :available-media-types ["application/json"]
    :exists? (fn [_] {::retrieved (get-all storage)})
    :handle-ok ::retrieved))

;; Liberator Group Resource Decisions & Actions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn malformed-group?
  "Given a group name and a uuid (only one of which need be non-nil), produces
  a function that takes one argument (the liberator context) and parses the body
  of the request in the context if said body is non-empty, as with
  `parse-if-body`. After parsing, merge in the group name and default attribute
  values and stick the result in the context under the ::group key."
  [group-name uuid]
  (fn [ctx]
    (let [parse-result (parse-if-body ctx)]
      ;; matches when parse-if-body successfully parsed a body
      (if (and (vector? parse-result) (not (first parse-result)))
        (let [{data ::data} (second parse-result)
              group (merge {:environment "production", :variables {}}
                           (if group-name
                             (assoc data :name group-name)
                             (assoc data :id uuid)))]
          (condp = (get-in ctx [:request :request-method])
            :put
            (do (sc/validate Group group)
                [false {::group group}])

            :post
            (do (sc/validate GroupDelta group)
                [false {::delta group}])))
        ;; else (either no body, or a malformed one)
        parse-result))))

(defn group-resource
  [db group-name-or-uuid]
  (let [uuid (if (uuid? group-name-or-uuid) (UUID/fromString group-name-or-uuid))
        group-name (if-not uuid group-name-or-uuid)
        exists? (fn [_]
                  {::retrieved (if uuid
                                 (storage/get-group-by-id db uuid)
                                 (storage/get-group-by-name db group-name))})
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
         :malformed? (malformed-group? group-name uuid)
         :exists? exists?
         :handle-ok #(or (::updated %) (::retrieved %))
         :put! (fn [ctx]
                 {::created (storage/create-group db (sc/validate Group (::group ctx)))})
         :post! (fn [ctx] {::updated (storage/update-group db (sc/validate GroupDelta
                                                                           (::delta ctx)))})
         :new? ::created
         :handle-created ::created
         :delete! delete!
         :handle-malformed handle-malformed}))))

;; Classification
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn classify-node
  [db node-name]
  (fn [ctx]
    (let [node {:name node-name}
          rules (storage/get-rules db)
          group-names (class8n/matching-groups node rules)
          class8ns (for [gn group-names]
                     (let [group (storage/get-group-by-name db gn)
                           ancestors (storage/get-ancestors db group)]
                       (class8n/collapse-to-inherited
                         (concat [(group->classification group)]
                                 (map group->classification ancestors)))))
          classes (->> class8ns
                    (mapcat #(-> % :classes keys))
                    set)
          parameters (->> class8ns
                       (map :classes)
                       (apply merge))
          environments (set (map :environment class8ns))
          variables (apply merge (map :variables class8ns))]
      (when-not (= (count environments) 1)
        (log/warn "Node" node-name "is classified into groups"
                  group-names
                  "with inconsistent environments" environments))
      (assoc node
             :groups group-names
             :classes classes
             :parameters parameters
             :environment (first environments)
             :variables variables))))

;; Ring Handler
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn app [{:keys [db] :as config}]
  (-> (routes
        (context
          "/v1" []

          (GET "/nodes" []
               (listing-resource db storage/get-nodes))

          (ANY "/nodes/:node-name" [node-name]
               (crd-resource db
                             Node
                             [node-name]
                             {:name node-name}
                             {:get storage/get-node
                              :create storage/create-node
                              :delete storage/delete-node}))

          (GET "/environments" []
               (listing-resource db storage/get-environments))

          (context "/environments/:environment-name" [environment-name]
                   (ANY "/" []
                        (crd-resource db
                                      Environment
                                      [environment-name]
                                      {:name environment-name}
                                      {:get storage/get-environment
                                       :create storage/create-environment
                                       :delete storage/delete-environment}))

                   (GET "/classes" []
                        (listing-resource db #(storage/get-classes % environment-name)))

                   (ANY "/classes/:class-name" [class-name]
                        (crd-resource db
                                      PuppetClass
                                      [environment-name class-name]
                                      {:name class-name
                                       :environment environment-name}
                                      {:get storage/get-class
                                       :create storage/create-class
                                       :delete storage/delete-class})))

          (GET "/groups" []
               (listing-resource db storage/get-groups))

          (ANY "/groups/:group-name-or-uuid" [group-name-or-uuid]
               (group-resource db group-name-or-uuid))

          (ANY "/classified/nodes/:node-name" [node-name]
               (resource
                 :allowed-methods [:get]
                 :available-media-types ["application/json"]
                 :exists? true
                 :handle-ok (classify-node db node-name)))

          (ANY "/update-classes" []
               (resource
                 :allowed-methods [:post]
                 :available-media-types ["application/json"]
                 :post! (fn [_]
                          (class-updater/update-classes! (:puppet-master config) db)))))

        (route/not-found "Not found"))

    wrap-schema-fail-explanations!
    wrap-hierarchy-validation-fail-explanations))
