(ns puppetlabs.classifier.http
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [compojure.core :refer [routes context GET POST PUT ANY]]
            [compojure.route :as route]
            [liberator.core :refer [resource run-resource]]
            [liberator.representation :as liberator-representation]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.classifier.class-updater :as class-updater]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.http.middleware :as middleware]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.storage :as storage]
            [puppetlabs.classifier.schema :refer [Environment Group GroupDelta group-delta
                                                  group->classification Node PuppetClass]]
            [puppetlabs.classifier.util :refer [uuid?]])
  (:import com.fasterxml.jackson.core.JsonParseException
           java.util.UUID))

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
      [true {::error e, ::request-body body
             :representation {:media-type "application/json"}}])))

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

(defn- validate
  [schema resource]
  (try (sc/validate schema resource)
    (catch clojure.lang.ExceptionInfo e
      (when-not (re-find #"does not match schema" (.getMessage e))
        (throw e))
      (let [{:keys [schema value error]} (.getData e)]
        (throw+ {:kind ::user-data-invalid
                 :schema schema
                 :value value
                 :error error})))))

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
                     inserted-resource (create storage (validate schema resource))]
                 {::created inserted-resource}))
        delete! (fn [_] (apply delete storage resource-path))
        ;; We override put-to-existing? to return false if the resource being put to already exists.
        ;; This causes liberator to just return 200 when this happens, rather than going down a part
        ;; of its decision graph that involves either a 409 response or another `put!` happening.
        not-if-exists (fn [request {submitted ::data, retrieved ::retrieved}]
                        (if (not= (:request-method request) :put)
                          false
                          (not= retrieved (merge submitted attributes))))]
    (fn [request]
      (run-resource
        request
        {:allowed-methods [:get :put :delete]
         :respond-with-entity? (not= (:request-method request) :delete)
         :available-media-types ["application/json"]
         :exists? exists?
         :handle-ok ::retrieved
         :put-to-existing? (partial not-if-exists request)
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
  values and stick the result in the context under the ::submitted-group key."
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
            :put [false {::submitted-group group}]
            :post [false {::delta group}]))
        ;; else (either no body, or a malformed one)
        parse-result))))

(defn- submitting-overwrite?
  [{retrieved ::retrieved, submitted ::submitted-group, delta ::delta
    {method :request-method} :request}]
  (cond
    (and (= method :post) retrieved delta)
    true

    (and (= method :put), retrieved, submitted
         (not= submitted (dissoc retrieved :id)))
    true

    :otherwise false))

(defn group-resource
  [db group-name-or-uuid]
  (let [uuid (if (uuid? group-name-or-uuid) (UUID/fromString group-name-or-uuid))
        group-name (if-not uuid group-name-or-uuid)
        exists? (fn [_]
                  (if-let [g (storage/get-group db uuid)]
                    {::retrieved g}))
        delete! (fn [_] (storage/delete-group db uuid))
        post! (fn [{delta ::delta, submitted ::submitted-group, retrieved ::retrieved}]
                (if delta
                  {::updated (storage/update-group db (validate GroupDelta delta))}
                  (let [delta (group-delta retrieved (validate Group submitted))]
                    {::created (storage/update-group db delta)})))
        ok (fn [{updated ::updated, retrieved ::retrieved}]
             (if-let [g (or updated retrieved)]
               (storage/annotate-group db g)))]
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
         :handle-ok ok
         :post-to-existing? submitting-overwrite?
         :can-post-to-missing? false
         :put-to-existing? (constantly false)
         :put! (fn [{submitted ::submitted-group}]
                 {::created (storage/create-group db (validate Group submitted))})
         :post! post!
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
                     (let [group (storage/get-group db gn)
                           ancestors (storage/get-ancestors db group)]
                       (class8n/collapse-to-inherited
                         (concat [(group->classification group)]
                                 (map group->classification ancestors)))))
          classes (->> class8ns
                       (map :classes)
                       (apply merge))
          environments (set (map :environment class8ns))
          parameters (apply merge (map :variables class8ns))]
      (when-not (= (count environments) 1)
        (log/warn "Node" node-name "is classified into groups"
                  group-names
                  "with inconsistent environments" environments))
      (assoc node
             :groups group-names
             :classes classes
             :parameters parameters
             :environment (first environments)))))

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
               (listing-resource db
                                 (fn [db]
                                   (->> (storage/get-groups db)
                                     (map (partial storage/annotate-group db))))))

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
                          (class-updater/update-classes! (select-keys config [:puppet-master :ssl-context]) db)))))

        (route/not-found "Not found"))

    middleware/wrap-schema-fail-explanations!
    middleware/wrap-hierarchy-validation-fail-explanations
    middleware/wrap-error-catchall))
