(ns puppetlabs.classifier.http
  (:require [clojure.set :refer [rename-keys]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [compojure.core :refer [routes context GET POST PUT ANY]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [liberator.core :refer [resource run-resource]]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.kitchensink.core :refer [deep-merge]]
            [puppetlabs.classifier.application :refer [Classifier]]
            [puppetlabs.classifier.application.polymorphic :as app]
            [puppetlabs.classifier.class-updater :as class-updater]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.http.middleware :as middleware]
            [puppetlabs.classifier.rules :as rules]
            [puppetlabs.classifier.schema :refer [Environment Group GroupDelta group-delta
                                                  group->classification Node PuppetClass
                                                  SubmittedNode]]
            [puppetlabs.classifier.util :refer [uuid?]])
  (:import com.fasterxml.jackson.core.JsonParseException
           java.util.UUID))

;; Liberator Resources
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn handle-404
  [ctx]
  {:kind "not-found"
   :msg "The resource could not be found."
   :details (get-in ctx [:request :uri])})

(defn- err-with-rep
  "Necessary when returning errors early in liberator's decision chain (e.g.
  from `malformed?`) because it doesn't know how to render error responses until
  the `media-type-available?` decision has been executed"
  [err]
  (assoc err :representation {:media-type "application/json"}))

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
      [true (err-with-rep {::error e, ::request-body body})])))

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
  "Create a basic CRD endpoint for a resource, given an application instance and
  a map of functions to create/retrieve/delete the resource."
  [app :- (sc/protocol Classifier)
   token
   schema :- (sc/protocol sc/Schema)
   resource-path :- [String]
   attributes :- {sc/Keyword sc/Any}
   {:keys [get create delete]} :- {:get (sc/pred fn?)
                                   :create (sc/pred fn?)
                                   :delete (sc/pred fn?)}]
  (let [exists? (fn [_]
                  (if-let [resource (apply get app token resource-path)]
                    {::retrieved resource}))
        put! (fn [ctx]
               (let [resource (merge (::data ctx {}) attributes)
                     inserted-resource (create app token (validate schema resource))]
                 {::created inserted-resource}))
        delete! (fn [_] (apply delete app token resource-path))
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
         :handle-not-found handle-404
         :handle-ok ::retrieved
         :put-to-existing? (partial not-if-exists request)
         :put! put!
         :new? ::created
         :handle-created ::created
         :delete! delete!
         :malformed? parse-if-body
         :handle-malformed handle-malformed}))))

(sc/defn listing-resource
  [app :- (sc/protocol Classifier)
   token
   get-all :- (sc/pred fn?)]
  (resource
    :allowed-methods [:get]
    :available-media-types ["application/json"]
    :exists? (fn [_] {::retrieved (get-all app token)})
    :handle-ok ::retrieved))

;; Liberator Group Resource Decisions & Actions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn handle-malformed-group
  "Checks the context for signs that a malformed group has been submitted;
  namely if there's a malformed UUID or if the id in the request's body differs
  from that in the request's URI. If none of these signs are found, pass the
  buck up to `handle-malformed`."
  [ctx]
  (let [{malformed-uuid ::malformed-uuid
         malformed-parent-uuid ::malformed-parent-uuid
         submitted-id ::submitted-id
         uri-id ::uri-id} ctx]
    (cond
      malformed-uuid
      {:kind "malformed-uuid"
       :msg (str "The group id in the request's URI is not a valid UUID.")
       :details malformed-uuid}

      malformed-parent-uuid
      {:kind "malformed-uuid"
       :msg (str "The group's parent id is not a valid UUID.")
       :details malformed-parent-uuid}

      (and submitted-id uri-id)
      {:kind "conflicting-ids"
       :msg (str "The group id submitted in the request body differs from the id."
                 " present in the URL's request.")
       :details {:submitted submitted-id, :fromUrl uri-id}}

      :otherwise (handle-malformed ctx))))

(defn convert-uuids
  "Takes a raw group representation straight from JSON decoding, and coerces
  fields that should be uuids into uuids so that it meets the Group schema."
  [{:keys [id parent] :as group}]
  (merge group
         (if (and (uuid? id) (string? id)) {:id (UUID/fromString id)})
         (if (and (uuid? parent) (string? parent)) {:parent (UUID/fromString parent)})))

(defn- hyphenate-group-keys
  [group]
  (rename-keys group {:environment_trumps :environment-trumps}))

(defn malformed-group?
  "Given a group uuid, produces a function that takes one argument (the
  liberator context) and parses the body of the request in the context if said
  body is non-empty, as with `parse-if-body`. After parsing, merge in default
  values and then validate that multiple IDs for the group match if given, and
  that the parent ID if supplied is actually a UUID. If so, stick the result
  in the context under the ::submitted key."
  [uuid]
  (fn [ctx]
    (let [parse-result (parse-if-body ctx)]
      ;; matches when parse-if-body successfully parsed a body
      (if (and (vector? parse-result) (false? (first parse-result)))
        (let [{{id :id :as data} ::data} (second parse-result)
              {:keys [parent] :as group} (cond-> data
                                           true convert-uuids
                                           uuid (assoc :id uuid)
                                           true hyphenate-group-keys)]
          (cond
            (and uuid id (not= (str uuid) (str id)))
            [true (err-with-rep {::submitted-id id, ::uri-id uuid})]

            (and (contains? group :parent) (not (uuid? parent)))
            [true (err-with-rep {::malformed-parent-uuid parent})]

            :else
            [false {::submitted group}]))
        ;; else (either no body, or a malformed one)
        parse-result))))

(def ^:private group-uri-regex
  #"/v1/groups/\p{XDigit}{8}-\p{XDigit}{4}-\p{XDigit}{4}-\p{XDigit}{4}-\p{XDigit}{12}")

(defn post-delta-update?
  "A predicate that determines whether the context represents a POST request to
  update a group with a delta."
  [{submitted ::submitted, {method :request-method, uri :uri} :request}]
  (boolean (and (= method :post)
                submitted
                (re-find group-uri-regex uri))))

(defn post-new-group?
  "A predicate that determines whether the context represents a POST request to
  create a new group."
  [{submitted ::submitted, {method :request-method, uri :uri} :request}]
  (boolean (and (= method :post)
                submitted
                (re-find #"/v1/groups$" uri))))

(defn put-group?
  "A predicate that determines whether the context represents a PUT request to
  submit a group wholesale."
  [{submitted ::submitted, {method :request-method, uri :uri} :request}]
  (boolean (and (= method :put)
                submitted
                (re-find group-uri-regex uri))))

(defn- submitting-overwrite?
  [{:as ctx, retrieved ::retrieved, submitted ::submitted}]
  (cond
    (and (post-delta-update? ctx) retrieved) true
    (and (put-group? ctx) retrieved (not= submitted retrieved)) true
    :else false))

(def group-defaults
  {:environment "production", :environment-trumps false, :variables {}})

(defn group-resource
  [app prefix token uuid-str]
  (let [uuid (if (uuid? uuid-str) (UUID/fromString uuid-str) uuid-str)
        malformed? (if (and uuid (not (uuid? uuid)))
                     (err-with-rep {::malformed-uuid uuid})
                     (malformed-group? uuid))
        exists? (fn [_]
                  (if-let [g (and uuid (app/get-group app token uuid))]
                    {::retrieved g}))
        delete! (fn [_] (app/delete-group app token uuid))
        post! (fn [{:as ctx, submitted ::submitted, retrieved ::retrieved}]
                (let [group (merge group-defaults submitted)]
                  (cond
                    (post-new-group? ctx)
                    (let [with-id (assoc group :id (UUID/randomUUID))]
                      {::created (app/create-group app token (validate Group with-id))})

                    (post-delta-update? ctx)
                    {::updated (app/update-group app token (validate GroupDelta submitted))}

                    (and (put-group? ctx) (not retrieved))
                    {::created (app/create-group app token (validate Group group))}

                    (and (put-group? ctx) retrieved (not= group retrieved))
                    (let [delta (group-delta retrieved (validate Group group))]
                      {::created (app/update-group app token (validate GroupDelta delta))}))))
        put! (fn [{submitted ::submitted}]
               (let [group (merge group-defaults submitted)]
                 {::created (app/create-group app token (validate Group group))}))
        redirect? (fn [{:as ctx, created ::created}]
                    (if (post-new-group? ctx)
                      {:location (str prefix "/v1/groups/" (:id created))}))]
    (fn [req]
      (run-resource
        req
        {:allowed-methods [:put :post :get :delete]
         :respond-with-entity? (not= (:request-method req) :delete)
         :available-media-types ["application/json"]
         :malformed? malformed?
         :exists? exists?
         :handle-not-found handle-404
         :handle-ok (fn [{updated ::updated, retrieved ::retrieved}] (or updated retrieved))
         :post-to-existing? submitting-overwrite?
         :can-post-to-missing? post-new-group?
         :put-to-existing? (constantly false)
         :put! put!
         :post! post!
         :post-redirect? redirect?
         :new? ::created
         :handle-created ::created
         :delete! delete!
         :handle-malformed handle-malformed-group}))))

;; Ring Handler
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn api-handler
  [app]
  (let [{:keys [api-prefix] :as config} (app/get-config app)]
    (-> (routes
          (context
            "/v1" []

            (GET "/nodes" [:as {token :shiro-subject}]
                 (listing-resource app token (fn [app token]
                                               (->> (app/get-nodes app token)
                                                 (map #(rename-keys % {:check-ins :check_ins}))))))

            (GET "/nodes/:node-name" [node-name :as {token :shiro-subject}]
                 (listing-resource
                   app, token
                   (fn [app token]
                     (let [check-ins (app/get-check-ins app token node-name)]
                       {:name node-name
                        :check_ins (map #(dissoc % :node) check-ins)}))))

            (GET "/classes" [:as {token :shiro-subject}]
                 (listing-resource app token app/get-all-classes))

            (GET "/environments" [:as {token :shiro-subject}]
                 (listing-resource app token app/get-environments))

            (context "/environments/:environment-name" [environment-name]
                     (ANY "/" [:as {token :shiro-subject}]
                          (crd-resource app
                                        token
                                        Environment
                                        [environment-name]
                                        {:name environment-name}
                                        {:get app/get-environment
                                         :create app/create-environment
                                         :delete app/delete-environment}))

                     (GET "/classes" [:as {token :shiro-subject}]
                          (listing-resource app, token
                                            (fn [app token]
                                              (app/get-classes app token environment-name))))

                     (ANY "/classes/:class-name" [class-name :as {token :shiro-subject}]
                          (crd-resource app
                                        token
                                        PuppetClass
                                        [environment-name class-name]
                                        {:name class-name
                                         :environment environment-name}
                                        {:get app/get-class
                                         :create app/create-class
                                         :delete app/delete-class})))

            (GET "/groups" [:as {token :shiro-subject}]
                 (listing-resource app token app/get-groups))

            (POST "/groups" [:as {token :shiro-subject}]
                  (group-resource app api-prefix token nil))

            (ANY "/groups/:uuid" [uuid inherited :as {token :shiro-subject}]
                 (if (or (nil? inherited) (= inherited "false") (= inherited "0"))
                   (group-resource app api-prefix token uuid)
                   (resource
                     :allowed-methods [:get]
                     :available-media-types ["application/json"]
                     :exists? (fn [_]
                                (if-let [g (and uuid
                                                (app/get-group-as-inherited
                                                  app token (UUID/fromString uuid)))]
                                  {::retrieved g}))
                     :handle-ok ::retrieved
                     :handle-not-found handle-404)))

            (POST "/import-hierarchy" [:as {token :shiro-subject}]
                  (let [import! (fn [{raw-groups ::data}]
                                  (let [groups (->> raw-groups
                                                 (map convert-uuids)
                                                 (map hyphenate-group-keys))]
                                    (app/import-hierarchy app token (validate [Group] groups))))]
                    (resource
                      :allowed-methods [:post]
                      :available-media-types ["application/json"]
                      :malformed? parse-if-body
                      :handle-malformed handle-malformed
                      :exists? false
                      :can-post-to-missing? true
                      :post! import!
                      :new? false
                      :respond-with-entity? false)))

            (ANY "/classified/nodes/:node-name" [node-name :as {token :shiro-subject}]
                 (resource
                   :allowed-methods [:get :post]
                   :available-media-types ["application/json"]
                   :exists? true
                   :new? false
                   :respond-with-entity? true
                   :malformed? parse-if-body
                   :handle-malformed handle-malformed
                   :handle-ok (fn [ctx]
                                (let [data (-> (::data ctx {})
                                             (rename-keys {:transaction_uuid :transaction-uuid}))
                                      uuid-str (:transaction-uuid data)
                                      transaction-uuid (if (uuid? uuid-str)
                                                         (UUID/fromString uuid-str))
                                      node (validate SubmittedNode
                                                     (-> data
                                                       (update-in [:fact] (fnil identity {}))
                                                       (update-in [:trusted] (fnil identity {}))
                                                       (assoc :name node-name)
                                                       (dissoc :transaction-uuid)))]
                                  (app/classify-node app token node transaction-uuid)))))

            (POST "/classified/nodes/:node-name/explanation" [node-name :as {token :shiro-subject}]
                  (resource
                    :allowed-methods [:post]
                    :available-media-types ["application/json"]
                    :malformed? parse-if-body
                    :handle-malformed handle-malformed
                    :exists? true
                    :new? false
                    :respond-with-entity? true
                    :handle-ok (fn [ctx]
                                 (let [node (validate SubmittedNode
                                                      (-> (::data ctx {})
                                                        (update-in [:fact] (fnil identity {}))
                                                        (update-in [:trusted] (fnil identity {}))
                                                        (assoc :name node-name)))]
                                   (app/explain-classification app token node)))))

            (POST "/rules/translate" [:as {token :shiro-subject}]
                  (resource
                    :allowed-methods [:post]
                    :available-media-types ["application/json"]
                    :malformed? parse-if-body
                    :handle-malformed handle-malformed
                    :exists? true
                    :new? false
                    :respond-with-entity? true
                    :handle-ok (fn [{data ::data}]
                                 (rules/condition->pdb-query data))))

            (ANY "/update-classes" [:as {token :shiro-subject}]
                 (resource
                   :allowed-methods [:post]
                   :available-media-types ["application/json"]
                   :post! (fn [_]
                            (class-updater/update-classes!
                              (select-keys config [:puppet-master :client-ssl-context])
                              app, token))))

            (GET "/last-class-update" [:as {token :shiro-subject}]
                 (resource
                   :allowed-methods [:get]
                   :available-media-types ["application/json"]
                   :exists? true
                   :handle-ok (fn [_]
                                {:last_update (app/get-last-sync app token)})))

            (POST "/validate/group" [:as {token :shiro-subject}]
                  (resource
                    {:allowed-methods [:post]
                     :respond-with-entity? true
                     :available-media-types ["application/json"]
                     :malformed? (malformed-group? nil)
                     :handle-malformed handle-malformed-group
                     :post-to-existing? false
                     :handle-ok ::validated
                     :exists? (fn [{submitted ::submitted}]
                                (let [group (validate Group (merge group-defaults
                                                                   {:id (UUID/randomUUID)}
                                                                   submitted))]
                                  {::validated (app/validate-group app token group)}))})))

          (ANY "*" [:as req]
               {:status 404
                :headers {"Content-Type" "application/json"}
                :body (json/encode (handle-404 {:request req}))}))

      middleware/wrap-errors-with-explanations
      handler/api)))
