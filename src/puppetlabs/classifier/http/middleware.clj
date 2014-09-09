(ns puppetlabs.classifier.http.middleware
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [postwalk]]
            [cheshire.generate :as gen]
            [clj-stacktrace.repl]
            [slingshot.slingshot :refer [try+]]
            [schema.core :as sc]
            [puppetlabs.rbac.services.http.middleware :refer [ssl?]]
            [puppetlabs.schema-tools :refer [explain-and-simplify-exception-data]]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [group->classification]]
            [puppetlabs.classifier.util :refer [encode to-sentence uuid?]
                                        :rename {encode encode-and-translate-keys}]))

(defn- log-exception
  "Log an exception including request method and URI"
  [request exception]
  (let [method (-> request :request-method name str/upper-case)
        message (str method " " (:uri request))]
    (log/warn message (clj-stacktrace.repl/pst-str exception))))

(defn wrap-schema-fail-explanations!
  "This wraps a ring handler that could throw a schema validation error. If such
  an error is thrown, this catches it and produces a 400 response whose body is
  a JSON object describing the submitted object, the schema it failed to
  validate against, and a description of the validation error.
  Note that this middleware has the side effect of registering a cheshire
  encoder for java.lang.Class"
  [handler]
  (gen/add-encoder java.lang.Class
                   (fn [c generator]
                     (.writeString generator (str c))))
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.http/user-data-invalid] exc-data
        (let [{:keys [schema value error]} (explain-and-simplify-exception-data exc-data)
              msg (-> (str "The object(s) in your request submitted did not conform to the schema."
                           " The problem is: " (seq error))
                    (str/replace #":rule \(not \(some \(check \% [^\)]+\) schemas\)\)"
                                 (str ":rule \"The rule is malformed. Please consult the group"
                                      " documentation for details on the rule grammar.\"")))]
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                     {:kind "schema-violation"
                      :msg msg
                      :details {:submitted value
                                :schema schema
                                :error error}})}))

      (catch clojure.lang.ExceptionInfo e
        ;; re-throw things that aren't schema validation errors
        (when-not (re-find #"does not match schema" (.getMessage e))
          (throw e))
        (log-exception request e)
        (let [{:keys [schema value error]} (explain-and-simplify-exception-data (.getData e))]
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                   (if (re-find #"Output of get-[a-z\-]*\*" (.getMessage e))
                     {:kind "database-corruption"
                      :msg (str "An object retrieved from the database did not conform to the"
                                " expected schema. This is indicative of either database"
                                " corruption, modification of the database by a third party, or a"
                                " bug on our part. Please open an issue at"
                                " https://tickets.puppetlabs.com so that we can investigate.")
                      :details {:retrieved value, :schema schema, :error error}}
                     {:kind "application-error"
                      :msg (str (.getMessage e) ". See `details` for full schema, value, error, and"
                                " stack trace.")
                      :details {:schema schema, :value value, :error error
                                :trace (->> (.getStackTrace e) (map #(.toString %)))}}))})))))

(defn wrap-error-catchall
  "Wrap the handler in a try/catch that swallows any throwable and returns a 500
  to the client with the throwable's error message and stack trace."
  [handler]
  (fn [request]
    (try (handler request)
      (catch Throwable e
        (let [root-e (if (instance? Iterable e)
                       (-> e seq last)
                       e)]
          (log-exception request e)
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                   {:kind "application-error"
                    :msg (str (.getMessage root-e) ". See `details` for the full stack trace")
                    :details {:trace (->> (.getStackTrace root-e) (map #(.toString %)))}})})))))


(defn- missing-referents-error-message
  [group-errors descendent-errors]
  (let [all-errors (concat group-errors descendent-errors)
        class-errors (filter #(= (:kind %) "missing-class") all-errors)
        parameter-errors (filter #(= (:kind %) "missing-parameter") all-errors)
        erroring-descendents (keys (group-by :group descendent-errors))
        referent->str (fn [{:keys [kind missing environment]}]
                        (str
                          (if (= kind "missing-parameter")
                            (let [[c p] missing]
                              (str "\"" (name c) "\" class's \"" (name p) "\" parameter"))
                            (str "\"" (name missing) "\" class"))
                          " in the \"" (name environment) "\" environment"))]
    (str
      (cond
        (and (empty? group-errors) (> (count erroring-descendents) 1))
        "Descendents of the group being edited make"

        (empty? group-errors)
        "A descendent of the group being edited makes"

        (empty? descendent-errors)
        "The group being edited or created makes"

        :else
        (str "The group being edited and " (count erroring-descendents) " of its descendents make"))

      " reference to the following missing "
      (cond
        (empty? class-errors) "class parameters"
        (empty? parameter-errors) "classes"
        :else "classes and class parameters")
      ": "
      (to-sentence (map referent->str (->> all-errors
                                        (map #(select-keys % [:kind :missing :environment]))
                                        distinct)))
      ". See the `details` key for complete information on where each reference to a missing "
      (cond
        (empty? class-errors) "class parameter"
        (empty? parameter-errors) "class"
        :else "class or class parameter")
      " originated.")))

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
        {:keys [tree ancestors]}
        (let [errors (flatten-errors tree (seq ancestors))
              group-err? #(= (:group %) (get-in tree [:group :name]))
              seggd-errs ((juxt (partial filter group-err?) (partial remove group-err?)) errors)]
          {:status 422
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                   {:details errors
                    :kind "missing-referents"
                    :msg (apply missing-referents-error-message seggd-errs)})})))))

(defn- pretty-cycle [groups]
  (->> (concat groups [(first groups)])
    (map :name)
    (str/join " -> ")))

(defn wrap-uniqueness-violation-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/uniqueness-violation]
        {:keys [entity-kind constraint fields values]}
        (let [conflict-description (->> (map #(str %1 " = " %2) fields values)
                                     (str/join ", "))
              conflict (zipmap fields values)
              msg (str "Could not complete the request because it violates a "
                       (name entity-kind) " uniqueness constraint. A " (name entity-kind)
                       " with " conflict-description " already exists.")]
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:kind "uniqueness-violation"
                  :msg msg
                  :details {:constraintName constraint
                            :conflict (zipmap fields values)}})})))))

(defn wrap-inheritance-fail-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier/inheritance-cycle]
        {:keys [cycle]}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:details cycle
                  :kind "inheritance-cycle"
                  :msg (str "Detected group inheritance cycle: "
                            (pretty-cycle cycle)
                            ". See the `details` key for the full groups of the cycle.")})}))))

(defn wrap-unreachable-groups-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier/unreachable-groups]
        {:keys [groups]}
        (let [plural? (> (count groups) 1)
              group-names (->> groups
                            (map :name)
                            (map #(str "\"" % "\""))
                            to-sentence)]
          {:status 422
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                   {:details groups
                    :kind "unreachable-groups"
                    :msg (str "The group" (if plural? "s") " named " group-names
                              (if plural? " are " " is ") "not reachable from the root of the"
                              " hierarchy.")})})))))

(defn wrap-missing-parent-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/missing-parent]
        {:keys [group]}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:details group
                  :kind "missing-parent"
                  :msg (str "The parent group "
                            (:parent group)
                            " does not exist.")})}))))

(defn wrap-classification-conflict-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.http/classification-conflict]
        {:keys [conflicts group->ancestors]}
        (let [details (class8n/explain-conflicts conflicts group->ancestors)
              wrap-quotes #(str "\"" % "\"")
              conflicting-things (str (if (contains? details :environment) "the environment, ")
                                      (if (contains? details :classes)
                                        (str "class parameters for "
                                             (->> (:classes details) keys
                                               (map (comp wrap-quotes name))
                                               to-sentence)
                                             " classes, "))
                                      (if (contains? details :variables)
                                        (str "variables named "
                                             (->> (:variables details)
                                               keys
                                               (map (comp wrap-quotes name))
                                               to-sentence))))
              group-names (->> group->ancestors
                            keys
                            (map (comp wrap-quotes :name))
                            to-sentence)
              msg (str "The node was classified into groups named " group-names
                       " that defined conflicting values for " conflicting-things "."
                       " See `details` for full information on all conflicts.")]
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                   {:kind "classification-conflict"
                    :msg msg
                    :details details})})))))

(defn wrap-root-rule-edit-explanation
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage/root-rule-edit]
        {delta :delta}
        (let [edits-besides-rule (dissoc delta :id :rule)
              error (if (empty? edits-besides-rule)
                      {:kind "root-rule-edit"
                       :msg (str "Changing the root group's rule is not permitted.")}
                      {:kind "root-rule-edit"
                       :msg (str "Changing the root group's rule is not permitted. None of your"
                                 " other edits were applied, but you may retry by re-submitting"
                                 " your delta less the `rule` key. The received delta can be"
                                 " found in this object's `details` key for convenience.")
                       :details delta})]
          {:status 422
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys error)})))))

(defn wrap-rule-translation-error-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.rules/illegal-puppetdb-query]
        {msg :msg, condition :condition}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:kind "untranslatable-rule"
                  :msg (str "The rule cannot be translated because " msg)
                  :details condition})}))))

(defn wrap-children-present-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/children-present]
        {:keys [group children]}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:kind "children-present"
                  :msg (str "The group cannot be deleted because it has children: "
                            (pr-str (mapv :name children)))
                  :details {:group group, :children children}})}))))

(defn wrap-unexpected-response-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.class-updater/unexpected-response]
        {response :response}
        {:status 500
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:kind "unexpected-response"
                  :msg (str "Received an unexpected "
                            (:status response)
                            " status response while trying to access "
                            (:url response))
                  :details (select-keys response [:url :status :headers :body])})}))))

(defn wrap-permission-denied-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.application.permissioned/permission-denied]
        {:keys [group-id description]}
        {:status 403
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:kind "permission-denied"
                  :msg (str "Sorry, but you don't have permission to " description
                            (if (and group-id (not (re-find #"^create this group" description)))
                              (str " for group " group-id " or any of its ancestors"))
                            ".")
                  :details nil})}))))

(defn wrap-errors-with-explanations
  [handler]
  "The standard set of middleware wrappers to use"
  (-> handler
    wrap-schema-fail-explanations!
    wrap-hierarchy-validation-fail-explanations
    wrap-uniqueness-violation-explanations
    wrap-inheritance-fail-explanations
    wrap-unreachable-groups-explanations
    wrap-missing-parent-explanations
    wrap-classification-conflict-explanations
    wrap-root-rule-edit-explanation
    wrap-rule-translation-error-explanations
    wrap-children-present-explanations
    wrap-unexpected-response-explanations
    wrap-permission-denied-explanations
    wrap-error-catchall))

(defn wrap-authn-errors
  "Middlware to handle authentication errors. This has to be wrapped around the
  handler *after* wrap-authenticated."
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.rbac/user-unauthenticated] {:keys [redirect]}
        (let [ssl-request? (ssl? request)]
          {:status (if ssl-request? 403 401)
           :headers {"Content-Type" "application/json"}
           :body (encode-and-translate-keys
                   (cond-> {:kind "user-unauthenticated"}
                     ssl-request?
                     (assoc :msg "The provided client ssl certificates are not trusted.")

                     (not ssl-request?)
                     (assoc :msg "No client ssl certificates were provided.")))}))
      (catch [:kind :puppetlabs.rbac/user-revoked] _
        {:status 403
         :headers {"Content-Type" "application/json"}
         :body (encode-and-translate-keys
                 {:kind "user-revoked"
                  :msg "This account has been revoked."})}))))
