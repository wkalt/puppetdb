(ns puppetlabs.classifier.http.middleware
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [postwalk]]
            [cheshire.generate :as gen]
            [cheshire.core :as json]
            [clj-stacktrace.repl]
            [slingshot.slingshot :refer [try+]]
            [schema.core :as sc]
            [puppetlabs.classifier.classification :as class8n]
            [puppetlabs.classifier.schema :refer [group->classification]]
            [puppetlabs.classifier.util :refer [->client-explanation uuid?]]))

(defn- log-exception
  "Log an exception including request method and URI"
  [request exception]
  (let [method (-> request :request-method name str/upper-case)
        message (str method " " (:uri request))]
    (log/warn message (clj-stacktrace.repl/pst-str exception))))

(defn- dissoc-indices
  [v & is]
  (let [remove-index? (set is)]
    (->> (map vector v (range))
      (remove (comp remove-index? second))
      (map first)
      vec)))

(defn remove-passing-arguments
  [{:keys [schema value error] :as data}]
  (if-not (sequential? error)
    data
    (let [passing-arg-indices (->> (map vector error (range))
                                (filter (comp nil? first))
                                (map second))
          remove-passing #(apply dissoc-indices % passing-arg-indices)]
      {:schema (remove-passing schema)
       :value (remove-passing value)
       :error (remove-passing error)})))

(defn remove-credentialed-arguments
  [{:keys [schema value error] :as data}]
  (if-not (sequential? value)
    data
    (let [has-credentials? #(and (associative? %)
                                 (contains? % :db)
                                 (associative? (:db %))
                                 (contains? (:db %) :user)
                                 (contains? (:db %) :password))
          credentialed-arg-indices (->> (map vector value (range))
                                     (filter has-credentials?)
                                     (map second))
          remove-creds #(apply dissoc-indices % credentialed-arg-indices)]
      {:schema (remove-creds schema)
       :value (remove-creds value)
       :error (remove-creds error)})))

(defn- unwrap-if-length-one
  [x]
  (if (and (sequential? x) (= (count x) 1))
    (first x)
    x))

(defn remove-argument-annotations
  [{:keys [schema value error]}]
  (let [select-schema-or-error (fn [v]
                                 (if-not (sequential? v)
                                   v
                                   (nth v 1)))]
    {:value (unwrap-if-length-one value)
     :schema (->> schema
               (map select-schema-or-error)
               unwrap-if-length-one)
     :error (->> error
              (map select-schema-or-error)
              unwrap-if-length-one)}))

(defn simplify-argument-schema-exception-data
  "Simplifies the schema, value, and error exception data from a schema
  validation exception so that exceptions from schema.core/defn's argument
  schemas don't contain any extraneous information about arguments that didn't
  actually fail validation. It does this by looking at the error field and
  noting the indices where nil appears (signifying that that argument doesn't
  fail), and removing the values at those indices from all three exception data
  fields. Then, if only 1-vecs remain after the removal, their sole remaining
  value will be unwrapped.
  This prevents us leaking database credentials through validation exceptions
  from the postgres storage protocol function implementations, since the first
  argument of those always contains the credentials, but there is no schema for
  that argument so it will never fail to validate."
  [data]
  (-> data
    remove-passing-arguments, remove-credentialed-arguments, remove-argument-annotations))

(defn explain-schema-exception-data
  [{:keys [value schema error]}]
  {:value value
   :schema (-> schema sc/explain ->client-explanation)
   :error (->client-explanation error)})

(defn process-schema-exception-data
  "Turn the schema, value, and error data attached to a schema exception into
  their simplified explanations with any credential values removed, ready to be
  serialized to JSON."
  [{:keys [schema] :as data}]
  (let [explained (explain-schema-exception-data data)
        {schema-exp :schema
         error-exp :error
         value :value} (if (sequential? schema)
                         (simplify-argument-schema-exception-data explained)
                         explained)]
    {:schema schema-exp
     :value value
     :error error-exp}))

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
        (let [{:keys [schema value error]} (process-schema-exception-data exc-data)]
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (json/encode
                   {:kind "schema-violation"
                    :msg (str "The object you submitted does not conform to the schema. The problem"
                              " is: " error)
                    :details {:submitted value
                              :schema schema
                              :error error}})}))

      (catch clojure.lang.ExceptionInfo e
        ;; re-throw things that aren't schema validation errors
        (when-not (re-find #"does not match schema" (.getMessage e))
          (throw e))
        (log-exception request e)
        (let [{:keys [schema value error]} (process-schema-exception-data (.getData e))]
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (json/encode
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
           :body (json/encode
                   {:kind "application-error"
                    :msg (str (.getMessage root-e) ". See `details` for the full stack trace")
                    :details {:trace (->> (.getStackTrace root-e) (map #(.toString %)))}})})))))


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
          {:status 422
           :headers {"Content-Type" "application/json"}
           :body (json/encode {:details errors
                               :kind "missing-referents"
                               :msg (referent-error-message error-count
                                                            group-error-count
                                                            child-error-count)})})))))
(defn- pretty-cycle [groups]
  (->> (concat groups [(first groups)])
    (map :name)
    (str/join " -> ")))

(defn wrap-uniqueness-violation-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/uniqueness-violation]
        {:keys [entity-kind constraint fields values offender]}
        (let [conflict-description (->> (map #(str %1 " = " %2) fields values)
                                     (str/join ", "))
              conflict (zipmap fields values)
              msg (str "Could not complete the request because it violates a "
                       (name entity-kind) " uniqueness constraint. A " (name entity-kind)
                       " with " conflict-description " already exists.")]
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (json/encode {:kind "uniqueness-violation"
                             :msg msg
                             :details {:constraintName constraint
                                       :conflict (zipmap fields values)}})})))))

(defn wrap-inheritance-fail-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/inheritance-cycle]
        {:keys [cycle]}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (json/encode {:details cycle
                             :kind "inheritance-cycle"
                             :msg (str "Detected group inheritance cycle: "
                                       (pretty-cycle cycle)
                                       ". See the `details` key for the full groups of the cycle.")})}))))

(defn wrap-missing-parent-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.storage.postgres/missing-parent]
        {:keys [group]}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (json/encode {:details group
                             :kind "missing-parent"
                             :msg (str "The parent group "
                                       (:parent group)
                                       " does not exist.")})}))))

(defn conflict-details
  [path values group->ancestors]
  (let [class8n->group (into {} (for [[g ancs] group->ancestors]
                                  (let [class8n (class8n/collapse-to-inherited
                                                  (map group->classification (concat [g] ancs)))]
                                    [class8n g])))]
    (for [v values]
      (let [class8n (first (filter #(= (get-in % path) v) (keys class8n->group)))
            classified-group (class8n->group class8n)
            defining-group (->> (concat [classified-group] (group->ancestors classified-group))
                             (filter #(= v (get-in % path)))
                             first)]
        {:value v
         :from classified-group
         :defined-by defining-group}))))

(defn- dissoc-if-empty
  [m k]
  (if (empty? (get m k))
    (dissoc m k)
    m))

(defn wrap-classification-conflict-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.http/classification-conflict]
        {:keys [group->ancestors conflicts]}
        (let [g->as group->ancestors
              details {:environment (conflict-details [:environment] (:environment conflicts) g->as)
                       :classes (into {} (for [[c params] (:classes conflicts)]
                                           [c (into {} (for [[p vs] params]
                                                         [p (conflict-details [:classes c p]
                                                                              vs, g->as)]))]))
                       :variables (into {} (for [[v values] (:variables conflicts)]
                                             [v (conflict-details [:variables v] values g->as)]))}
              details (reduce dissoc-if-empty details [:environment :classes :variables])
              wrap-quotes #(str "\"" % "\"")
              conflicting-things (str (if (contains? details :environment) "the environment, ")
                                      (if (contains? details :classes)
                                        (str "class parameters for "
                                             (->> (:classes details) keys
                                               (map (comp wrap-quotes name))
                                               (str/join ", "))
                                             " classes, "))
                                      (if (contains? details :variables)
                                        (str "variables named "
                                             (->> (:variables details)
                                               keys
                                               (map (comp wrap-quotes name))
                                               (str/join ", ")))))
              msg (str "The node was classified into groups named "
                       (str/join ", "  (map (comp wrap-quotes :name) (keys group->ancestors)))
                       " that defined conflicting values for " conflicting-things "."
                       " See `details` for full information on all conflicts.")]
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (json/encode
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
           :body (json/encode error)})))))

(defn wrap-rule-translation-error-explanations
  [handler]
  (fn [request]
    (try+ (handler request)
      (catch [:kind :puppetlabs.classifier.rules/illegal-puppetdb-query]
        {msg :msg, condition :condition}
        {:status 422
         :headers {"Content-Type" "application/json"}
         :body (json/encode {:kind "untranslatable-rule"
                             :msg (str "The rule cannot be translated because " msg)
                             :details condition})}))))

(defn wrap-errors-with-explanations
  [handler]
  "The standard set of middleware wrappers to use"
  (-> handler
    wrap-schema-fail-explanations!
    wrap-hierarchy-validation-fail-explanations
    wrap-uniqueness-violation-explanations
    wrap-inheritance-fail-explanations
    wrap-missing-parent-explanations
    wrap-classification-conflict-explanations
    wrap-root-rule-edit-explanation
    wrap-rule-translation-error-explanations
    wrap-error-catchall))
