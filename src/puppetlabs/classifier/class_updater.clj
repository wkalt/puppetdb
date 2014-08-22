(ns puppetlabs.classifier.class-updater
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [throw+ try+]]
            [puppetlabs.http.client.sync :as http]
            [puppetlabs.classifier.application :as app]))

(defn throw-unexpected-response
  [response body url]
  (throw+ {:kind ::unexpected-response
           :response (assoc response :body body :url url)}))

(defn get-environments
  [ssl-context puppet-origin]
  (let [search-url (str puppet-origin "/v2.0/environments")
        {:keys [status] :as response} (http/get search-url
                                                {:ssl-context ssl-context
                                                 :as :stream
                                                 :headers {"Accept" "application/json"}})
        body (slurp (:body response) :encoding "UTF-8")]

    (when-not (= 200 status)
      (throw-unexpected-response response body search-url))

    (-> body
      (json/decode true)
      :environments
      keys)))

(defn get-classes-for-environment
  [ssl-context puppet-origin environment]
  (let [environment (name environment)
        search-url (str puppet-origin "/" environment "/resource_types/*")
        {:keys [status] :as response} (http/get search-url
                                                {:ssl-context ssl-context
                                                 :as :stream
                                                 :headers {"Accept" "text/pson"}})
        body (slurp (:body response) :encoding "ISO-8859-1")]
    (condp = status
      200 (for [class (json/decode body true)]
            (-> class
              (->> (merge {:parameters {}}))
              (select-keys [:name :parameters])
              (assoc :environment environment)))

      404 nil

      (throw-unexpected-response response body search-url))))

(defn get-classes
  [ssl-context puppet-origin]
  (let [environments (get-environments ssl-context puppet-origin)]
    (->> (for [env environments]
           (get-classes-for-environment ssl-context puppet-origin env))
      (remove nil?))))

(defn update-classes!
  [{:keys [puppet-master ssl-context]} app]
  (let [start (time/now)
        env-name-comp (fn [[env1 name1]
                           [env2 name2]]
                        (if (not= env1 env2)
                          (compare env1 env2)
                          (compare name1 name2)))
        puppet-classes (-> (get-classes ssl-context puppet-master)
                         flatten)
        _ (app/synchronize-classes app puppet-classes)
        stop (time/now)]
    (log/info "Synchronized" (count puppet-classes) "classes from the Puppet Master in"
              (-> (time/interval start stop) time/in-seconds) "seconds")))

(defn update-classes-and-log-errors!
  [config app]
  (let [start (time/now)]
    (try+
      (update-classes! config app)
      (catch [:kind ::unexpected-response]
        {{:keys [body status url]} :response}
        (log/error "Received an unexpected" status "response when trying to synchronize classes"
                   "from the Puppet Master's REST interface at" url "The response is:"
                   (str "\"" body "\"")))
      (catch clojure.lang.ExceptionInfo e
        (if (re-find #"not \(instance\? javax\.net\.ssl\.SSLContext" (.getMessage e))
          (log/warn "Could not synchronize classes from the Puppet Master because SSL is not"
                    "configured in the classifier's configuration file")
          (log/error "Encountered an unexpected exception while trying to synchronize classes from"
                     "the Puppet Master:" (.getMessage e))))
      (catch Exception e
        (log/error "Encountered an unexpected exception while trying to synchronize classes from"
                   "the Puppet Master:" (.getMessage e))))))
