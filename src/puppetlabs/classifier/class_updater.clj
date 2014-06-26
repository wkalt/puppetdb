(ns puppetlabs.classifier.class-updater
  (:require [cheshire.core :as json]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.http.client.sync :as http]
            [puppetlabs.classifier.storage :as storage]))

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
  [{:keys [puppet-master ssl-context]} db]
  (let [env-name-comp (fn [[env1 name1]
                           [env2 name2]]
                        (if (not= env1 env2)
                          (compare env1 env2)
                          (compare name1 name2)))
        puppet-classes (-> (get-classes ssl-context puppet-master)
                         flatten)]
    (storage/synchronize-classes db puppet-classes)))
