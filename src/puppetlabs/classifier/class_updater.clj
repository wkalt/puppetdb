(ns puppetlabs.classifier.class-updater
  (:require [cheshire.core :as json]
            [puppetlabs.http.client.sync :as http]
            [puppetlabs.classifier.storage :as storage]))

(defn get-environments
  [ssl-context puppet-origin]
  (let [search-url (str puppet-origin "/v2.0/environments")
        response (http/get search-url
                           {:ssl-context ssl-context
                            :as :stream
                            :headers {"Accept" "application/json"}})]
    (-> response
      :body
      (slurp :encoding "UTF-8")
      (json/decode true)
      :environments
      keys)))

(defn get-classes-for-environment
  [ssl-context puppet-origin environment]
  (let [environment (name environment)
        search-url (str puppet-origin "/" environment "/resource_types/*")
        pson-body (:body (http/get search-url
                                   {:ssl-context ssl-context
                                    :as :stream
                                    :headers {"Accept" "text/pson"}}))
        classes (-> pson-body
                  (slurp :encoding "ISO-8859-1")
                  (json/decode true))]
    (for [class classes]
      (-> class
        (->> (merge {:parameters {}}))
        (select-keys [:name :parameters])
        (assoc :environment environment)))))

(defn get-classes
  [ssl-context puppet-origin]
  (let [environments (get-environments ssl-context puppet-origin)]
    (for [env environments]
      (get-classes-for-environment ssl-context puppet-origin env))))

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
