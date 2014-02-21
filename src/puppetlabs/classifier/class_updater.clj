(ns puppetlabs.classifier.class-updater
  (:require [clj-http.client :as http]
            [puppetlabs.classifier.storage :as storage]))

(defn get-environments
  [puppet-origin]
  (let [search-url (str puppet-origin "/v2.0/environments")
        response (http/get search-url
                           {:as :json
                            :accept "application/json"
                            :character-encoding "UTF-8"
                            :insecure? true})]
    (-> response :body :environments keys)))

(defn get-classes-for-environment
  [puppet-origin environment]
  (let [environment (name environment)
        search-url (str puppet-origin "/" environment "/resource_types/*")
        pson-classes (:body (http/get search-url
                                      {:as :json
                                       ;; This endpoint uses legacy pson encoding
                                       ;; which is equivalent to json in ISO-8859-1
                                       :accept "text/pson"
                                       :character-encoding "ISO-8859-1"
                                       :insecure? true}))]
    (for [class pson-classes]
      (-> class
        (->> (merge {:parameters {}}))
        (select-keys [:name :parameters])
        (assoc :environment environment)))))

(defn get-classes
  [puppet-origin]
  (let [environments (get-environments puppet-origin)]
    (for [env environments]
      (get-classes-for-environment puppet-origin env))))

(defn update-classes!
  [puppet-origin db]
  (let [env-name-comp (fn [[env1 name1]
                           [env2 name2]]
                        (if (not= env1 env2)
                          (compare env1 env2)
                          (compare name1 name2)))
        puppet-classes (-> (get-classes puppet-origin)
                         flatten
                         (->> (sort-by (juxt :environment :name) env-name-comp)))]
    (storage/synchronize-classes db puppet-classes)))
