(ns puppetlabs.puppetdb-sync.command
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.cli.export :as export]
            [puppetlabs.kitchensink.core :as kitchensink]
            [clojure.set :as set]
            [clj-http.util :refer [url-encode]]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.cheshire :as json]))

(def catalogs-endpoint "/v4/catalogs")
(def reports-endpoint "/v4/reports")
(def factsets-endpoint "/v4/factsets")

(defn query-db 
  [host endpoint query]
  (let [{:keys [status body]}
        (client/get (format "http://%s:8080%s?query=%s"
                            host endpoint (url-encode (json/generate-string query))))]
    (if (= status 200) (json/parse-string body) [])))

(defn fetch-hashes
  [host entity]
  (let [operator (if (= entity :catalogs) "name" "certname")
        endpoint (case entity
                        :factsets factsets-endpoint
                        :reports reports-endpoint
                        :catalogs catalogs-endpoint)]
    (into #{} (map #(get % "hash") (query-db host endpoint ["extract" ["hash"] ["~" operator ".*"]])))))

(defn transform-factset
  [response]
  {"command" "replace facts"
   "version" 3
   "payload" {"name" (get response "certname")
             "environment" (get response "environment")
             "producer-timestamp" (get response "producer-timestamp")
             "values" (get response "facts")}})

(defn transform-catalog
  [response]
  {"command" "replace catalog"
   "version" 5
   "payload" (dissoc response "hash")})

(defn transform-report
  [remote hash]
  (fn [response]
    {"command" "store report"
     "version" 4
     "payload" (-> response
                   (dissoc "hash" "receive-time")
                   (assoc "resource-events" (export/events-for-report-hash remote 8080 hash)))}))

(defn transfer-response
  [local port payload]
  (let [url (format "http://%s:%s/v4/commands" local port)]
    (client/post url {:body               payload
                      :throw-exceptions   false
                      :content-type       :json
                      :character-encoding "utf-8"
                      :accept             :json})))

(defn query-hash-and-transfer!
  [remote local entity hash]
  (let [[transform endpoint] (case entity
                               :factsets [transform-factset factsets-endpoint]
                               :reports [(transform-report remote hash) reports-endpoint]
                               :catalogs [transform-catalog catalogs-endpoint]) 
        response (first (query-db remote endpoint ["=" "hash" hash]))]
    (transfer-response local 8080 (json/generate-string (transform response)))))


(defn puppetdb-sync*
  [{:keys [timestamp sender catalog-hashes report-hashes factset-hashes]}]
  (let [catalog-targets (set/difference catalog-hashes (fetch-hashes local :catalogs))
        factset-targets (set/difference factset-hashes (fetch-hashes local :factsets))
        report-targets (set/difference report-hashes (fetch-hashes local :reports))]

    (doseq [h catalog-targets]
      (query-hash-and-transfer! sender local :catalogs h))

    (doseq [h report-targets]
      (query-hash-and-transfer! sender local :reports h))

    (doseq [h factset-targets]
      (query-hash-and-transfer! sender local :factsets h))))
