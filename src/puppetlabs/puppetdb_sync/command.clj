(ns puppetlabs.puppetdb-sync.command
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.cli.export :as export]
            [puppetlabs.kitchensink.core :as kitchensink]
            [clojure.set :as set]
            [clj-http.util :refer [url-encode]]
            [fs.core :as fs]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.schema :as pls]
            [clj-time.core :refer [now]]
            [schema.core :as s]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.mq-listener :as mql]))

(def catalogs-endpoint "/v4/catalogs")
(def reports-endpoint "/v4/reports")
(def factsets-endpoint "/v4/factsets")

(defn query-db 
  [host host-port endpoint query]
  (let [{:keys [status body]}
        (->> (url-encode (json/generate-string query))
             (format "http://%s:%s%s?query=%s" host host-port endpoint)
             (client/get))]
    (if (= status 200) (json/parse-string body) [])))

(pls/defn-validated fetch-hashes :- #{String}
  [host host-port entity]
  (let [certname-field (if (= entity :catalogs) "name" "certname")
        endpoint (case entity
                   :factsets factsets-endpoint
                   :reports reports-endpoint
                   :catalogs catalogs-endpoint)]
    (->> ["extract" "hash" ["~" certname-field ".*"]]
         (query-db host host-port endpoint)
         (map #(get % "hash"))
         (into #{}))))

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
  [remote remote-port hash]
  (fn [response]
    {"command" "store report"
     "version" 4
     "payload" (-> response
                   (dissoc "hash" "receive-time")
                   (assoc "resource-events"
                          (export/events-for-report-hash
                            remote remote-port hash)))}))

(defn transfer-response
  [local port payload]
  (let [url (format "http://%s:%s/v4/commands" local port)]
    (client/post url {:body               payload
                      :throw-exceptions   false
                      :content-type       :json
                      :character-encoding "utf-8"
                      :accept             :json})))

(defn query-hash-and-transfer!
  [remote remote-port local entity hash]
  (let [[transform endpoint]
        (case entity
          :factsets [transform-factset factsets-endpoint]
          :reports [(transform-report remote remote-port hash)
                    reports-endpoint]
          :catalogs [transform-catalog catalogs-endpoint]) 
        response (first (query-db remote remote-port endpoint ["=" "hash" hash]))]
    (transfer-response local 8080 (json/generate-string (transform response)))))


(pls/defn-validated puppetdb-sync*
  [local local-port
   {:keys [timestamp sender sender-port
           catalog-hashes report-hashes factset-hashes]}]
  (doseq [[entity hashes] [[:catalogs catalog-hashes]
                           [:factsets factset-hashes]
                           [:reports report-hashes]]]
    (->> (into #{} hashes)
         (remove (fetch-hashes local local-port entity))
         (map (partial query-hash-and-transfer! sender local sender-port entity)))))

(defn test-pdb-sync
  "syncs remote to local. remote must be running pdb-sync"
  [local local-port remote remote-port]
  (client/post url {:body (json/generate-string
                            {:command "sync"
                             :version 1
                             :payload {:timestamp (now)
                                       :catalog-hashes (fetch-hashes local local-port :catalogs)
                                       :factset-hashes (fetch-hashes local local-port :factsets)
                                       :report-hashes (fetch-hashes local local-port :reports)
                                       :sender local
                                       :sender-port local-port}})
                    :throw-exceptions   false
                    :content-type       :json
                    :character-encoding "utf-8"
                    :accept             :json}))
