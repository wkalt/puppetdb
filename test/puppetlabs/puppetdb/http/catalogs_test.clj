(ns puppetlabs.puppetdb.http.catalogs-test
  (:require [cheshire.core :as json]
            [puppetlabs.puppetdb.testutils.catalogs :as testcat]
            [puppetlabs.puppetdb.catalogs :as cats]
            [clojure.java.io :refer [resource reader]]
            [clojure.walk :refer [keywordize-keys]]
            [puppetlabs.puppetdb.scf.hash :as shash]
            [clojure.test :refer :all]
            [ring.mock.request :as request]
            [puppetlabs.puppetdb.testutils :refer [get-request deftestseq]]
            [puppetlabs.puppetdb.fixtures :as fixt]))

(def endpoints [[:v3 "/v3/catalogs"]
                [:v4 "/v4/catalogs"]])

(use-fixtures :each fixt/with-test-db fixt/with-http-app)

(def c-t "application/json")

(defn get-response
  ([endpoint]
     (get-response endpoint nil))
  ([endpoint node]
     (fixt/*app* (get-request (str endpoint "/" node))))
  ([endpoint node query]
   (fixt/*app* (get-request (str endpoint "/" node) query)))
  ([endpoint node query params]
   (fixt/*app* (get-request (str endpoint "/" node) query params))))


(def catalog1
  (-> (slurp (resource "puppetlabs/puppetdb/cli/export/tiny-catalog.json"))
      json/parse-string
      keywordize-keys))

(def catalog2 (merge catalog1
                 {:name "host2.localdomain"
                  :producer-timestamp "2010-07-10T22:33:54.781Z"
                  :transaction-uuid "000000000000000000000000000"
                  :environment "PROD"}))

(def queries
  {["=" "name" "myhost.localdomain"]
   [catalog1]

   ["=" "name" "host2.localdomain"]
   [catalog2]

   ["<" "producer-timestamp" "2014-07-10T22:33:54.781Z"]
   [catalog2]

   ["=" "environment" "PROD"]
   [catalog2]

   ["~" "environment" "PR"]
   [catalog2]

   []
   [catalog1 catalog2]})

(def paging-options
  {{:order-by (json/generate-string [{:field "environment"}])}
   [catalog1 catalog2]

   {:order-by (json/generate-string [{:field "producer-timestamp"}])}
   [catalog2 catalog1]

   {:order-by (json/generate-string [{:field "name"}])}
   [catalog2 catalog1]

   {:order-by (json/generate-string [{:field "transaction-uuid"}])}
   [catalog2 catalog1]

   {:order-by (json/generate-string [{:field "name" :order "desc"}])}
   [catalog1 catalog2]})

(defn extract-tags
  [xs]
  (sort (flatten (map :tags (flatten (map :resources xs))))))

(defn strip-hash
  [xs]
  (map #(dissoc % :hash) xs))

(deftestseq v3-catalog-queries
  [[version endpoint] [[:v3 "/v3/catalogs"]]
   :let [original-catalog-str (slurp (resource "puppetlabs/puppetdb/cli/export/big-catalog.json"))
         original-catalog     (json/parse-string original-catalog-str true)
         certname             (:name original-catalog)]]

  (testcat/replace-catalog original-catalog-str)
  (testing "it should return the catalog if it's present"
    (let [{:keys [status body] :as response} (get-response endpoint certname)
          result (json/parse-string body)]
      (is (= status 200))

      (let [original (testcat/munged-canonical->wire-format version original-catalog)
            result (testcat/munged-canonical->wire-format version (update-in
                                                                     (cats/parse-catalog body 3)
                                                                     [:resources] vals)) ]
        (is (= original result)))))
  
  (testing "catalog-not-found"
    (let [result (get-response endpoint "something-random.com")]
      (is (= 404 (:status result)))
      (is (re-find #"Could not find catalog" (-> (:body result)
                                                 (json/parse-string true)
                                                 :error))))))

(deftestseq v4-catalog-queries
  [[version endpoint] [[:v4 "/v4/catalogs"]]]
  (testcat/replace-catalog (json/generate-string catalog1))
  (testcat/replace-catalog (json/generate-string catalog2))
  (testing "v4 catalog endpoint is queryable"
    (doseq [q (keys queries)]
      (let [{:keys [status body] :as response} (get-response endpoint nil q)
            response-body (strip-hash (json/parse-stream (reader body) true))
            expected (get queries q)]
        (is (= (count expected) (count response-body)))
        (is (= (map :name expected) (map :name response-body)))
        (is (= (extract-tags expected) (extract-tags response-body))))))

  (testing "top-level extract works with catalogs"
    (let [query ["extract" ["name"] ["~" "name" ""]]
          {:keys [body]} (get-response endpoint nil query)
          response-body (strip-hash (json/parse-stream (reader body) true))
          expected [{:name "myhost.localdomain"}
                    {:name "host2.localdomain"}]]
      (is (= expected response-body))))

  (testing "v3 and v4 catalogs should have the same essential content"
    (doseq [certname ["myhost.localdomain" "host2.localdomain"]]
      (let [query ["=" "name" certname]
            {:keys [body]} (get-response "/v4/catalogs" nil query)
            v4-response-body (first (strip-hash (json/parse-stream (reader body) true)))
            {:keys [body]} (get-response "/v3/catalogs" certname)
            v3-response-body (-> (json/parse-string body)
                                 (keywordize-keys)
                                 :data)]
        (testing "contain the same resources"
          (is (= (shash/generic-identity-string (sort-by :line (:resources v3-response-body)))
                 (shash/generic-identity-string (sort-by :line (:resources v4-response-body))))))
        (testing "contain the same edges"
          (is (= (set (:edges v3-response-body))
                 (set (:edges v4-response-body))))))))

  (testing "paging options"
    (doseq [p (keys paging-options)]
      (testing (format "checking ordering %s" p)
      (let [{:keys [status body] :as response} (get-response endpoint nil nil p)
            response-body (strip-hash (json/parse-stream (reader body) true))
            expected (get paging-options p)]
        (is (= (map :name expected) (map :name response-body)))))))

  (testing "/v4 endpoint is still responsive to old-style node queries"
    (let [{:keys [body]} (get-response "/v4/catalogs" "myhost.localdomain")
          response-body  (json/parse-stream (reader body) true)]
      (is (= "myhost.localdomain" (:name response-body))))))
