(ns puppetlabs.puppetdb.http.population-test
  (:require [puppetlabs.puppetdb.examples.reports :refer [reports]]
            [puppetlabs.puppetdb.fixtures :as fixt]
            [clj-time.core :refer [now]]
            [clojure.test :refer :all]
            [puppetlabs.puppetdb.testutils :refer [get-request]]
            [puppetlabs.puppetdb.testutils.reports :refer [store-example-report!]]))

(def v1-pop-prefix "/metrics/v1/mbeans/puppetlabs.puppetdb.query.population:")

(use-fixtures :each fixt/with-test-db fixt/with-http-app)

(defn get-response
  [endpoint]
  (let [resp (fixt/*app* #spy/d (get-request endpoint nil))]
    (if (string? (:body resp))
      resp
      (update-in resp [:body] slurp))))

(deftest dashboard-metrics
  (let [num-nodes (str v1-pop-prefix "type=default,name=num-nodes")
        num-resources (str v1-pop-prefix "type=default,name=num-resources")
        pct-resource-dups (str v1-pop-prefix "type=default,name=pct-resource-dupes")
        avg-resources-node (str v1-pop-prefix "type=default,name=avg-resources-per-node")
        basic (:basic reports)]
    (store-example-report! basic (now))

  (testing "num-nodes"
    (let [response (get-response num-nodes)]
      (println "RESPONSE IS" response)
      (is (= response "foobar"))))

  (testing "num-resources"
    (let [response (get-response num-resources)]
      (println "RESPONSE IS" response)
      (is (= response "foobar"))))

  (testing "pct-resource-dups"
    (let [response (get-response pct-resource-dups)]
      (println "RESPONSE IS" response)
      (is (= response "foobar"))))

  (testing "avg-resources-node"
    (let [response (get-response avg-resources-node)]
      (println "RESPONSE IS" response)
      (is (= response "foobar"))))))
