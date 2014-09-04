(ns com.puppetlabs.puppetdb.test.http.aggregate-event-counts
  (:require [com.puppetlabs.http :as pl-http]
            [cheshire.core :as json]
            [com.puppetlabs.puppetdb.fixtures :as fixt]
            [clojure.test :refer :all]
            [com.puppetlabs.puppetdb.examples.reports :refer :all]
            [clj-time.core :refer [now]]
            [com.puppetlabs.puppetdb.testutils :refer [assert-success! deftestseq]]
            [com.puppetlabs.puppetdb.testutils.event-counts :refer [get-response]]
            [com.puppetlabs.puppetdb.testutils.reports :refer [store-example-report!]]))

(def endpoints [[:v3 "/v3/aggregate-event-counts"]
                [:v4 "/v4/aggregate-event-counts"]])

(use-fixtures :each fixt/with-test-db fixt/with-http-app)

(deftestseq query-aggregate-event-counts
  [[version endpoint] endpoints]
  (println "testing" version endpoint)

  (store-example-report! (:basic reports) (now))

  (testing "summarize-by rejects unsupported values"
    (let [response  (get-response endpoint
                                  ["=" "certname" "foo.local"]
                                  "illegal-summarize-by"
                                  {} true)
          body      (get response :body "null")]
      (is (= (:status response) pl-http/status-bad-request))
      (is (re-find #"Unsupported value for 'summarize-by': 'illegal-summarize-by'" body))))

  (testing "count-by rejects unsupported values"
    (let [response  (get-response endpoint
                                  ["=" "certname" "foo.local"]
                                  "certname"
                                  {"count-by" "illegal-count-by"} true)
          body      (get response :body "null")]
      (is (= (:status response) pl-http/status-bad-request))
      (is (re-find #"Unsupported value for 'count-by': 'illegal-count-by'" body))))

  (testing "nontrivial query using all the optional parameters"
    (let [expected  {:successes 0
                     :failures 0
                     :noops 0
                     :skips 1
                     :total 1}
          response  (get-response endpoint
                                  ["or" ["=" "status" "success"] ["=" "status" "skipped"]]
                                  "containing-class"
                                  {"count-by"      "certname"
                                   "counts-filter" ["<" "successes" 1]})
          actual    (json/parse-string (:body response) true)]
      (is (= actual expected)))))

(deftestseq query-distinct-event-counts
  [[version endpoint] endpoints]

  (store-example-report! (:basic reports) (now))
  (store-example-report! (:basic3 reports) (now))
  (testing "should only count the most recent event for each resource"
    (let [expected  {:successes 1
                     :skips 1
                     :failures 1
                     :noops 0
                     :total 3}
          response  (get-response endpoint
                                  ["=" "certname" "foo.local"]
                                  "resource"
                                  {"distinct-resources" true
                                   "distinct-start-time" 0
                                   "distinct-end-time" (now)})]
      (assert-success! response)
      (is (= expected (json/parse-string (:body response) true))))))

(deftestseq query-with-environment
  [[version endpoint] endpoints
   :when (not= version :v3)]

  (store-example-report! (:basic reports) (now))
  (store-example-report! (assoc (:basic2 reports)
                           :certname "bar.local"
                           :environment "PROD") (now))
  (are [result query] (= result (-> (get-response endpoint
                                                  query
                                                  "resource"
                                                  {"distinct-resources" false
                                                   "distinct-start-time" 0
                                                   "distinct-end-time" (now)})
                                    :body
                                    (json/parse-string true)))
       {:successes 2
        :skips 1
        :failures 0
        :noops 0
        :total 3}
       ["=" "environment" "DEV"]

       {:successes 2
        :skips 1
        :failures 0
        :noops 0
        :total 3}
       ["~" "environment" "DE"]

       {:successes 3
        :skips 0
        :failures 0
        :noops 0
        :total 3}
       ["=" "environment" "PROD"]

       {:successes 3
        :skips 0
        :failures 0
        :noops 0
        :total 3}
       ["~" "environment" "PR"]

       {:successes 5
        :skips 1
        :failures 0
        :noops 0
        :total 6}
       ["~" "environment" "D"]

       {:successes 5
        :skips 1
        :failures 0
        :noops 0
        :total 6}
       ["OR"
        ["=" "environment" "DEV"]
        ["=" "environment" "PROD"]]))
