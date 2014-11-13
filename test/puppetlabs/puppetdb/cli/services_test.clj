(ns puppetlabs.puppetdb.cli.services-test
  (:import [java.security KeyStore])
  (:require clojure.string
            [fs.core :as fs]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.version]
            [cheshire.core :as json]
            [puppetlabs.puppetdb.testutils.reports :as tur]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.fixtures :as fixt]
            [puppetlabs.puppetdb.testutils :as testutils]
            [puppetlabs.trapperkeeper.testutils.logging :refer [with-log-output logs-matching]]
            [puppetlabs.trapperkeeper.testutils.bootstrap :as tk]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :as jetty9]
            [puppetlabs.puppetdb.examples.reports :refer [reports]]
            [puppetlabs.puppetdb.cli.services :refer :all]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-time.core :refer [days hours minutes secs]]
            [clj-http.util :refer [url-encode]]  
            [clojure.java.io :refer [resource]]
            [puppetlabs.puppetdb.time :refer [to-secs to-minutes to-hours to-days period?]]
            [puppetlabs.puppetdb.testutils.jetty :as jutils]
            [puppetlabs.trapperkeeper.app :refer [get-service]]
            [puppetlabs.puppetdb.cli.import-export-roundtrip-test :refer [block-on-node
                                                                          submit-command
                                                                          block-until-results]]
            [clj-time.coerce :refer [to-string]]
            [clj-time.core :refer [now]]
            [puppetlabs.puppetdb.cli.export :as export]))

;(use-fixtures :each fixt/with-test-db fixt/with-http-app)

(deftest update-checking
  (testing "should check for updates if running as puppetdb"
    (with-redefs [puppetlabs.puppetdb.version/update-info (constantly {:version "0.0.0" :newer true})]
      (with-log-output log-output
        (maybe-check-for-updates "puppetdb" "update-server!" {})
        (is (= 1 (count (logs-matching #"Newer version 0.0.0 is available!" @log-output)))))))

  (testing "should skip the update check if running as pe-puppetdb"
    (with-log-output log-output
      (maybe-check-for-updates "pe-puppetdb" "update-server!" {})
      (is (= 1 (count (logs-matching #"Skipping update check on Puppet Enterprise" @log-output)))))))

(deftest whitelisting
  (testing "should log on reject"
    (let [wl (fs/temp-file)]
      (.deleteOnExit wl)
      (spit wl "foobar")
      (let [f (build-whitelist-authorizer (fs/absolute-path wl))]
        (is (true? (f {:ssl-client-cn "foobar"})))
        (with-log-output logz
          (is (false? (f {:ssl-client-cn "badguy"})))
          (is (= 1 (count (logs-matching #"^badguy rejected by certificate whitelist " @logz)))))))))

(deftest url-prefix-test
  (testing "should mount web app at `/` by default"
    (jutils/with-puppetdb-instance
      (let [response (client/get (jutils/current-url "/v4/version"))]
        (is (= 200 (:status response))))))
  (testing "should support mounting web app at alternate url prefix"
    (jutils/puppetdb-instance
     (assoc-in (jutils/create-config) [:global :url-prefix] "puppetdb")
     (fn []
       (let [response (client/get (jutils/current-url "/v4/version") {:throw-exceptions false})]
         (is (= 404 (:status response))))
       (let [response (client/get (jutils/current-url "/puppetdb/v4/version"))]
         (is (= 200 (:status response))))))))

;; need to know about factsets, catalogs, reports

(defn get-response
  [endpoint query]
  (let [resp (fixt/*app* (testutils/get-request endpoint query))]
    (if (string? (:body resp))
      resp
      (update-in resp [:body] slurp)))) 

(def queries
  [[:reports ["=" "certname" "foo.local"] #{{"blah" "blah"}}]
   [:facts ["=" "name" "bar"] #{{:value "the bar" :environment "DEV" :certname "foo.local" :name "bar"}}]])

(defn parse-response
  "The parsed JSON response body"
  [{:keys [status body]}]
  (when (= status 200)
    (seq (json/parse-string body true))))

#_ (query-via-puppdbserver-service)
(deftest query-via-puppdbserver-service
  (jutils/with-puppetdb-instance
    (submit-command :store-report 3 (tur/munge-example-report-for-storage (:basic reports)))
    (submit-command :replace-facts 3 {:name "foo.local"
                                      :environment "DEV"
                                      :values {:foo "the foo"
                                               :bar "the bar"
                                               :baz "the baz"}
                                      :producer-timestamp (to-string (now))})

    @(block-until-results 100 (export/facts-for-node "localhost" jutils/*port* "foo.local"))
    @(block-until-results 100 (export/reports-for-node "localhost" jutils/*port* "foo.local"))


    (let [pdb-service (get-service jutils/*server* :PuppetDBServer)
          results (atom nil)
          before-slurp? (atom nil)
          after-slurp? (atom nil)]

      (doseq [[endpoint q result] queries]
        (println "DEBUG")
        (println (parse-response (client/get (format "http://%s:%s/%s/reports?query=%s"
                                                     "localhost" jutils/*port* "v4" (url-encode (json/generate-string q)))
                                             {:accept :json})))
        (query pdb-service endpoint :v4 q (fn [f]
                                            (f
                                             (fn [result-set]
                                               (reset! before-slurp? (realized? result-set))
                                               (reset! results result-set)
                                               (reset! after-slurp? (realized? result-set))))))
        (is (false? @before-slurp?))
        (is (= result
               (set @results)))
        (is (false? @after-slurp?))))))
