(ns puppetlabs.puppetdb.testutils.cli
  (:require [clj-time.coerce :as time-coerce]
            [clj-time.core :as time]
            [clojure.string :as str]
            [clojure.walk :refer [keywordize-keys stringify-keys]]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.catalogs :as catalogs]
            [puppetlabs.puppetdb.examples :as examples]
            [puppetlabs.puppetdb.examples.reports :as examples-reports]
            [puppetlabs.puppetdb.factsets :as factsets]
            [puppetlabs.puppetdb.reports :as reports]
            [puppetlabs.puppetdb.testutils.reports :as tur]
            [puppetlabs.puppetdb.testutils.catalogs :as tuc]
            [puppetlabs.puppetdb.testutils.facts :as tuf]
            [puppetlabs.puppetdb.testutils.services :as svc-utils]
            [clojure.walk :as walk]))

(defn get-nodes []
  (svc-utils/get-json (svc-utils/pdb-query-url) "/nodes"))

(defn get-catalogs [certname]
  (-> (svc-utils/pdb-query-url)
      (svc-utils/get-catalogs certname)
      catalogs/catalogs-query->wire-v8
      vec))

(defn get-reports [certname]
  (-> (svc-utils/pdb-query-url)
      (svc-utils/get-reports certname)
      tur/munge-reports
      reports/reports-query->wire-v7
      vec))

(defn get-factsets [certname]
  (-> (svc-utils/pdb-query-url)
      (svc-utils/get-factsets certname)
      factsets/factsets-query->wire-v4
      vec))

(defn get-summary-stats []
  (-> (svc-utils/pdb-admin-url)
      (svc-utils/get-summary-stats)))

(def example-certname "foo.local")

(def example-facts
  {:certname example-certname
   :environment "DEV"
   :values {:foo "the foo"
            :bar "the bar"
            :baz "the baz"
            :biz {:a [3.14 2.71] :b "the b" :c [1 2 3] :d {:e nil}}}
   :producer_timestamp (time-coerce/to-string (time/now))})

(def example-facts2
  {:certname example-certname
   :environment "DEV"
   :values {:foo "the foo"
            :bar "the bar"
            :spam "eggs"
            :biz {:a [3.14 2.71] :b "the b" :c [1 2 3] :d {:e nil}}}
   :producer_timestamp (time-coerce/to-string (time/now))})

(def example-catalog
  (-> examples/wire-catalogs
      (get-in [8 :empty])
      (assoc :certname example-certname
             :producer_timestamp (time/now))))

(def example-catalog2
  (get-in examples/wire-catalogs [8 :basic]))

(def example-report
  (-> examples-reports/reports
      :basic
      (assoc :certname example-certname)
      tur/munge-report
      reports/report-query->wire-v7))

(defn munge-tar-map
  [tar-map]
  (-> tar-map
      (dissoc "export-metadata.json")
      (update "facts" tuf/munge-facts)
      (update "reports" (comp stringify-keys
                              tur/munge-report
                              keywordize-keys))
      (update "catalogs" tuc/munge-catalog)))
