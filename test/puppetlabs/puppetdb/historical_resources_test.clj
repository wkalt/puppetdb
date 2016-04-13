(ns puppetlabs.puppetdb.historical-resources-test
  (:require [clojure.java.jdbc :as sql]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.reports :as report]
            [puppetlabs.puppetdb.scf.hash :as shash]
            [puppetlabs.puppetdb.facts :as facts
             :refer [path->pathmap string-to-factpath value->valuemap]]
            [puppetlabs.puppetdb.schema :as pls :refer [defn-validated]]
            [puppetlabs.puppetdb.scf.migrate :as migrate]
            [clojure.walk :as walk]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.kitchensink.core :as kitchensink]
            [puppetlabs.puppetdb.testutils :as tu]
            [puppetlabs.puppetdb.testutils.db :refer [*db* with-test-db]]
            [metrics.histograms :refer [sample histogram]]
            [metrics.counters :as counters]
            [schema.core :as s]
            [puppetlabs.trapperkeeper.testutils.logging :as pllog]
            [clojure.string :as str]
            [puppetlabs.puppetdb.examples :refer [catalogs]]
            [puppetlabs.puppetdb.examples.reports :refer [reports]]
            [puppetlabs.puppetdb.testutils.reports :refer :all]
            [puppetlabs.puppetdb.testutils.events :refer :all]
            [puppetlabs.puppetdb.random :as random]
            [puppetlabs.puppetdb.scf.storage :refer :all]
            [clojure.test :refer :all]
            [clojure.math.combinatorics :refer [combinations subsets]]
            [clj-time.core :refer [ago from-now now days]]
            [clj-time.coerce :refer [to-timestamp to-string]]
            [puppetlabs.puppetdb.jdbc :as jdbc :refer [query-to-vec]]))

(def reference-time "2014-10-28T20:26:21.727Z")
(def previous-time "2014-10-26T20:26:21.727Z")


(def catalog (:basic catalogs))
(def certname (:certname catalog))
(def current-time (str (now)))

;; When only one db is needed.
(defmacro deftest-db [name & body]
  `(deftest ~name (with-test-db ~@body)))

(deftest-db historical-resources-persistence
  (testing "Persisted catalogs"
    (add-certname! certname)
    (store-historical-resources (assoc catalog :producer_timestamp current-time) nil)))

;; (testing "should contain proper catalog metadata"
;;   (is (= (query-to-vec ["SELECT certname, api_version, catalog_version, producer_timestamp FROM catalogs"])
;;          [{:certname certname :api_version 1 :catalog_version "123456789" :producer_timestamp (to-timestamp current-time)}])))

;; (testing "should contain a complete edges list"
;;   (is (= (query-to-vec [(str "SELECT r1.type as stype, r1.title as stitle, r2.type as ttype, r2.title as ttitle, e.type as etype "
;;                              "FROM edges e, catalog_resources r1, catalog_resources r2 "
;;                              "WHERE e.source=r1.resource AND e.target=r2.resource "
;;                              "ORDER BY r1.type, r1.title, r2.type, r2.title, e.type")])
;;          [{:stype "Class" :stitle "foobar" :ttype "File" :ttitle "/etc/foobar" :etype "contains"}
;;           {:stype "Class" :stitle "foobar" :ttype "File" :ttitle "/etc/foobar/baz" :etype "contains"}
;;           {:stype "File" :stitle "/etc/foobar" :ttype "File" :ttitle "/etc/foobar/baz" :etype "required-by"}])))

;; (testing "should contain a complete resources list"
;;   (is (= (query-to-vec ["SELECT type, title FROM catalog_resources ORDER BY type, title"])
;;          [{:type "Class" :title "foobar"}
;;           {:type "File" :title "/etc/foobar"}
;;           {:type "File" :title "/etc/foobar/baz"}]))

;;   (testing "properly associated with the host"
;;     (is (= (query-to-vec ["SELECT c.certname, cr.type, cr.title
;;                              FROM catalog_resources cr, certnames c
;;                              WHERE c.id=cr.certname_id
;;                              ORDER BY cr.type, cr.title"])
;;            [{:certname certname :type "Class" :title "foobar"}
;;             {:certname certname :type "File"  :title "/etc/foobar"}
;;             {:certname certname :type "File"  :title "/etc/foobar/baz"}])))

;;   (testing "with all parameters"
;;     (is (= (query-to-vec ["SELECT cr.type, cr.title, rp.name, rp.value FROM catalog_resources cr, resource_params rp WHERE rp.resource=cr.resource ORDER BY cr.type, cr.title, rp.name"])
;;            [{:type "File" :title "/etc/foobar" :name "ensure" :value (sutils/db-serialize "directory")}
;;             {:type "File" :title "/etc/foobar" :name "group" :value (sutils/db-serialize "root")}
;;             {:type "File" :title "/etc/foobar" :name "user" :value (sutils/db-serialize "root")}
;;             {:type "File" :title "/etc/foobar/baz" :name "ensure" :value (sutils/db-serialize "directory")}
;;             {:type "File" :title "/etc/foobar/baz" :name "group" :value (sutils/db-serialize "root")}
;;             {:type "File" :title "/etc/foobar/baz" :name "require" :value (sutils/db-serialize "File[/etc/foobar]")}
;;             {:type "File" :title "/etc/foobar/baz" :name "user" :value (sutils/db-serialize "root")}])))

;;   (testing "with all metadata"
;;     (let [result (query-to-vec ["SELECT cr.type, cr.title, cr.exported, cr.tags, cr.file, cr.line FROM catalog_resources cr ORDER BY cr.type, cr.title"])]
;;       (is (= (map #(assoc % :tags (sort (:tags %))) result)
;;              [{:type "Class" :title "foobar" :tags ["class" "foobar"] :exported false :file nil :line nil}
;;               {:type "File" :title "/etc/foobar" :tags ["class" "file" "foobar"] :exported false :file "/tmp/foo" :line 10}
;;               {:type "File" :title "/etc/foobar/baz" :tags ["class" "file" "foobar"] :exported false :file "/tmp/bar" :line 20}])))))


