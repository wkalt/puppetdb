(ns puppetlabs.puppetdb.query.population
  "Population-wide queries

   Contains queries and metrics that apply across an entire population."
  (:require [puppetlabs.puppetdb.jdbc :refer [query-to-vec
                                              table-count
                                              with-transacted-connection]]
            [puppetlabs.kitchensink.core :refer [quotient]]
            [metrics.gauges :refer [gauge]]
            [honeysql.core :as hcore]
            [honeysql.helpers :as hh]))

;;; Query library

(def ^:private from-all-resources-and-their-nodes
  (hcore/build :from [:catalogs :catalog_resources :certnames]
               :merge-where [:and
                             [:= :catalogs.id :catalog_resources.catalog_id]
                             [:= :certnames.certname :catalogs.certname]]))

(defn- where-nodes-are-active [q]
  (hh/merge-where q [:and
                     [:= :certnames.deactivated nil]
                     [:= :certnames.expired nil]]))

(defn- select-count [q]
  (-> q
      (hh/select [:%count.* :c])
      hcore/format
      query-to-vec
      first
      :c))

;;; Public

(defn num-resources
  "The number of resources in the population"
  []
  {:post [(number? %)]}
  (-> from-all-resources-and-their-nodes
      where-nodes-are-active
      select-count))

(defn num-nodes
  "The number of unique certnames in the population"
  []
  {:post [(number? %)]}
  (-> (hh/from :certnames)
      where-nodes-are-active
      select-count))

(defn avg-resource-per-node
  "The average number of resources per node"
  []
  {:post [(number? %)]}
  (quotient (num-resources) (num-nodes)))

(defn pct-resource-duplication
  "What percentage of resources in the population are duplicates"
  []
  {:post [(number? %)]}
  (let [distinct-resources-q (-> from-all-resources-and-their-nodes
                                 where-nodes-are-active
                                 (hh/select :catalog_resources.resource)
                                 (hh/modifiers :distinct))
        num-unique (-> (hh/from [distinct-resources-q :r])
                       select-count)
        num-total (num-resources)]
    (quotient (- num-total num-unique) num-total)))

(defn fact-value-duplication
  "Percentage of fact values shared between paths"
  []
  {:post [(number? %)]}
  (let [num-values (:count (first (query-to-vec "select count(*) from fact_values")))
        num-paths (:count (first (query-to-vec "select count(*) from fact_paths")))]
    (- 1 (/ num-values num-paths))))

;; ## Population-wide metrics

;; This is pinned to the old namespace for backwards compatibility
(def ns-str "puppetlabs.puppetdb.query.population")
(def metrics (atom nil))

(defn population-gauges
  "Create a set of gauges that calculate population-wide metrics"
  [db]
  {:num-resources          (gauge [ns-str "default" "num-resources"]
                                  (with-transacted-connection db
                                    (num-resources)))
   :num-nodes              (gauge [ns-str "default" "num-nodes"]
                                  (with-transacted-connection db
                                    (num-nodes)))
   :avg-resources-per-node (gauge [ns-str "default" "avg-resources-per-node"]
                                  (with-transacted-connection db
                                    (avg-resource-per-node)))
   :pct-fact-value-dupes (gauge [ns-str "default" "pct-fact-value-dupes"]
                                  (with-transacted-connection db
                                    (fact-value-duplication)))
   :pct-resource-dupes     (gauge [ns-str "default" "pct-resource-dupes"]
                                  (with-transacted-connection db
                                    (pct-resource-duplication)))})

(defn initialize-metrics
  "Initializes the set of population-wide metrics"
  [db]
  (compare-and-set! metrics nil (population-gauges db)))
