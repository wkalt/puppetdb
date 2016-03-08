(ns puppetlabs.puppetdb.admin-test
  (:require [clojure.test :refer :all]
            [puppetlabs.puppetdb.cli.services :refer :all]
            [puppetlabs.puppetdb.command :refer [enqueue-command]]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.export :as export]
            [puppetlabs.puppetdb.import :as import]
            [puppetlabs.puppetdb.anonymizer :as anon]
            [puppetlabs.trapperkeeper.app :as tk-app]
            [puppetlabs.puppetdb.testutils :as tu]
            [puppetlabs.puppetdb.testutils.catalogs :as tuc]
            [puppetlabs.puppetdb.testutils.reports :as tur]
            [puppetlabs.puppetdb.testutils.facts :as tuf]
            [puppetlabs.puppetdb.testutils.db :refer [*db*]]
            [puppetlabs.puppetdb.testutils.cli
             :refer [get-nodes get-catalogs get-factsets get-reports munge-tar-map
                     example-catalog example-catalog2
                     example-report example-facts example-certname
                     example-facts2
                     get-summary-stats]]
            [puppetlabs.puppetdb.testutils.tar :refer [tar->map]]
            [puppetlabs.puppetdb.testutils.services :as svc-utils]))

(use-fixtures :each tu/call-with-test-logging-silenced)

(deftest test-basic-roundtrip
  (let [export-out-file (tu/temp-file "export-test" ".tar.gz")]

    (svc-utils/call-with-single-quiet-pdb-instance
     (fn []
       (is (empty? (get-nodes)))

       (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                    "replace catalog" 8 example-catalog)
       (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                    "store report" 7 example-report)
       (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                    "replace facts" 4 example-facts)

       (is (= (tuc/munge-catalog example-catalog)
              (tuc/munge-catalog (get-catalogs example-certname))))
       (is (= [example-report] (get-reports example-certname)))
       (is (= (tuf/munge-facts example-facts)
              (tuf/munge-facts (get-factsets example-certname))))

       (let [query-fn (partial query (tk-app/get-service svc-utils/*server* :PuppetDBServer))]
         (export/export! export-out-file query-fn))))

    (svc-utils/call-with-single-quiet-pdb-instance
     (fn []
       (is (empty? (get-nodes)))

       (let [dispatcher (tk-app/get-service svc-utils/*server*
                                            :PuppetDBCommandDispatcher)
             submit-command-fn (partial enqueue-command dispatcher)]
         (import/import! export-out-file submit-command-fn))

       @(tu/block-until-results 100 (first (get-catalogs example-certname)))
       @(tu/block-until-results 100 (first (get-reports example-certname)))
       @(tu/block-until-results 100 (first (get-factsets example-certname)))

       (is (= (tuc/munge-catalog example-catalog)
              (tuc/munge-catalog (get-catalogs example-certname))))
       (is (= [example-report] (get-reports example-certname)))
       (is (= (tuf/munge-facts example-facts)
              (tuf/munge-facts (get-factsets example-certname))))))))

(deftest test-anonymized-export
  (doseq [profile (keys anon/anon-profiles)]
    (let [export-out-file (tu/temp-file "export-test" ".tar.gz")
          anon-out-file (tu/temp-file "anon-test" ".tar.gz")]

      (svc-utils/call-with-single-quiet-pdb-instance
       (fn []
         (is (empty? (get-nodes)))

         (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                      "replace catalog" 8 example-catalog)
         (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                      "store report" 7 example-report)
       (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                    "replace facts" 4 example-facts)

         (is (= (tuc/munge-catalog example-catalog)
                (tuc/munge-catalog (get-catalogs example-certname))))
         (is (= [example-report] (get-reports example-certname)))
         (is (= (tuf/munge-facts example-facts)
                (tuf/munge-facts (get-factsets example-certname))))

         (let [query-fn (partial query (tk-app/get-service svc-utils/*server* :PuppetDBServer))]
           (export/export! export-out-file query-fn)
           (export/export! anon-out-file query-fn profile)
           (let [export-out-map (munge-tar-map (tar->map export-out-file))
                 anon-out-map (munge-tar-map (tar->map anon-out-file))]
             (if (= profile "none")
               (is (= export-out-map anon-out-map))
               (is (not= export-out-map anon-out-map))))))))))

(deftest test-sample-statistics
  (svc-utils/call-with-single-quiet-pdb-instance
    (fn []
      (is (empty? (get-nodes)))

      (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                   "replace catalog" 8 example-catalog)
       (svc-utils/sync-command-post (svc-utils/pdb-cmd-url)
                                    "bar.com"
                                    "replace catalog" 8
                                    (assoc example-catalog2 :certname "bar.com"))

      (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                   "store report" 7 example-report)
      (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) example-certname
                                   "replace facts" 4 example-facts)
       (svc-utils/sync-command-post (svc-utils/pdb-cmd-url) "bar.com"
                                    "replace facts" 4
                                    (assoc example-facts2 :certname "bar.com"))

      (sutils/vacuum-analyze *db*)

      (let [summary-stats (get-summary-stats)
            quantile-fields [:string_fact_value_character_lengths
                             :structured_fact_value_character_lengths
                             :num_associated_factsets_over_fact_paths
                             :num_resources_per_file
                             :num_times_paths_values_shared_given_sharing
                             :num_unique_fact_values_over_nodes
                             :report_log_size_dist
                             :report_metric_size_dist]]
        (doseq [f quantile-fields]
          (let [quantiles (:quantiles (first (f summary-stats)))]
            (testing (format "metric %s has 21 quantiles" f)
              (is (= 21 (count quantiles))))
            (testing (format "metric %s quantiles increasing" f)
              (is (= quantiles (sort quantiles))))
            (testing (format "metric %s contains no nils" f)
              (every? (complement nil?) quantiles))))))))
