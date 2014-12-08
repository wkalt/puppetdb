(ns puppetlabs.puppetdb-sync.command-test
  (:require [puppetlabs.puppetdb-sync.command :as command]
            [clj.time.core :refer [now]]))

(use-fixtures :each with-test-db)

(def example-catalog
  {
  "producer-timestamp" (now)
  "name" "foo.com"
  "version" "3423453"
  "environment" "production"
  "transaction-uuid" "1234567890"
  "resources" [1 2 3]
  "edges" [3 2 1]})

(deftest puppetdb-sync
  
  (testing "transform factset"
    )
  )

