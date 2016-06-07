(ns puppetlabs.pe-puppetdb-extensions.storage-test.clj
  (:require [puppetlabs.pe-puppetdb-extensions.storage :refer :all]
            [clojure.test :refer :all]))

(deftest string-bytea-conversion
  (testing "conversion from bytea to string"
    (let [s "abcdef"]
      (is (= s (bytea->string (string->bytea s)))))))
