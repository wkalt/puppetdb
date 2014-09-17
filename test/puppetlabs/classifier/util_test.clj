(ns puppetlabs.classifier.util-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [puppetlabs.classifier.util :refer :all])
  (:import java.util.UUID))

(deftest ^:unit map-deltas
  (testing "a map delta"
    (let [m1 {:same "same", :rm'd "gone", :changed "old-val"
              :nested {:also-rm'd nil, :also-same "same", :also-changed "old-val"}}
          m2 {:same "same", :added "new", :changed "new-val"
              :nested {:also-same "same", :also-changed "new-val"}}
          delta (map-delta m1 m2)]
      (testing "doesn't contain unchanged keys"
        (is (not (contains? delta :same)))
        (is (= ::not-found (get-in delta [:nested :also-same] ::not-found))))
      (testing "maps removed keys to nil"
        (is (nil? (get delta :rm'd ::not-found)))
        (is (nil? (get-in delta [:nested :also-rm'd] ::not-found))))
      (testing "contains new values of changed keys"
        (is (= "new-val" (:changed delta)))
        (is (= "new-val" (get-in delta [:nested :also-changed]))))
      (testing "yields the second map when applied to first using merge-and-clean"
        (is (= m2 (merge-and-clean m1 delta)))))))

(deftest ^:unit merge-and-clean-maps
  (let [group {:name "agroup"
               :environment "production"
               :classes {:aclass {:verbose true, :log "info"}}
               :variables {}}
        rm-log-param {:classes {:aclass {:log nil}}}
        rm-aclass {:classes {:aclass nil}}
        change-env-add-param {:environment "space", :classes {:aclass {:silent false}}}]
    (testing "merges maps"
      (let [merged (merge-and-clean group change-env-add-param)]
        (is (= "space" (:environment merged)))
        (is (= {:verbose true, :log "info", :silent false}
               (get-in merged [:classes :aclass])))))
    (testing "remove entire nested keys"
      (is (= {} (:classes (merge-and-clean group rm-aclass)))))
    (testing "remove nested keys without disturbing siblings"
      (is (= {:aclass {:verbose true}} (:classes (merge-and-clean group rm-log-param)))))))

(deftest ^:unit relative-complement
  (let [seq-a '({:a "one" :b "ha"}
                {:a "two" :b "sdf"}
                {:a "three" :b "kjher"})
        seq-b '({:a "two" :b "234"}
                {:a "three" :b "123"}
                {:a "four" :b "7896"})]
    (testing "Gets the right complements for sorted seqs of maps"
      (is (= (relative-complements-by-key :a seq-a seq-b)
             [[{:a "one" :b "ha"}]
              [{:a "four" :b "7896"}]
              [[{:a "two" :b "sdf"} {:a "two" :b "234"}]
               [{:a "three" :b "kjher"} {:a "three" :b "123"}]]])))))

(deftest ^:unit uuid-handling
  (testing "nil is not a uuid" (is (false? (uuid? nil))))
  (testing "numbers are not UUIDs" (is (false? (uuid? 42))))
  (testing "strings that don't have UUIDS are not UUIDs" (is (false? (uuid? (str "not-uuid")))))
  (testing "UUID strings are UUIDs" (is (uuid? (str (UUID/randomUUID)))))
  (testing "UUID kewyords are UUIDs" (is (uuid? (-> (UUID/randomUUID) str keyword))))
  (testing "UUIDs are UUIDs" (is (uuid? (UUID/randomUUID))))

  (testing "nil does not convert to a UUID" (is (nil? (->uuid nil))))
  (testing "numbers do not convert to a UUID" (is (nil? (->uuid 42))))
  (testing "UUID strings do convert to a UUID" (is (->uuid (str (UUID/randomUUID)))))
  (testing "UUID keywords do convert to a UUID" (is (->uuid (-> (UUID/randomUUID) str keyword))))
  (testing "UUIDs do convert to a UUID" (is (->uuid (UUID/randomUUID)))))
