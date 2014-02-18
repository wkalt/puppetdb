(ns puppetlabs.classifier.util-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.walk :refer [postwalk]]
            [schema.core :as sc]
            [puppetlabs.classifier.schema]
            [puppetlabs.classifier.util :refer :all]))

(def ^:private PuppetClass puppetlabs.classifier.schema/Class)

(defn no-java-lang-prefixes?
  [explanation]
  (let [!prefixes? (atom false)
        walker (fn [x]
                 (when (and (symbol? x) (re-find #"^java\.lang\." (str x)))
                   (reset! !prefixes? true))
                 x)]
    (postwalk walker explanation)
    (not @!prefixes?)))

(defn no-keyword-symbols?
  [explanation]
  (let [!keyword-symbols? (atom false)
        walker (fn [x]
                 (when (= x 'Keyword)
                   (reset! !keyword-symbols? true))
                 x)]
    (postwalk walker explanation)
    (not @!keyword-symbols?)))

(deftest client-explanation-transformation
  (let [bad-class {:name "badclass"
                   :parameters {:good "false"}
                   :environment :production}
        explanations (try (sc/validate PuppetClass bad-class)
                       (catch clojure.lang.ExceptionInfo e
                         (let [{:keys [schema error]} (.getData e)]
                           {:schema (-> schema sc/explain ->client-explanation)
                            :error (->client-explanation error)})))
        {schema-explanation :schema, error-explanation :error} explanations]
    (testing "any java.lang. prefixes are stripped from symbols in the explanation"
      (is (no-java-lang-prefixes? schema-explanation))
      (is (no-java-lang-prefixes? error-explanation)))
    (testing "the 'Keyword symbol does not appear in the explanation"
      (is (no-keyword-symbols? schema-explanation))
      (is (no-keyword-symbols? error-explanation)))))

(deftest merge-and-clean-maps
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
