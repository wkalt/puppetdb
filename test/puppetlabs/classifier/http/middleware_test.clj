(ns puppetlabs.classifier.http.middleware-test
  (:require [clojure.test :refer :all]
            [compojure.core :as compojure]
            [ring.mock.request :as mock :refer [request]]
            [slingshot.slingshot :refer [throw+]]
            [schema.core :as sc]
            [puppetlabs.trapperkeeper.testutils.logging :refer [with-test-logging
                                                                with-log-output
                                                                logs-matching]]
            [puppetlabs.classifier.http.middleware :refer :all]))

(sc/defn ^:always-validate schema-fn
  [map :- {:required String}]
  nil)

(deftest logging
  (let [routes (compojure/routes
                 (compojure/ANY "/500" [] (throw (RuntimeException. "oops")))
                 (compojure/ANY "/schema-fail" [] (schema-fn {}))
                 (compojure/ANY "/invalid-data" []
                                (throw+ {:kind :puppetlabs.classifier.http/user-data-invalid
                                         :schema {}, :submitted "oops", :error "bad"})))
        app (wrap-errors-with-explanations routes)]
    (testing "catchall wrapper logs exceptions"
      (with-test-logging
        (app (request :get "/500"))
        (is (logged? #"GET /500" :warn))))

    (testing "schema exceptions get logged"
      (with-test-logging
        (app (request :get "/schema-fail"))
        (is (logged? #"GET /schema-fail" :warn))))

    (testing "not all exceptions are logged"
      (with-log-output logs
        (app (request :get "/invalid-data"))
        (is (= 0 (count (logs-matching #"GET /invalid-data" @logs))))))))
