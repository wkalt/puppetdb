(ns puppetlabs.classifier.class-updater-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [slingshot.test]
            [puppetlabs.classifier.class-updater :refer :all]
            [puppetlabs.trapperkeeper.core :as tk]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :as jetty9]
            [puppetlabs.trapperkeeper.testutils.bootstrap :as testutils]
            [puppetlabs.trapperkeeper.testutils.logging :as testlogging])
  (:import java.io.ByteArrayInputStream
           javax.net.ssl.SSLContext))

(use-fixtures :once schema.test/validate-schemas)

(defn ->input-stream
  "Convert a string to an InputStream"
  [s encoding]
  (ByteArrayInputStream. (.getBytes s encoding)))

(defn environments-handler-400
  [req]
  {:status 400
   :body (->input-stream "weird puppet error" "UTF-8")})

(defn resource-types-handler-400
  [req]
  {:status 400
   :body (->input-stream "weird puppet error" "ISO-8859-1")})

(defn handler-404
  [req]
  {:status 404
   :body (->input-stream "not found" "ISO-8859-1")})

(tk/defservice test-web-service
  [[:WebserverService add-ring-handler]]
  (init [this context]
        (add-ring-handler environments-handler-400 "/v2.0/environments")
        (add-ring-handler resource-types-handler-400 "/env1/resource_types/*")
        (add-ring-handler handler-404 "/env2/resource_types/*")
        context))

(deftest errors
  (testlogging/with-test-logging
    (testutils/with-app-with-config app
      [jetty9/jetty9-service test-web-service]
      {:webserver {:port 18954}}

      (let [origin "http://localhost:18954"
            ssl-context (SSLContext/getDefault)]

        (testing "get-environments throws up unexpected responses"
          (is (thrown+? [:kind :puppetlabs.classifier.class-updater/unexpected-response]
                        (get-environments ssl-context origin))))

        (testing "get-classes-for-environment throws up unexpected responses"
          (is (thrown+? [:kind :puppetlabs.classifier.class-updater/unexpected-response]
                        (get-classes-for-environment ssl-context origin "env1"))))

        (testing "get-classes-for-environment returns nil on 404"
          (is (nil? (get-classes-for-environment ssl-context origin "env2"))))))))
