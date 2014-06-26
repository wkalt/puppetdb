(ns puppetlabs.classifier.class-updater-test
  (:require [clojure.test :refer :all]
            [org.httpkit.fake :refer [with-fake-http]]
            [schema.test]
            [slingshot.test]
            [puppetlabs.classifier.class-updater :refer :all])
  (:import java.io.ByteArrayInputStream
           javax.net.ssl.SSLContext))

(use-fixtures :once schema.test/validate-schemas)

(defn ->input-stream
  "Convert a string to an InputStream"
  [s encoding]
  (ByteArrayInputStream. (.getBytes s encoding)))

(deftest errors
  (let [origin "http://puppet"
        ssl-context (SSLContext/getDefault)]
    
    (testing "get-environments throws up unexpected responses"
      (with-fake-http ["http://puppet/v2.0/environments"
                       {:status 400
                        :body (->input-stream "weird puppet error" "UTF-8")}]
        (is (thrown+? [:kind :puppetlabs.classifier.class-updater/unexpected-response]
                      (get-environments ssl-context origin)))))
    
    (testing "get-classes-for-environment throws up unexpected responses"
      (with-fake-http ["http://puppet/env1/resource_types/*"
                       {:status 400
                        :body (->input-stream "weird puppet error" "ISO-8859-1")}]
        (is (thrown+? [:kind :puppetlabs.classifier.class-updater/unexpected-response]
                      (get-classes-for-environment ssl-context origin "env1")))))

    (testing "get-classes-for-environment returns nil on 404"
      (with-fake-http ["http://puppet/env2/resource_types/*"
                       {:status 404
                        :body (->input-stream "not found" "ISO-8859-1")}]
        (is (nil? (get-classes-for-environment ssl-context origin "env2")))))))
