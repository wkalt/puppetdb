(ns puppetlabs.classifier.class-updater-test
  (:require [clojure.test :refer :all]
            [org.httpkit.fake :refer [with-fake-http]]
            [schema.test]
            [slingshot.test]
            [puppetlabs.classifier.class-updater :refer :all])
  (:import javax.net.ssl.SSLContext))

(use-fixtures :once schema.test/validate-schemas)

(deftest errors
  (let [origin "http://puppet"
        ssl-context (SSLContext/getDefault)]
    
    (testing "get-environments throws up unexpected responses"
      (with-fake-http ["http://puppet/v2.0/environments" {:status 400 :body "weird puppet error"}]
        (is (thrown+? [:kind :puppetlabs.classifier.class-updater/unexpected-response]
                      (get-environments ssl-context origin)))))
    
    (testing "get-classes-for-environment throws up unexpected responses"
      (with-fake-http ["http://puppet/env1/resource_types/*" {:status 400 :body "weird puppet error"}]
        (is (thrown+? [:kind :puppetlabs.classifier.class-updater/unexpected-response]
                      (get-classes-for-environment ssl-context origin "env1")))))

    (testing "get-classes-for-environment returns nil on 404"
      (with-fake-http ["http://puppet/env2/resource_types/*" {:status 404 :body "not found"}]
        (is (nil? (get-classes-for-environment ssl-context origin "env2")))))))
