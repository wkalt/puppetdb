(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.http :refer [app]]
            [ring.mock.request :refer [request]]))

(deftest routes
  (testing "retrieves a node"
  (is (= (app (request :get "/v1/node/testy"))
         {:status 200
          :headers {"content-type" "text/plain"}
          :body "node: testy\nclass: Foo"}))))
