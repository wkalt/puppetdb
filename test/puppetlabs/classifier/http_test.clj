(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.http :refer [app]]
            [ring.mock.request :refer [request]]
            [puppetlabs.classifier.storage :as storage]))

(deftest routes
  (testing "retrieves a node"
  (is (= ((app nil) (request :get "/v1/classified/nodes/testy"))
         {:status 200
          :headers {"content-type" "application/json"}
          :body "{\"name\":\"testy\",\"classes\":[\"foo\"],\"environment\":\"production\",\"parameters\":{}}"})))
  )

(deftest add-nodes
  (let [mock-db (storage/memory)]

  (testing "the node is not present"
    (is (= 404 (:status ((app mock-db) (request :get "/v1/nodes/addone"))))))

  (testing "add one node"

    (is (= 201 (:status ((app mock-db) (request :put "/v1/nodes/addone")))))
    (is (= 200 (:status ((app mock-db) (request :get "/v1/nodes/addone"))))))))
