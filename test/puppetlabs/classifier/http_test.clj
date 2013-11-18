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
  (let [empty-storage (reify storage/Storage
                        (get-node [_ _] nil))]

    (testing "returns 404 when storage returns nil"
      (is (= 404 (:status ((app empty-storage) (request :get "/v1/nodes/addone")))))))

  (let [mock-db (reify storage/Storage
                   (get-node [_ node]
                     (is (= node "addone"))
                     "addone"))]
    (testing "asks storage for the node and returns 200 if it exists"
      (is (= 200 (:status ((app mock-db) (request :get "/v1/nodes/addone")))))))

  (let [mock-db (reify storage/Storage
                   (create-node [_ node]
                     (is (= node "addone"))))]
    (testing "tells storage to create the node and returns 201"
      (is (= 201 (:status ((app mock-db) (request :put "/v1/nodes/addone"))))))))
