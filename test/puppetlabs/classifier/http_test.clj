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

(defn is-http-status
  "Assert an http status code"
  [status response]
  (is (= status (:status response))))

(defn node-request
  [method node]
  (request method (str "/v1/nodes/" node)))

(deftest nodes
  (let [empty-storage (reify storage/Storage
                        (get-node [_ _] nil))]

    (testing "returns 404 when storage returns nil"
      (is-http-status 404 ((app empty-storage) (node-request :get "addone")))))

  (let [mock-db (reify storage/Storage
                   (get-node [_ node]
                     (is (= node "addone"))
                     "addone"))]
    (testing "asks storage for the node and returns 200 if it exists"
      (is-http-status 200 ((app mock-db) (node-request :get "addone")))))

  (let [mock-db (reify storage/Storage
                   (create-node [_ node]
                     (is (= node "addone"))))]
    (testing "tells storage to create the node and returns 201"
      (is-http-status 201 ((app mock-db) (node-request :put "addone"))))))
