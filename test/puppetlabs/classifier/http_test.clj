(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.http :refer [app]]
            [ring.mock.request :refer [request]]
            [puppetlabs.classifier.storage :refer [Storage]]))

(deftest routes
  (testing "retrieves a node"
  (is (= ((app nil) (request :get "/v1/classified/nodes/testy"))
         {:status 200
          :headers {"content-type" "application/json"}
          :body "{\"name\":\"testy\",\"classes\":[\"foo\"],\"environment\":\"production\",\"parameters\":{}}"})))
  )

(def node-storage (ref #{}))
(def mock-db
  (reify
    Storage

    (create-node [this node] (dosync (alter node-storage conj node)))
    (get-node [this node] (if (contains? (deref node-storage) node) node nil))
    ))


(deftest add-nodes
  (testing "add one node"

    (is (= 200 (:status ((app mock-db) (request :put "/v1/nodes/addone")))))
    (is (= 200 (:status ((app mock-db) (request :get "/v1/nodes/addone")))))))
