(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [cheshire.core :refer [parse-string generate-string]]
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

(defn is-http-status
  "Assert an http status code"
  [status response]
  (is (= status (:status response))))

(defn node-request
  [method node]
  (request method (str "/v1/nodes/" node)))

(deftest nodes
  (let [empty-storage (reify Storage
                        (get-node [_ _] nil))]

    (testing "returns 404 when storage returns nil"
      (is-http-status 404 ((app empty-storage) (node-request :get "addone")))))

  (let [mock-db (reify Storage
                   (get-node [_ node]
                     (is (= node "addone"))
                     "addone")
                   (create-node [_ node]
                     (is (= node "addone"))))]
    (testing "asks storage for the node and returns 200 if it exists"
      (is-http-status 200 ((app mock-db) (node-request :get "addone"))))

    (testing "tells storage to create the node and returns 201"
      (is-http-status 201 ((app mock-db) (node-request :put "addone"))))))

(defn group-request
  [method group]
  (request method (str "/v1/groups/" group)))

(deftest groups
  (let [mock-db (reify Storage
                   (get-group [_ group]
                     (is (= group "agroup"))
                     "agroup")
                   (create-group [_ group]
                     (is (= group "agroup"))))]
    (testing "asks storage for the group and returns 200 if it exists"
      (is-http-status 200 ((app mock-db) (group-request :get "agroup"))))

    (testing "returns the group name in json"
      (let [response ((app mock-db) (group-request :get "agroup"))]
        (is (= "agroup"
             ((parse-string (:body response)) "name")))))

    (testing "tells storage to create the group and returns 201"
      (is-http-status 201 ((app mock-db) (group-request :put "agroup"))))

    (testing "when creating the group returns the group as json"
      (let [response ((app mock-db) (group-request :put "agroup"))]
        (is (= {"name" "agroup"}
             (parse-string (:body response))))))))

(defn class-request
  ([method class-name]
   (request method (str "/v1/classes/" class-name)))
  ([method class-name body]
   (request method (str "/v1/classes/" class-name) (generate-string body))))

(deftest classes
  (let [myclass {:name "myclass",
                 :params {:param1 "value"}} 
        mock-db (reify Storage
                  (get-class [_ class-name] myclass)
                  (create-class [_ class]
                    (is (= class myclass))))]
    (testing "returns class with its parameters"
      (let [response ((app mock-db) (class-request :get "myclass"))]
        (is-http-status 200 response)
        (is (= {"name" "myclass"
                "params" {"param1" "value"}}
               (parse-string (:body response))))))

    (testing "tells the storage layer to store the class map"
      (let [response ((app mock-db) (class-request :put "myclass" myclass))]
        (is-http-status 201 response)))))
