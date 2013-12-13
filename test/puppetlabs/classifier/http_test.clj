(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [cheshire.core :refer [parse-string generate-string]]
            [puppetlabs.classifier.http :refer [app]]
            [ring.mock.request :refer [request]]
            [puppetlabs.classifier.storage :refer [Storage]]))

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

  (let [test-node {:name "addone"}
        mock-db (reify Storage
                   (get-node [_ node]
                     (is (= node "addone"))
                     test-node)
                   (create-node [_ node]
                     (is (= node test-node))))
        app (app mock-db)]
    (testing "asks storage for the node and returns it if it exists"
      (let [response (app (node-request :get "addone"))]
        (is-http-status 200 response)
        (is (= (generate-string test-node) (:body response)))))

    (testing "tells storage to create the node and returns 201"
      (is-http-status 201 (app (node-request :put "addone"))))))

(defn group-request
  [method group]
  (request method (str "/v1/groups/" group)))

(deftest groups
  (let [test-group {:name "agroup"}
        mock-db (reify Storage
                   (get-group [_ group]
                     (is (= group "agroup"))
                     test-group)
                   (create-group [_ group]
                     (is (= group test-group))))
        app (app mock-db)]
    (testing "returns the group if it exists"
      (let [response (app (group-request :get "agroup"))]
        (is-http-status 200 (app (group-request :get "agroup")))
        (is (= (generate-string test-group) (:body response)))))

    (testing "tells storage to create the group and returns 201"
      (is-http-status 201 (app (group-request :put "agroup"))))

    (testing "when creating the group returns the group as json"
      (let [response (app (group-request :put "agroup"))]
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
                    (is (= class myclass))))
        app (app mock-db)]
    (testing "returns class with its parameters"
      (let [response (app (class-request :get "myclass"))]
        (is-http-status 200 response)
        (is (= (generate-string myclass) (:body response)))))

    (testing "tells the storage layer to store the class map"
      (let [response (app (class-request :put "myclass" myclass))]
        (is-http-status 201 response)
        (is (= (generate-string myclass) (:body response)))))))

(defn rule-request
  [method rule]
  (request method "/v1/rules" (generate-string rule)))

(deftest rules
  (let [simple-rule {:when ["=" "name" "foo"]
                     :groups ["food"]}
        rule-id "3kf8"
        mock-db (reify Storage
                  (get-node [_ _] nil)
                  (create-rule [_ rule]
                    rule-id)
                  (get-rules [_]
                    [simple-rule]))
        app (app mock-db)]
    (testing "returns a key when storing a rule"
      (let [response (app (rule-request :post simple-rule))]
        (is-http-status 201 response)
        (is (= (str "/v1/rules/" rule-id)
               ((response :headers) "Location")))))
    (testing "classifies a node using the simple rule"
      (let [response (app (request :get "/v1/classified/nodes/foo"))]
        (is (= ["food"] ((parse-string (:body response)) "groups")))))))
