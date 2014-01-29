(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :refer [decode encode generate-string parse-string]]
            [compojure.core :as compojure]
            [ring.mock.request :as mock :refer [request]]
            [schema.test]
            [schema.core :as sc]
            [puppetlabs.classifier.http :refer :all]
            [puppetlabs.classifier.storage :refer [Storage]]))

(defn is-http-status
  "Assert an http status code"
  [status response]
  (is (= status (:status response))))

(defn node-request
  ([] (node-request :get nil))
  ([method node]
   (request method (str "/v1/nodes" (if node (str "/" node))))))

(use-fixtures :once schema.test/validate-schemas)

(deftest crud
  (let [test-obj-name "test-obj"
        test-obj {:name "test-obj" :property "hello"}
        bad-obj {:name "bad" :property 3}
        defaults {:property "goodbye"}
        schema {:name String
                :property String}
        storage (reify Storage)  ; unused in the test, needed to satisfy schema
        storage-fns {:get (fn [_ obj-name]
                            (if (= obj-name test-obj-name) test-obj))
                     :create (fn [_ obj]
                               nil)
                     :delete (fn [_ obj-name])}
        app (compojure/routes
              (compojure/ANY "/objs/:obj-name" [obj-name]
                             (crud-resource obj-name schema storage defaults storage-fns)))]

    (testing "returns 404 when storage returns nil"
      (is-http-status 404 (app (request :get "/objs/nothing"))))

    (testing "returns 200 with the object when it exists"
      (let [response (app (request :get "/objs/test-obj"))]
        (is-http-status 200 response)
        (is (= (generate-string test-obj) (:body response)))))

    (let [response (app (mock/body
                          (request :put "/objs/test-obj")
                          (generate-string test-obj)))]
      (testing "returns the 201 on creation"
        (is-http-status 201 response))

      (testing "returns the created object"
        (is (= (generate-string test-obj) (:body response)))))))

(deftest nodes
  (let [empty-storage (reify Storage
                        (get-node [_ _] nil))]

    (testing "returns 404 when storage returns nil"
      (is-http-status 404 ((app empty-storage) (node-request :get "addone")))))

  (let [nodes [{:name "bert"} {:name "ernie"}]
        node-map (into {} (for [n nodes] [(:name n) n]))
        mock-db (reify Storage
                  (get-node [_ node-name]
                    (get node-map node-name))
                  (get-nodes [_]
                    nodes)
                  (create-node [_ node]
                    (is (= (get node-map (:name node)) node))
                    (get node-map node))
                  (delete-node [_ node-name]
                    (is (contains? node-map node-name))
                    '(1)))
        app (app mock-db)]

    (testing "asks storage for the node and returns it if it exists"
      (let [response (app (node-request :get "bert"))]
        (is-http-status 200 response)
        (is (= (get node-map "bert") (-> response :body (decode true))))))

    (testing "tells storage to create the node and returns 201"
      (is-http-status 201 (app (node-request :put "bert"))))

    (testing "returns all nodes"
      (let [response (app (node-request))]
        (is-http-status 200 response)
        (is (= (set nodes) (-> response :body (decode true) set)))))

    (testing "tells storage to delete the node and returns 204"
      (let [response (app (node-request :delete "bert"))]
        (is-http-status 204 response)))))

(defn group-request
  ([] (group-request :get nil nil))
  ([method group] (group-request method group nil))
  ([method group body]
   (let [req (request method (str "/v1/groups" (if group (str "/" group))))]
     (if body
       (mock/body req body)
       req))))

(deftest groups
  (let [groups [{:name "agroup"
                 :environment "bar"
                 :classes {:foo {:param "override"}}
                 :variables {:ntp_servers ["0.us.pool.ntp.org" "ntp.example.com"]}}
                {:name "bgroup"
                 :environment "quux"
                 :classes {}
                 :variables {}}]
        group-map (into {} (for [g groups] [(:name g) g]))
        mock-db (reify Storage
                  (get-group [_ group-name]
                    (get group-map group-name))
                  (get-groups [_]
                    groups)
                  (create-group [_ group]
                    (is (= (get group-map (:name group)) group))
                    (get group-map group))
                  (delete-group [_ group-name]
                    (is (contains? group-map group-name))
                    '(1)))
        group-creation-req (fn [group-name]
                             (group-request :put group-name
                                            (-> (get group-map group-name)
                                              (dissoc :name)
                                              encode)))
        app (app mock-db)]

    (testing "returns the group if it exists"
      (let [{body :body, :as resp} (app (group-request :get "agroup"))]
        (is-http-status 200 resp)
        (is (= (get group-map "agroup") (parse-string body true)))))

    (let [{body :body, :as resp} (app (group-creation-req "agroup"))]
      (testing "tells storage to create the group and returns 201"
        (is-http-status 201 resp))

      (testing "when creating the group returns the group as json"
        (is (= (get group-map "agroup") (parse-string body true)))))

    (testing "returns all groups"
      (app (group-creation-req "bgroup"))
      (let [{body :body, :as resp} (app (group-request))]
        (is-http-status 200 resp)
        (is (= (set groups) (-> body (decode true) set)))))

    (testing "tells storage to delete the group and returns 204"
      (let [response (app (group-request :delete "agroup"))]
        (is-http-status 204 response)))))

(defn class-request
  ([] (class-request :get nil nil))
  ([method name] (class-request method name nil))
  ([method name body]
   (let [req (request method (str "/v1/classes" (if name (str "/" name))))]
     (if body
       (mock/body req body)
       req))))

(deftest classes
  (let [classes [{:name "myclass",
                  :parameters {:sweetness "totes"
                               :radness "utterly"
                               :awesomeness-level "off the charts"}
                  :environment "test"}
                 {:name "theirclass"
                  :parameters {:dumbness "totally"
                               :stinkiness "definitely"
                               :looks-like-a-butt? "from some angles"}
                  :environment "test"}]
        class-map (into {} (for [c classes] [(:name c) c]))
        mock-db (reify Storage
                  (get-class [_ class-name]
                    (get class-map class-name))
                  (get-classes [_]
                    classes)
                  (create-class [_ class]
                    (is (= (get class-map (:name class)) class)))
                  (delete-class [_ class-name]
                    (is (contains? class-map class-name))
                    '(1)))
        app (app mock-db)]

    (testing "returns class with its parameters"
      (let [{body :body, :as resp} (app (class-request :get "myclass"))]
        (is-http-status 200 resp)
        (is (= (get class-map "myclass") (decode body true)))))

    (testing "tells the storage layer to store the class map"
      (let [{body :body, :as resp} (app (class-request :put "myclass"
                                                       (encode (get class-map "myclass"))))]
        (is-http-status 201 resp)
        (is (= (get class-map "myclass") (decode body true)))))

    (testing "retrieves all classes"
      (let [{body :body, :as resp} (app (class-request))]
        (is-http-status 200 resp)
        (is (= (set classes) (set (decode body true))))))

    (testing "tells storage to delete the class and returns 204"
      (let [response (app (class-request :delete "theirclass"))]
        (is-http-status 204 response)))))

(defn rule-request
  [method rule]
  (request method "/v1/rules" (generate-string rule)))

(deftest rules
  (let [simple-rule {:when ["=" "name" "foo"]
                     :groups ["food"]}
        rule-id 42
        mock-db (reify Storage
                  (get-node [_ _] nil)
                  (create-rule [_ rule]
                    rule-id)
                  (get-rules [_]
                    [simple-rule])
                  (get-group [_ _]
                    {:name "food"}))
        app (app mock-db)]
    (testing "returns a key when storing a rule"
      (let [response (app (rule-request :post simple-rule))]
        (println (:body response))
        (is-http-status 201 response)
        (is (= (str "/v1/rules/" rule-id)
               ((response :headers) "Location")))))
    (testing "classifies a node using the simple rule"
      (let [response (app (request :get "/v1/classified/nodes/foo"))]
        (is (= ["food"] ((parse-string (:body response)) "groups")))))))

(deftest errors
  (let [incomplete-group {:classes ["foo" "bar" "baz"]}
        mock-db (reify Storage
                  (get-group [_ _] nil))
        app (app mock-db)]
    (testing "bad requests get a structured 400 response."
      (let [resp (app (-> (request :put "/v1/groups/badgroup")
                        (mock/body (encode incomplete-group))))]
        (is-http-status 400 resp)
        (is (= "application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:submitted :schema :error}
               (-> (decode (:body resp) true)
                 keys set)))))))
