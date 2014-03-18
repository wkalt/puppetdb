(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [cheshire.core :refer [decode encode generate-string parse-string]]
            [compojure.core :as compojure]
            [ring.mock.request :as mock :refer [request]]
            [schema.test]
            [schema.core :as sc]
            [puppetlabs.classifier.http :refer :all]
            [puppetlabs.classifier.schema :refer [Group GroupDelta Node PuppetClass]]
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
        schema {:name String
                :property String}
        storage (reify Storage)  ; unused in the test, needed to satisfy schema
        !created? (atom false)
        storage-fns {:get (fn [_ obj-name]
                            (if (and (= obj-name test-obj-name) @!created?)
                              test-obj))
                     :create (fn [_ obj]
                               (reset! !created? true)
                               obj)
                     :delete (fn [_ obj-name])}
        app (compojure/routes
              (compojure/ANY "/objs/:obj-name" [obj-name]
                             (crd-resource storage sc/Any [obj-name] {:name obj-name} storage-fns)))]

    (testing "returns 404 when storage returns nil"
      (is-http-status 404 (app (request :get "/objs/nothing"))))

    (let [response (app (mock/body
                          (request :put "/objs/test-obj")
                          (generate-string test-obj)))]
      (testing "returns the 201 on creation"
        (is-http-status 201 response))

      (testing "returns the created object"
        (is (= (generate-string test-obj) (:body response)))))

    (testing "returns 200 with the object when it exists"
      (let [response (app (request :get "/objs/test-obj"))]
        (is-http-status 200 response)
        (is (= (generate-string test-obj) (:body response)))))))

(deftest nodes
  (let [empty-storage (reify Storage
                        (get-node [_ _] nil))
        app (app {:db empty-storage})]

    (testing "returns 404 when storage returns nil"
      (is-http-status 404 (app (node-request :get "addone")))))

  (let [nodes [{:name "bert"} {:name "ernie"}]
        node-map (into {} (for [n nodes] [(:name n) n]))
        !created? (atom {})
        mock-db (reify Storage
                  (get-node [_ node-name]
                    (if (get @!created? node-name)
                      (get node-map node-name)))
                  (get-nodes [_]
                    nodes)
                  (create-node [_ node]
                    (sc/validate Node node)
                    (is (= (get node-map (:name node)) node))
                    (swap! !created? assoc (:name node) true)
                    (get node-map (:name node)))
                  (delete-node [_ node-name]
                    (is (contains? node-map node-name))
                    '(1)))
        app (app {:db mock-db})]

    (testing "tells storage to create the node and returns 201 with the created object"
      (let [{body :body :as response} (app (node-request :put "bert"))]
        (is-http-status 201 response)
        (is (= (get node-map "bert") (decode body true)))))

    (testing "asks storage for the node and returns it if it exists"
      (let [{body :body :as response} (app (node-request :get "bert"))]
        (is-http-status 200 response)
        (is (= (get node-map "bert") (decode body true)))))

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
  (let [groups [{:name "default", :environment "production", :parent "default"
                 :rule {:when ["=" "nofact" "noval"]}, :classes {}, :variables {}}
                {:name "agroup"
                 :environment "bar"
                 :parent "default"
                 :rule {:when ["=" "name" "bert"]}
                 :classes {:foo {:param "override"}}
                 :variables {:ntp_servers ["0.us.pool.ntp.org" "ntp.example.com"]}}
                {:name "agroupprime"
                 :environment "bar"
                 :parent "default"
                 :rule {:when ["=" "name" "ernie"]}
                 :classes {:foo {}}
                 :variables {}}
                {:name "bgroup"
                 :environment "quux"
                 :parent "default"
                 :rule {:when ["=" "name" "elmo"]}
                 :classes {}
                 :variables {}}]
        classes [{:name "foo", :environment "bar", :parameters {:override "default"}}]
        group-map (into {} (map (juxt :name identity) groups))
        !created? (atom {})
        mock-db (reify Storage
                  (get-group-by-name [_ group-name]
                    (if (get @!created? group-name)
                      (get group-map group-name)))
                  (get-ancestors [_ group]
                    [])
                  (get-groups [_]
                    groups)
                  (create-group [_ group]
                    (sc/validate Group group)
                    (is (= (get group-map (:name group)) group))
                    (swap! !created? assoc (:name group) true)
                    (get group-map (:name group)))
                  (update-group [_ _]
                    (get group-map "agroupprime"))
                  (delete-group-by-name [_ group-name]
                    (is (contains? group-map group-name))
                    '(1))
                  (get-classes [_ _] classes))
        app (app {:db mock-db})]

    (testing "tells storage to create the group and returns 201 with the group object in its body"
      (let [req-body (encode (get group-map "agroup"))
            {body :body, :as resp} (app (group-request :put "agroup" req-body))]
        (is-http-status 201 resp)
        (is (= (get group-map "agroup") (decode body true)))))

    (testing "returns the group if it exists"
      (let [{body :body, :as resp} (app (group-request :get "agroup"))]
        (is-http-status 200 resp)
        (is (= (get group-map "agroup") (parse-string body true)))))

    (testing "returns all groups"
      (let [{body :body, :as resp} (app (group-request))]
      (app (group-request :put "bgroup" (encode (get group-map "bgroup"))))
        (is-http-status 200 resp)
        (is (= (set groups) (-> body (decode true) set)))))

    (testing "updates a group"
      (let [req-body (encode {:classes {:foo {:param nil}}
                              :variables {:ntp_servers nil}})
            {body :body, :as resp} (app (group-request :post "agroup" req-body))]
        (is-http-status 200 resp)
        (is (= (get group-map "agroupprime") (decode body true)))))

    (testing "tells storage to delete the group and returns 204"
      (let [response (app (group-request :delete "agroup"))]
        (is-http-status 204 response)))))

(defn class-request
  ([method env] (class-request method env nil nil))
  ([method env name] (class-request method env name nil))
  ([method env name body]
   (let [req (request method (str "/v1/environments/" env "/classes" (if name (str "/" name))))]
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
        !created? (atom {})
        mock-db (reify Storage
                  (get-class [_ _ class-name]
                    (if (get @!created? class-name)
                      (get class-map class-name)))
                  (get-classes [_ _]
                    classes)
                  (create-class [_ class]
                    (sc/validate PuppetClass class)
                    (is (= (get class-map (:name class)) class))
                    (swap! !created? assoc (:name class) true)
                    (get class-map (:name class)))
                  (delete-class [_ _ class-name]
                    (is (contains? class-map class-name))
                    '(1)))
        app (app {:db mock-db})]

    (testing "tells the storage layer to store the class map"
      (let [{body :body, :as resp} (app (class-request :put "test" "myclass"
                                                       (encode (get class-map "myclass"))))]
        (is-http-status 201 resp)
        (is (= (get class-map "myclass") (decode body true)))))

    (testing "returns class with its parameters"
      (let [{body :body, :as resp} (app (class-request :get "test" "myclass"))]
        (is-http-status 200 resp)
        (is (= (get class-map "myclass") (decode body true)))))

    (testing "retrieves all classes"
      (let [{body :body, :as resp} (app (class-request :get "test"))]
        (is-http-status 200 resp)
        (is (= (set classes) (set (decode body true))))))

    (testing "tells storage to delete the class and returns 204"
      (let [response (app (class-request :delete "test" "myclass"))]
        (is-http-status 204 response)))))

(deftest errors
  (let [incomplete-group {:classes ["foo" "bar" "baz"]}
        mock-db (reify Storage
                  (get-group-by-name [_ _]
                    nil)
                  (create-group [_ group]
                    (sc/validate Group group))
                  (get-ancestors [_ _]
                    []))
        app (app {:db mock-db})]
    (testing "bad requests get a structured 400 response."
      (let [resp (app (-> (request :put "/v1/groups/badgroup")
                        (mock/body (encode incomplete-group))))]
        (is-http-status 400 resp)
        (is (= "application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:kind :msg :details}
               (-> (decode (:body resp) true)
                 keys set)))))
    (testing "malformed requests get a structured 400 response."
      (let [bad-body "{\"haha\": [\"i'm broken\"})"
            resp (app (-> (request :put "/v1/groups/badgroup")
                        (mock/body bad-body)))
            err (decode (:body resp) true)]
        (is-http-status 400 resp)
        (is (re-find #"application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:kind :msg :details} (-> err keys set)))
        (is (= bad-body (-> err :details :body)))
        (is (re-find #"Unexpected close marker" (-> err :details :error)))))))
