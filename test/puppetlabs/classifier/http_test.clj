(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [cheshire.core :refer [decode encode generate-string parse-string]]
            [compojure.core :as compojure]
            [ring.mock.request :as mock :refer [request]]
            [schema.test]
            [schema.core :as sc]
            [puppetlabs.classifier.http :refer :all]
            [puppetlabs.classifier.schema :refer [Group GroupDelta Node PuppetClass]]
            [puppetlabs.classifier.storage :as storage :refer [Storage]]
            [puppetlabs.classifier.storage.postgres :refer [root-group-uuid]])
  (:import java.util.UUID))

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
                             (crd-resource storage, schema, [obj-name]
                                           {:name obj-name}, storage-fns)))]

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
  (let [annotated {:name "annotated"
                   :id (UUID/randomUUID)
                   :environment "production"
                   :parent root-group-uuid
                   :rule ["=" "name" "kermit"]
                   :variables {}
                   :classes {:foo {}
                             :baz {:buzz "37"}}}
        with-annotations (assoc annotated
                                :deleted {:foo {:puppetlabs.classifier/deleted true}
                                          :baz {:puppetlabs.classifier/deleted false
                                                :buzz {:puppetlabs.classifier/deleted true
                                                       :value "37"}}})
        agroup-id (UUID/randomUUID)
        agroup {:name "agroup"
                :id agroup-id
                :environment "bar"
                :parent root-group-uuid
                :rule ["=" "name" "bert"]
                :classes {:foo {:param "override"}}
                :variables {:ntp_servers ["0.us.pool.ntp.org" "ntp.example.com"]}}
        agroup' {:name "agroupprime"
                 :id agroup-id
                 :environment "bar"
                 :parent root-group-uuid
                 :rule ["=" "name" "ernie"]
                 :classes {:foo {}}
                 :variables {}}
        root {:name "default"
              :id root-group-uuid
              :environment "production"
              :parent root-group-uuid
              :rule ["=" "nofact" "noval"]
              :classes {}
              :variables {}}
        bgroup {:name "bgroup"
                :id (UUID/randomUUID)
                :environment "quux"
                :parent root-group-uuid
                :rule ["=" "name" "elmo"]
                :classes {}
                :variables {}}
        groups [root agroup annotated bgroup]
        classes [{:name "foo", :environment "bar", :parameters {:override "default"}}]
        !group-storage (atom {})
        !agroup-updated? (atom false)
        mock-db (reify Storage
                  (get-group [_ id]
                    (if (and (= id agroup-id) (get @!group-storage id))
                      (if @!agroup-updated? agroup' agroup)
                      (get @!group-storage id)))
                  (get-ancestors [_ group]
                    [])
                  (get-groups [_]
                    groups)
                  (annotate-group [_ group]
                    (if (= (:name group) "annotated")
                      with-annotations
                      group))
                  (create-group [_ group]
                    (sc/validate Group group)
                    (swap! !group-storage assoc (:id group) group)
                    group)
                  (update-group [_ _]
                    (reset! !agroup-updated? true)
                    agroup')
                  (delete-group [_ id]
                    (is (contains? @!group-storage id))
                    '(1))
                  (get-classes [_ _] classes))
        app (app {:db mock-db})]

    (testing "returns 404 to a GET request if the group doesn't exist"
      (let [{status :status} (app (group-request :get (UUID/randomUUID)))]
        (is (= 404 status))))

    (testing "returns 404 to a POST request if the group doesn't exist"
      (let [{status :status} (app (group-request :post, (UUID/randomUUID)
                                                 (encode {:environment "staging"})))]
        (is (= 404 status))))

    (testing "can create a group with a PUT to the URI"
      (let [{body :body, :as resp} (app (group-request :put (:id agroup) (encode agroup)))]
        (testing "and get a 201 Created response"
          (is-http-status 201 resp))
        (testing "and get the group back in the response body"
          (is (= agroup (-> body (decode true) convert-uuids))))))

    (testing "can create a group with a POST to the group collection"
      (let [{:keys [status headers]} (app (group-request :post nil (encode (dissoc bgroup :id))))]
        (testing "and get a 303 back with the group's location"
          (is (= 303 status))
          (is (contains? headers "Location")))
        (testing "and retrieve the group from the received location"
          (let [{:keys [body status]} (app (request :get (get headers "Location" "dne")))
                bgroup-id (re-find #"[^-]{8}-[^-]{4}-[^-]{4}-[^-]{4}-[^-]{12}"
                                   (get headers "Location" ""))
                bgroup-with-id (-> bgroup (assoc :id bgroup-id), convert-uuids)]
            (is (= 200 status))
            (is (= bgroup-with-id (-> body (decode true) convert-uuids)))))))

    (testing "returns the group if it exists"
      (let [{body :body, :as resp} (app (group-request :get (:id agroup)))]
        (is-http-status 200 resp)
        (is (= agroup (-> body (decode true) convert-uuids)))))

    (testing "returns all groups"
      (let [{body :body, :as resp} (app (group-request))
            groups-with-annotations (map (partial storage/annotate-group mock-db) groups)]
        (is-http-status 200 resp)
        (is (= (set groups-with-annotations) (-> body
                                               (decode true)
                                               (->> (map convert-uuids))
                                               set)))))

    (testing "returns annotated version of a group"
      (app (group-request :put (:id annotated) (encode annotated)))
      (let [{body :body, :as resp} (app (group-request :get (:id annotated)))]
        (is-http-status 200 resp)
        (is (= with-annotations (-> body (decode true) convert-uuids)))))

    (testing "updates a group"
      (let [req-body (encode {:classes {:foo {:param nil}}
                              :variables {:ntp_servers nil}})
            {body :body, :as resp} (app (group-request :post (:id agroup') req-body))]
        (is-http-status 200 resp)
        (is (= agroup' (-> body (decode true) convert-uuids)))))

    (testing "updating a group that doesn't exist produces a 404"
      (let [post-body (encode {:name "different", :variables {:exists false}})
            {:keys [status]} (app (group-request :post (UUID/randomUUID) post-body))]
        (is (= 404 status))))

    (testing "tells storage to delete the group and returns 204"
      (let [response (app (group-request :delete (:id agroup)))]
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

(defn classification-request
  ([] (classification-request :get nil nil))
  ([method node-name] (classification-request method node-name nil))
  ([method node-name body]
   (let [req (request method (str "/v1/classified/nodes" (if node-name (str "/" node-name))))]
     (if body
       (mock/body req body)
       req))))

(deftest classification
  (let [group-id (UUID/randomUUID)
        rule {:when ["and" ["=" ["facts" "a"] "b"] ["=" ["trusted" "certname"] "abcdefg"]]
              :group-id group-id}
        group {:name "factgroup"
               :id (UUID/randomUUID)
               :environment "production"
               :parent root-group-uuid
               :rule (:when rule)
               :classes {}
               :variables {}}
        mock-db (reify Storage
                  (get-rules [_]
                    [rule])
                  (get-group [_ _]
                    group)
                  (get-ancestors [_ _]
                    nil))
        app (app {:db mock-db})]
    (testing "facts submitted via POST can be used for classification"
      (let [facts {:name "qwkeju" :values {:a "b"}}
            trusted {:certname "abcdefg"}
            {body :body, :as response} (app (classification-request
                                             :post
                                             "qwkeju"
                                             (encode {:facts facts :trusted trusted})))]
        (is-http-status 200 response)
        (is (= [group-id] (map #(UUID/fromString %) (:groups (decode body true)))))))))

(deftest errors
  (let [mock-db (reify Storage
                  (get-group [_ _]
                    nil)
                  (create-group [_ group]
                    (sc/validate Group group))
                  (get-ancestors [_ _]
                    []))
        app (app {:db mock-db})]

    (testing "requests with a malformed UUID get a structured 400 response"
      (let [{:keys [status body]} (app (request :get "/v1/groups/not-a-uuid"))
            error (decode body true)]
        (is (= 400 status))
        (is (= #{:kind :msg :details}) (-> error keys set))
        (is (= "malformed-uuid" (:kind error)))
        (is (= (:details error) "not-a-uuid")))

      (let [{:keys [status body]} (app (-> (request :put (str "/v1/groups/" (UUID/randomUUID)))
                                         (mock/body (encode {:parent "not-a-uuid"}))))
            error (decode body true)]
        (is (= 400 status))
        (is (= #{:kind :msg :details}) (-> error keys set))
        (is (= "malformed-uuid" (:kind error)))
        (is (= (:details error) "not-a-uuid"))))

    (testing "requests that don't match schema get a structured 400 response."
      (let [incomplete-group {:classes ["foo" "bar" "baz"]}
            resp (app (-> (request :put (str "/v1/groups/" (UUID/randomUUID)))
                        (mock/body (encode incomplete-group))))
            error (decode (:body resp) true)]
        (is-http-status 400 resp)
        (is (= "application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:kind :msg :details} (-> error keys set)))
        (is (= "schema-violation" (:kind error)))
        (is (= #{:submitted :schema :error} (-> error :details keys set)))))

    (testing "malformed requests get a structured 400 response."
      (let [bad-body "{\"haha\": [\"i'm broken\"})"
            resp (app (-> (request :put (str "/v1/groups/" (UUID/randomUUID)))
                        (mock/body bad-body)))
            error (decode (:body resp) true)]
        (is-http-status 400 resp)
        (is (re-find #"application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:kind :msg :details} (-> error keys set)))
        (is (= bad-body (-> error :details :body)))
        (is (re-find #"Unexpected close marker" (-> error :details :error)))))))
