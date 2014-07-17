(ns puppetlabs.classifier.http-test
  (:require [clojure.test :refer :all]
            [cheshire.core :refer [decode encode generate-string parse-string]]
            [clj-time.format :as fmt-time]
            [compojure.core :as compojure]
            [ring.mock.request :as mock :refer [request]]
            [schema.test]
            [schema.core :as sc]
            [slingshot.slingshot :refer [throw+]]
            [puppetlabs.kitchensink.core :refer [deep-merge mapkeys mapvals]]
            [puppetlabs.kitchensink.json :refer [add-common-json-encoders!]]
            [puppetlabs.classifier.http :refer :all]
            [puppetlabs.classifier.schema :refer [CheckIn ClientNode Group GroupDelta PuppetClass]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid Storage]]
            [puppetlabs.classifier.storage.memory :refer [in-memory-storage]]
            [puppetlabs.classifier.test-util :refer [blank-group blank-group-named]]
            [puppetlabs.classifier.util :refer [->uuid clj-key->json-key json-key->clj-key
                                                merge-and-clean]])
  (:import java.util.UUID))

(defn is-http-status
  "Assert an http status code"
  [status response]
  (is (= status (:status response))))

(defn json-encoders-fixture
  [f]
  (add-common-json-encoders!)
  (f))

(use-fixtures :once json-encoders-fixture schema.test/validate-schemas)

(deftest crud
  (let [test-obj-name "test-obj"
        test-obj {:name "test-obj" :property "hello"}
        bad-obj {:name "bad" :property 3}
        schema {:name String
                :property String}
        storage (in-memory-storage {})
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
                   :environment-trumps false
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
        agroup {:name "agroup"
                :id (UUID/randomUUID)
                :environment "bar"
                :environment-trumps false
                :parent root-group-uuid
                :rule ["=" "name" "bert"]
                :classes {:foo {:override "overriden"}}
                :variables {:ntpservers ["0.us.pool.ntp.org" "ntp.example.com"]}}
        root {:name "default"
              :id root-group-uuid
              :environment "production"
              :environment-trumps false
              :parent root-group-uuid
              :rule ["=" "nofact" "noval"]
              :classes {}
              :variables {}}
        bgroup {:name "bgroup"
                :id (UUID/randomUUID)
                :environment "quux"
                :environment-trumps false
                :parent root-group-uuid
                :rule ["=" "name" "elmo"]
                :classes {}
                :variables {}}
        classes [{:name "foo", :environment "bar", :parameters {:override "default"}}
                 {:name "baz", :environment "production", :parameters {}}]
        mem-db (in-memory-storage {:groups [root] , :classes classes})
        app (app {:db mem-db})]

    (testing "returns 404 to a GET request if the group doesn't exist"
      (let [{status :status} (app (group-request :get (UUID/randomUUID)))]
        (is (= 404 status))))

    (testing "returns 404 to a POST request if the group doesn't exist"
      (let [{status :status} (app (group-request :post, (UUID/randomUUID)
                                                 (encode {:environment "staging"})))]
        (is (= 404 status))))

    (testing "can create a group with a PUT to the URI"
      (let [group-json-rep (encode agroup {:key-fn clj-key->json-key})
            {body :body, :as resp} (app (group-request :put (:id agroup) group-json-rep))]
        (testing "and get a 201 Created response"
          (is-http-status 201 resp))
        (testing "and get the group back in the response body"
          (is (= agroup (-> body (decode json-key->clj-key) convert-uuids))))))

    (testing "can create a group with a POST to the group collection"
      (let [group-json-rep (-> bgroup
                             (dissoc :id)
                             (encode {:key-fn clj-key->json-key}))
            {:keys [status headers]} (app (group-request :post nil group-json-rep))]
        (testing "and get a 303 back with the group's location"
          (is (= 303 status))
          (is (contains? headers "Location")))
        (testing "and retrieve the group from the received location"
          (let [{:keys [body status]} (app (request :get (get headers "Location" "dne")))
                bgroup-id (re-find #"[^-]{8}-[^-]{4}-[^-]{4}-[^-]{4}-[^-]{12}"
                                   (get headers "Location" ""))
                bgroup-with-id (-> bgroup (assoc :id bgroup-id), convert-uuids)]
            (is (= 200 status))
            (is (= bgroup-with-id (-> body (decode json-key->clj-key) convert-uuids)))))))

    (testing "returns the group if it exists"
      (let [{body :body, :as resp} (app (group-request :get (:id agroup)))]
        (is-http-status 200 resp)
        (is (= agroup (-> body (decode json-key->clj-key) convert-uuids)))))

    (testing "returns annotated version of a group"
      (app (group-request :put (:id annotated) (encode annotated {:key-fn clj-key->json-key})))
      (let [{body :body, :as resp} (app (group-request :get (:id annotated)))]
        (is-http-status 200 resp)
        (is (= with-annotations (-> body (decode json-key->clj-key) convert-uuids)))))

    (testing "returns all groups"
      (let [{body :body, :as resp} (app (group-request))
            groups-with-annotations (->> [root agroup bgroup annotated]
                                      (map (partial storage/annotate-group mem-db))
                                      (map #(dissoc % :id)))]
        (is-http-status 200 resp)
        (is (= (set groups-with-annotations) (-> body
                                               (decode json-key->clj-key)
                                               (->>
                                                 (map convert-uuids)
                                                 (map #(dissoc % :id))
                                                 set))))))

    (testing "updates a group"
      (let [delta {:classes {:foo {:override nil}}
                   :variables {:ntpservers nil}}
            {body :body, :as resp} (app (group-request :post (:id agroup) (encode delta)))]
        (is-http-status 200 resp)
        (is (= (merge-and-clean agroup delta)
               (-> body (decode json-key->clj-key) convert-uuids)))))

    (testing "updating a group that doesn't exist produces a 404"
      (let [post-body (encode {:name "different", :variables {:exists false}})
            {:keys [status]} (app (group-request :post (UUID/randomUUID) post-body))]
        (is (= 404 status))))

    (testing "tells storage to delete the group and returns 204"
      (let [response (app (group-request :delete (:id agroup)))]
        (is-http-status 204 response)
        (is-http-status 404 (app (group-request :get (:id agroup))))))))

(deftest dont-make-orphans
  (let [parent {:name "Breeders"
                :parent root-group-uuid
                :id (UUID/randomUUID)
                :environment "production", :environment-trumps false
                :rule ["=" "foo" "bar"], :classes {}, :variables {}}
        annie {:name "Annie"
               :parent (:id parent)
               :id (UUID/randomUUID)
               :environment "production", :environment-trumps false
               :rule ["=" "foo" "bar"], :classes {}, :variables {}}
        mem-db (in-memory-storage {:groups [parent annie]})
        handler (app {:db mem-db})]

    (testing "delete requests that would create orphans get a 422 response"
      (let [{:keys [body status]} (handler (group-request :delete (:id parent)))
            {:keys [kind msg details]} (decode body json-key->clj-key)]
        (is (= 422 status))
        (is (= "children-present" kind))
        (is (re-find #"not be deleted because it has children: \[\"Annie\"\]" msg))
        (is (= {:group parent, :children [annie]}
               (-> details
                 (update-in [:group] convert-uuids)
                 (update-in [:children] (partial map convert-uuids)))))))))

(deftest inherited-group
  (let [suspenders {:name "suspenders"
                    :parameters {:color "0x000000", :length nil}}
        music {:name "music"
               :parameters {:genre "Rock'n'Roll", :top-artists ["Elvis Presley" "Chuck Berry"]}}
        opinions {:name "opinions"
                  :parameters {:the-guvmnt nil
                               :politicians nil
                               :fashion nil
                               :popular-music nil}}
        crotchety-ancestor {:name "Hubert"
                            :id (UUID/randomUUID)
                            :environment "FDR's third term"
                            :environment-trumps false
                            :parent root-group-uuid
                            :rule [">=" ["fact" "age"] "93"]
                            :classes {:suspenders {:color "0xff0000"
                                                   :length "38"}
                                      :music {:genre "Crooners"
                                              :top-artists ["Bing Crosby" "Frank Sinatra"]}
                                      :opinions {:the-guvmnt "not what it used to be"
                                                 :politicians "bunch of lying crooks these days"
                                                 :fashion "utterly depraved"
                                                 :popular-music "don't get me started"}}
                            :variables {:ancestry "Scottish"}}
        child {:name "Huck"
               :id (UUID/randomUUID)
               :environment "not inherited"
               :environment-trumps false
               :parent (:id crotchety-ancestor)
               :rule ["and" ["<" ["fact" "age"] "93"]
                            [">=" ["fact" "age"] "60"]]
               :classes {:music {:genre "Rock 'n' Roll"
                                 :top-artists ["The Beatles" "Led Zeppelin"]}
                         :opinions {:politicians "always been rotten"}}
               :variables {}}
        child-path (str "/v1/groups/" (:id child))
        groups [crotchety-ancestor child]
        mem-db (in-memory-storage {:groups groups
                                   :classes (for [env (map :environment groups)
                                                  class [suspenders music opinions]]
                                              (assoc class :environment env))})
        handler (app {:db mem-db})]

    (testing "can get an inherited version of a group"
      (let [{:keys [status body]} (handler (request :get child-path {:inherited true}))]
        (is (= 200 status))
        (is (= (deep-merge crotchety-ancestor child)
               (-> body
                 (decode json-key->clj-key)
                 convert-uuids)))))

    (testing "can explicitly decline to see the group as inherited"
      (let [{:keys [status body]} (handler (request :get child-path {:inherited 0}))]
        (is (= 200 status))
        (is (= child
               (-> body
                 (decode json-key->clj-key)
                 convert-uuids)))))))

(defn class-request
  ([method env] (class-request method env nil nil))
  ([method env name] (class-request method env name nil))
  ([method env name body]
   (let [req (request method (str "/v1/environments/" env "/classes" (if name (str "/" name))))]
     (if body
       (mock/body req body)
       req))))

(deftest classes
  (let [myclass {:name "myclass", :environment "test"
                 :parameters {:sweetness "totes"
                              :radness "utterly"
                              :awesomeness_level "off the charts"}}
        theirclass {:name "theirclass", :environment "test"
                    :parameters {:dumbness "totally"
                                 :stinkiness "definitely"
                                 :looks_like_a_butt? "from some angles"}}
        mem-db (in-memory-storage {:classes [theirclass]})
        app (app {:db mem-db})]

    (testing "tells the storage layer to store the class map"
      (let [{body :body, :as resp} (app (class-request :put "test" (:name myclass)
                                                       (encode myclass)))]
        (is-http-status 201 resp)
        (is (= myclass (decode body true)))))

    (testing "returns class with its parameters"
      (let [{body :body, :as resp} (app (class-request :get "test" (:name myclass)))]
        (is-http-status 200 resp)
        (is (= myclass (decode body true)))))

    (testing "retrieves all classes"
      (let [{body :body, :as resp} (app (class-request :get "test"))]
        (is-http-status 200 resp)
        (is (= #{myclass theirclass} (set (decode body true))))))

    (testing "tells storage to delete the class and returns 204"
      (let [response (app (class-request :delete "test" (:name myclass)))]
        (is-http-status 204 response)))))

(defn classification-request
  ([] (classification-request :get nil nil))
  ([node-name] (classification-request :get node-name))
  ([method node-name] (classification-request method node-name nil))
  ([method node-name body]
   (let [req (request method (str "/v1/classified/nodes" (if node-name (str "/" node-name))))]
     (if body
       (mock/body req body)
       req))))

(deftest empty-classification
  (let [root {:name "default"
              :id root-group-uuid
              :environment "production"
              :environment-trumps false
              :parent root-group-uuid
              :rule ["~" "name" ".*"]
              :classes {}, :variables {}}
        mem-db (in-memory-storage {:groups [root]})
        app (app {:db mem-db})]
    (testing "classification returns the right structure with a blank db"
      (let [{body :body, :as response} (app (classification-request
                                              :post
                                              "hellote"
                                              (encode {})))]
        (is-http-status 200 response)
        (is (= {:name "hellote"
                :environment "production"
                :groups [root-group-uuid]
                :classes {}
                :parameters {}}
               (-> body
                 (decode json-key->clj-key)
                 (update-in [:groups] (partial map #(UUID/fromString %))))))))))

(deftest classification
  (let [group-id (UUID/randomUUID)
        rule {:when ["and" ["=" ["fact" "a"] "b"] ["=" ["trusted" "certname"] "abcdefg"]]
              :group-id group-id}
        group {:name "factgroup"
               :id group-id
               :environment "production"
               :environment-trumps false
               :parent root-group-uuid
               :rule (:when rule)
               :classes {}
               :variables {}}
        mem-db (in-memory-storage {:groups [group]})
        app (app {:db mem-db})]

    (testing "facts submitted via POST can be used for classification"
      (let [fact {:a "b"}
            trusted {:certname "abcdefg"}
            {body :body, :as response} (app (classification-request
                                             :post
                                             "qwkeju"
                                             (encode {:fact fact :trusted trusted})))]
        (is-http-status 200 response)
        (is (= [group-id] (map #(UUID/fromString %) (:groups (decode body json-key->clj-key))))))))

  (let [red-class {:name "redclass"
                   :environment "staging"
                   :parameters {:red "the blood of angry men", :black "the dark of ages past"}}
        blue-class {:name "blueclass"
                    :environment "staging"
                    :parameters {:blue "dabudi dabudai"}}
        root {:name "default", :id root-group-uuid
              :parent root-group-uuid
              :environment "production"
              :environment-trumps false
              :rule ["=" "foo" "foo"]
              :classes {}, :variables {}}
        left-child {:name "left-child", :id (UUID/randomUUID)
                    :environment "staging"
                    :environment-trumps false
                    :parent root-group-uuid
                    :rule ["=" "name" "multinode"]
                    :classes {:redclass {:red "a world about to dawn"}
                              :blueclass {:blue "since my baby left me"}}
                    :variables {:snowflake "identical"}}
        right-child {:name "right-child", :id (UUID/randomUUID)
                     :environment "staging"
                     :environment-trumps false
                     :parent root-group-uuid
                     :rule ["=" "name" "multinode"]
                     :classes {:redclass {:black "the night that ends at last"}}
                     :variables {:snowflake "identical"}}
        grandchild {:name "grandchild", :id (UUID/randomUUID)
                    :environment "staging"
                    :environment-trumps false
                    :parent (:id right-child)
                    :rule ["=" "name" "multinode"]
                    :classes {:blueclass {:blue "since my baby left me"}}
                    :variables {:snowflake "identical"}}
        mem-db (in-memory-storage {:groups [root left-child right-child grandchild]})
        handler (app {:db mem-db})]

    (testing "classifications are merged if they are disjoint"
      (let [{:keys [status body]} (handler (classification-request "multinode"))]
        (is (= 200 status))
        (is (= {:name "multinode"
                :environment "staging"
                :groups (->> [left-child right-child grandchild]
                          (map :id)
                          set)
                :classes {:blueclass {:blue "since my baby left me"}
                          :redclass {:red "a world about to dawn"
                                     :black "the night that ends at last"}}
                :parameters {:snowflake "identical"}}
               (-> body
                 (decode json-key->clj-key)
                 (update-in [:groups] (comp set (partial map #(UUID/fromString %)))))))))

    (testing "if classifications are not disjoint"
      (let [right-child' (deep-merge right-child {:classes {:blueclass {:blue "suede shoes"}}})
            left-child' (deep-merge left-child {:environment "production"})
            grandchild' (merge-and-clean grandchild {:classes {:blueclass nil}
                                                     :variables {:snowflake "unique"}})
            mem-db' (in-memory-storage {:groups [root left-child' right-child' grandchild']})
            handler' (app {:db mem-db'})
            {:keys [status body]} (handler' (classification-request "multinode"))
            error (decode body json-key->clj-key)
            convert-uuids-if-group #(if (map? %) (convert-uuids %) %)]

        (testing "a 500 error is thrown"
          (is (= 500 status))
          (testing "that has kind 'classification-conflict'"
            (is (= "classification-conflict" (:kind error))))

          (testing "that contains details for"
            (testing "environment conflicts"
              (let [environment-conflicts (->> (get-in error [:details :environment])
                                            (map (partial mapvals convert-uuids-if-group)))]
                (is (= #{{:value "production", :from left-child', :defined-by left-child'}
                         {:value "staging", :from grandchild', :defined-by grandchild'}}
                       (set environment-conflicts)))))

            (testing "variable conflicts"
              (let [variable-conflicts (get-in error [:details :variables])
                    snowflake-conflicts (->> (:snowflake variable-conflicts)
                                          (map (partial mapvals convert-uuids-if-group)))]
                (is (= [:snowflake] (keys variable-conflicts)))
                (is (= #{{:value "unique", :from grandchild', :defined-by grandchild'}
                         {:value "identical", :from left-child', :defined-by left-child'}}))))

            (testing "class parameter conflicts"
              (let [class-conflicts (get-in error [:details :classes])
                    blueclass-conflicts (:blueclass class-conflicts)
                    blue-param-conflicts (->> (:blue blueclass-conflicts)
                                           (map (partial mapvals convert-uuids-if-group)))]
                (is (= [:blueclass] (keys class-conflicts)))
                (is (= [:blue] (keys blueclass-conflicts)))
                (is (= #{{:value "suede shoes", :from grandchild', :defined-by right-child'}
                         {:value "since my baby left me"
                          :from grandchild'
                          :defined-by grandchild'}}))))))))))

(deftest environment-trump-classification
  (let [root {:name "default"
              :id root-group-uuid
              :parent root-group-uuid
              :environment "production"
              :environment-trumps false
              :rule ["~" "name" ".*"]
              :classes {}, :variables {}}
        base-group #(merge (blank-group) {:rule ["=" "name" "pets"]})
        classified-pets-path "/v1/classified/nodes/pets"
        kittehs (merge (base-group)
                       {:name "Kittehs"
                        :environment "house"
                        :environment-trumps true
                        :variables {:meows true}})
        doggehs (merge (base-group)
                       {:name "Doggehs"
                        :environment "outside"
                        :environment-trumps false
                        :variables {:woofs true}})
        snakes (merge (base-group)
                      {:name "Snakes"
                       :environment "plane"
                       :environment-trumps false
                       :variables {:hisses true}})
        groups [kittehs doggehs snakes root]
        group-ids (->> groups
                    (map :id)
                    set)
        mem-db (in-memory-storage {:groups groups})
        handler (app {:db mem-db})]

    (testing "classifying a node into multiple groups with different environments"

      (testing "succeeds if one group has the environment-trumps flag set"
        (let [{:keys [status body]} (handler (classification-request "pets"))]
          (is (= 200 status))
          (is (= {:name "pets"
                  :environment "house"
                  :groups group-ids
                  :classes {}
                  :parameters (apply merge (map :variables groups))}
                 (-> body
                   (decode json-key->clj-key)
                   (update-in [:groups] (comp set (partial map #(UUID/fromString %)))))))))

      (testing "fails if multiple groups with distinct enviroments have the trumps flag set"
        (let [doggehs-trump (assoc doggehs :environment-trumps true)
              groups' [kittehs doggehs-trump snakes root]
              mem-db' (in-memory-storage {:groups groups'})
              handler' (app {:db mem-db'})
              {:keys [status body]} (handler' (classification-request "pets"))
              error (decode body json-key->clj-key)]
          (is (= 500 status))
          (is (= "classification-conflict" (:kind error))))))))

(deftest node-check-ins
  (let [dwarf-planets {:name "dwarf planets"
                       :rule ["and" ["=" ["fact" "orbits"] "sol"]
                              [">" ["fact" "orbital neighbors"] "0"]
                              ["=" ["fact" "shape"] "spherical"]]
                       :id (UUID/randomUUID), :environment "space", :environment-trumps false
                       :parent root-group-uuid, :classes {}, :variables {}}
        eris {:name "eris"
              :fact {"orbits" "sol"
                      "orbital neighbors" "1200"
                      "shape" "spherical"}}
        mem-db (in-memory-storage {:groups [dwarf-planets]})
        handler (app {:db mem-db})]

    (testing "stores a node check-in when a node is classified"
      (is-http-status 200 (handler (classification-request :post (:name eris) (encode eris))))
      (let [node-check-ins (storage/get-check-ins mem-db (:name eris))]
        (is (= 1 (count node-check-ins)))
        (is (sc/validate CheckIn (first node-check-ins)))))

    (testing "returns node check-ins"
      (let [{:keys [body status]} (handler (request :get (str "/v1/nodes/" (:name eris))))
            fmt-dt (fn [dt] (fmt-time/unparse (fmt-time/formatters :date-time-no-ms) dt))
            check-ins (storage/get-check-ins mem-db (:name eris))
            expected-response {:name (:name eris)
                               :check_ins (->> check-ins
                                            (map #(dissoc % :node))
                                            (map #(update-in %, [:explanation]
                                                             (partial mapkeys ->uuid)))
                                            (map #(update-in % [:time] fmt-dt)))}
            coerce-nonvalidated-keys #(if (contains? #{"check_ins" "transaction_uuid"} %)
                                        (keyword %)
                                        (json-key->clj-key %))]
        (is (= 200 status))
        (is (= expected-response (-> body
                                   (decode coerce-nonvalidated-keys)
                                   (->> (sc/validate ClientNode))
                                   (update-in [:check_ins 0 :explanation]
                                              (partial mapkeys ->uuid)))))))))

(deftest malformed-requests
  (let [mem-db (in-memory-storage {})
        app (app {:db mem-db})]

    (testing "requests with a malformed UUID get a structured 400 response"
      (let [{:keys [status body]} (app (request :get "/v1/groups/not-a-uuid"))
            error (decode body json-key->clj-key)]
        (is (= 400 status))
        (is (= #{:kind :msg :details}) (-> error keys set))
        (is (= "malformed-uuid" (:kind error)))
        (is (= (:details error) "not-a-uuid")))

      (let [{:keys [status body]} (app (-> (request :put (str "/v1/groups/" (UUID/randomUUID)))
                                         (mock/body (encode {:parent "not-a-uuid"}))))
            error (decode body json-key->clj-key)]
        (is (= 400 status))
        (is (= #{:kind :msg :details}) (-> error keys set))
        (is (= "malformed-uuid" (:kind error)))
        (is (= (:details error) "not-a-uuid"))))

    (testing "requests that don't match schema get a structured 400 response."
      (let [incomplete-group {:classes ["foo" "bar" "baz"]}
            resp (app (-> (request :put (str "/v1/groups/" (UUID/randomUUID)))
                        (mock/body (encode incomplete-group))))
            error (decode (:body resp) json-key->clj-key)]
        (is-http-status 400 resp)
        (is (= "application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:kind :msg :details} (-> error keys set)))
        (is (= "schema-violation" (:kind error)))
        (is (= #{:submitted :schema :error} (-> error :details keys set)))))

    (testing "malformed requests get a structured 400 response."
      (let [bad-body "{\"haha\": [\"i'm broken\"})"
            resp (app (-> (request :put (str "/v1/groups/" (UUID/randomUUID)))
                        (mock/body bad-body)))
            error (decode (:body resp) json-key->clj-key)]
        (is-http-status 400 resp)
        (is (re-find #"application/json" (get-in resp [:headers "Content-Type"])))
        (is (= #{:kind :msg :details} (-> error keys set)))
        (is (= bad-body (-> error :details :body)))
        (is (re-find #"Unexpected close marker" (-> error :details :error)))))))

(deftest schema-validation
  (let [app (app {:db (in-memory-storage {})})
        invalid {:name "invalid"
                 :rule ["=" "name" "Val Knott"]
                 :classes {}}
        valid (assoc invalid :parent root-group-uuid)]

    (testing "invalid groups get a 400 with a structured error message"
      (let [{:keys [body status]} (app (request :post "/v1/validate/group" (encode invalid)))
            {:keys [kind msg details]} (decode body json-key->clj-key)]
        (is (= status 400))
        (is (= kind "schema-violation"))
        (is (re-find #"parent missing-required-key" msg))))

    (testing "valid groups get a 200 back with the as-validated group in the body"
      (let [{:keys [body status]} (app (request :post "/v1/validate/group" (encode valid)))
            as-validated (-> body (decode json-key->clj-key) convert-uuids)]
        (is (= status 200))
        (is (contains? as-validated :id))
        (is (= (assoc valid :environment "production" :environment-trumps false :variables {})
               (dissoc as-validated :id)))))

    (testing "groups with malformed IDs are marked as invalid"
      (let [bad-id (assoc valid :id "not-a-uuid")
            {:keys [status]} (app (request :post "/v1/validate/group" (encode bad-id)))]
        (is (= status 400))))))

(deftest rule-translation
  (let [app (app {:db (in-memory-storage {})})
        endpoint "/v1/rules/translate"]

    (testing "the rule translation endpoint"
      (testing "translates a rule when possible"
        (let [translatable [">=" ["fact" "docking_pylons"] "3"]
              translation ["and" ["=" "name" "docking_pylons"]
                                 [">=" "value" "3"]]
              {:keys [body status]} (app (request :post endpoint (encode translatable)))]
          (is (= status 200))
          (is (= translation (decode body)))))

      (let [structured ["=" ["fact" "ifaces" "eth0" "ip"] "10.9.8.7"]
            trusted ["=" ["trusted" "certname"] "trent.totally-leg.it"]]
        (doseq [rule [structured trusted]]
          (testing "returns an error when translation isn't possible"
            (let [{:keys [body status]} (app (request :post endpoint (encode rule)))
                  {:keys [kind msg details]} (decode body json-key->clj-key)]
              (testing "that has a 422 status code"
                (is (= 422 status)))
              (testing "that has a structured body that has kind, msg, and details keys"
                (and kind msg details))
              (testing "whose kind is \"untranslatable-rule\""
                (is (= "untranslatable-rule" kind)))
              (testing "whose msg explains why the translation isn't possible"
                (if (= rule structured)
                  (is (re-find #"support structured facts" msg))
                  (is (re-find #"support trusted facts" msg)))))))))))

(deftest hierarchy-import
  (testing "import-hierarchy endpoint"
    (let [root (merge (blank-group-named "default") {:id root-group-uuid})
          rand-child #(blank-group-named (str "child-" %))
          old-groups (conj (map rand-child (range 10)) root)
          new-children (mapv rand-child (range 10 20))]

      (testing "works as expected when given a valid hierarchy"
        (let [mem-store (in-memory-storage {:groups old-groups})
              app (app {:db mem-store})
              rand-grandchild #(merge (blank-group-named (str "grandchild-" %))
                                      {:parent (:id (rand-nth new-children))})
              new-grandchildren (map rand-grandchild (range 10))
              new-groups (concat [root] new-children new-grandchildren)
              {:keys [status]} (app (request :post, "/v1/import-hierarchy"
                                             (encode new-groups clj-key->json-key)))]
          (is (= 204 status))
          (is (= (set new-groups) (set (storage/get-groups mem-store))))))

      (testing "returns understandable errors when given malformed groups"
        (let [mem-store (in-memory-storage {:groups old-groups})
              app (app {:db mem-store})
              malformed-group-1 (merge (blank-group-named "malformed-1") {:parent "sally"})
              malformed-group-2 (merge (blank-group-named "malformed-2") {:parent "breedbot 9000"})
              new-groups (concat [root] [malformed-group-1 malformed-group-2] new-children)
              {:keys [status body]} (app (request :post "/v1/import-hierarchy" (encode new-groups)))
              {:keys [msg kind details]} (decode body json-key->clj-key)
              {:keys [submitted error]} details]
          (is (= 400 status))
          (is (= kind "schema-violation"))
          (is (re-find #"not.*instance\?.*java\.util\.UUID \"breedbot 9000\"" msg))
          (is (= (set [malformed-group-1 malformed-group-2])
                 (->> submitted
                   (map convert-uuids)
                   set)))))

      (testing "returns understandable errors when given a malformed hierarchy"
        (testing "that contains a cycle"
          (let [mem-store (in-memory-storage {:groups old-groups})
                app (app {:db mem-store})
                cycle-child (blank-group-named "cycle child")
                cycle-root (merge root {:parent (:id cycle-child)})
                new-groups [cycle-child cycle-root]
                {:keys [status body]} (app (request :post "/v1/import-hierarchy" (encode new-groups)))
                {:keys [msg kind details] :as error} (decode body json-key->clj-key)]
            (is (= 422 status))
            (is (= kind "inheritance-cycle"))
            (is (re-find #"default -> cycle child -> default" msg))
            (is (= (set new-groups) (->> details
                                      (map convert-uuids)
                                      set)))))

      (testing "that contains unreachable group"
        (let [mem-store (in-memory-storage {:groups old-groups})
              app (app {:db mem-store})
              island-id (UUID/randomUUID)
              island (merge (blank-group-named "a rock; an island")
                            {:id island-id
                             :parent island-id})
              new-groups (concat [root] new-children [island])
              {:keys [status body]} (app (request :post "/v1/import-hierarchy" (encode new-groups)))
              {:keys [msg kind details] :as error} (decode body json-key->clj-key)]
          (is (= 422 status))
          (is (= kind "unreachable-groups"))
          (is (re-find #"group named \"a rock; an island\" is not reachable" msg))
          (is (= [island] (map convert-uuids details)))))))))
