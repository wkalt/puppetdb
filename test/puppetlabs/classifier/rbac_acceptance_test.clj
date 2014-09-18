(ns puppetlabs.classifier.rbac-acceptance-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [schema.test]
            [puppetlabs.certificate-authority.core :as ssl]
            [puppetlabs.http.client.sync :as phttp]
            [puppetlabs.kitchensink.core :refer [deep-merge]]
            [puppetlabs.rbac.storage :as rbac-storage]
            [puppetlabs.rbac.testutils.fixtures :refer [reset-db test-db]]
            [puppetlabs.trapperkeeper.config :refer [load-config]]
            [puppetlabs.classifier.application.permissioned.rbac-test
             :refer [config-with-rbac-db-from-env]]
            [puppetlabs.classifier.acceptance-test
             :refer [config-path with-classifier-instance-fixture]]
            [puppetlabs.classifier.application.permissioned.rbac :refer [rbac-permission]]
            [puppetlabs.classifier.application.permissioned.rbac-test
             :refer [config-with-rbac-db-from-env get-role-by-name insert-test-subject-and-role]]
            [puppetlabs.classifier.storage :refer [root-group-uuid]]))

(def pos-test-config
  (-> (load-config config-path)
    config-with-rbac-db-from-env
    (deep-merge {:classifier {:access-control true}
                 :rbac {:certificate-whitelist "./dev-resources/ssl/certs.txt"}})))

(def neg-test-config
  (assoc-in pos-test-config [:rbac :certificate-whitelist] "./dev-resources/ssl/bogus-certs.txt"))

(defn- origin-url
  [app-config]
  (let [{{{host :ssl-host port :ssl-port} :classifier} :webserver} app-config]
    (str "https://" (if (= host "0.0.0.0") "localhost" host) ":" port)))

(defn base-url
  [app-config]
  (str (origin-url app-config)
       (or (get-in app-config
                   [:web-router-service :puppetlabs.classifier.main/classifier-service :route])
           (get-in app-config
                   [:web-router-service :puppetlabs.classifier.main/classifier-service]))))

(def test-base-url (base-url pos-test-config))

(def test-ssl-context (ssl/pems->ssl-context "./dev-resources/ssl/cert.pem"
                                             "./dev-resources/ssl/key.pem"
                                             "./dev-resources/ssl/ca.pem"))

(use-fixtures :once schema.test/validate-schemas)
(use-fixtures :each reset-db)

(deftest ^:acceptance rbac-smoke
  (testing "request using certs not in whitelist gets a 403"
    ((with-classifier-instance-fixture neg-test-config)
     (fn []
       (let [resp (phttp/get (str test-base-url "/v1/environments")
                             {:ssl-context test-ssl-context})]
         (is (= 403 (:status resp)))))))

  (testing "request using certs in whitelist"
    ((with-classifier-instance-fixture pos-test-config)
     (fn []
       (testing "gets a 200 if the action is permitted"
         (let [resp (phttp/get (str test-base-url "/v1/groups/" root-group-uuid)
                               {:ssl-context test-ssl-context})]
           (is (= 200 (:status resp))))

         (let [resp (phttp/post (str test-base-url "/v1/groups")
                                {:ssl-context test-ssl-context
                                 :headers {"Content-Type" "application/json"}
                                 :body (json/encode {:name "test"
                                                     :parent root-group-uuid
                                                     :rule ["=" "name" "foo"]
                                                     :classes {}})})]
           (is (= 200 (:status resp)))
           (println (-> resp :body io/reader line-seq str/join))))


       (testing "gets a 403 if the action is not permitted"
         (let [test-role {:display_name "NC RBAC acceptance test"
                          :description ""
                          :permissions []
                          :user_ids []}]
           (rbac-storage/create-role! test-db test-role)
           (let [role-id (:id (rbac-storage/get-role
                                test-db :display_name (:display_name test-role)))
                 api-user' (-> (rbac-storage/get-user test-db :display_name "API User")
                             (assoc :role_ids [role-id]))]
             (rbac-storage/update-local-user! test-db (:id api-user') api-user')
             (let [root-group-path (str "/v1/groups/" root-group-uuid)
                   {:keys [status body]} (phttp/get (str test-base-url root-group-path)
                                                    {:ssl-context test-ssl-context})
                   string-body (-> body io/reader line-seq str/join)
                   {:keys [kind msg]} (json/decode string-body true)]
               (is (= 403 status))
               (is (= kind "permission-denied"))
               (is (re-find #"don't have permission to view" msg))))))))))
