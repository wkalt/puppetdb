(ns puppetlabs.classifier.activity-acceptance-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [cheshire.core :as json]
            [puppetlabs.rbac.testutils.fixtures :as rbac-fixtures]
            [puppetlabs.rbac.testutils.test-helpers :as h]
            [puppetlabs.activity.postgres :as pgs]
            [puppetlabs.classifier.storage.postgres-test :refer [db-fixtures]]
            [clj-http.client :as http]
            [schema.test]
            [puppetlabs.certificate-authority.core :as ssl]
            [puppetlabs.rbac.testutils.fixtures :refer [reset-db test-db]]
            [puppetlabs.http.client.sync :as phttp]
            [puppetlabs.trapperkeeper.app :refer [get-service]]
            [puppetlabs.classifier.main :refer [classifier-service]]
            [puppetlabs.classifier.acceptance-test
             :refer [config-path with-classifier-instance-fixture]]
            [puppetlabs.activity.services :refer
             [activity-service activity-reporting-service]]
            [puppetlabs.classifier.application.permissioned.rbac-test
             :refer [config-with-rbac-db-from-env]]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-config]]
            [puppetlabs.rbac.services.rbac :refer [rbac-service]]
            [puppetlabs.rbac.services.http.middleware :refer [rbac-authn-middleware]]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :refer [jetty9-service]]
            [puppetlabs.rbac.services.authn :refer [rbac-authn-service]]
            [puppetlabs.rbac.services.authz :refer [rbac-authz-service]]
            [puppetlabs.rbac.testutils.services.dev-login :refer [dev-login-service]]
            [puppetlabs.classifier.rbac-acceptance-test :as rat]
            [puppetlabs.trapperkeeper.services.webrouting.webrouting-service :refer
             [webrouting-service]])
  (:import java.sql.BatchUpdateException))

(def activity-url-prefix "/activity-api")

(def services [jetty9-service rbac-authn-middleware rbac-service
               rbac-authn-service rbac-authz-service webrouting-service
               rbac-authn-middleware dev-login-service activity-service
               activity-reporting-service classifier-service])

(def activity-db
  {:subname "activity_test" :subprotocol "postgresql"
   :user "activity_test" :password "activity_test"})

(def test-config
  {:activity {:database activity-db :cors-origin-pattern ".*"}
   :webserver {:classifier-rbac-authz-test {:host "0.0.0.0"
                                            :port 1728}
               :rbac {:host        "127.0.0.1"
                      :port        8881
                      :ssl-key     "./dev-resources/ssl/key.pem"
                      :ssl-cert    "./dev-resources/ssl/cert.pem"
                      :ssl-ca-cert "./dev-resources/ssl/ca.pem"
                      :default-server true}
               :activity {:host "127.0.0.1"
                          :port 8882}
               :classifier {:ssl-ca-cert "./dev-resources/ssl/ca.pem"
                            :ssl-host "0.0.0.0"
                            :ssl-port 1262
                            :ssl-key "./dev-resources/ssl/key.pem"
                            :ssl-cert "./dev-resources/ssl/cert.pem"
                            :host "0.0.0.0"
                            :port 1261}}

   :rbac {:token-public-key "./dev-resources/ssl/cert.pem"
          :token-private-key "./dev-resources/ssl/key.pem"
          :session-timeout 60
          :password-reset-expiration 24
          :certificate-whitelist "./dev-resources/ssl/certs.txt"}

   :rbac-database {:password "foobar" :user "perbac" :classname "org.postgresql.Driver"
                   :subname "//localhost:5432/perbac_test" :subprotocol "postgresql"}
   :classifier {:access-control true
                :activity-service "enabled"
                :puppet-master "https://localhost:8140"
                :database {:subprotocol "postgresql"
                           :subname "classifier_test"
                           :user "classifier_test"
                           :password "classifier_test"}}
   :web-router-service {:puppetlabs.activity.services/activity-service
                        {:route activity-url-prefix
                         :server "activity"}
                        :puppetlabs.rbac.services.http.api/rbac-http-api-service
                        {:route "/rbac-api/v1"
                         :server "rbac"}
                        :puppetlabs.rbac.testutils.services.dev-login/dev-login-service
                        {:route "/auth"
                         :server "rbac"}
                        :puppetlabs.classifier.main/classifier-service {:server "classifier"
                                                                        :route "/classifier"}}})

(def test-base-url (rat/base-url test-config))

(def api-base-url
  (format "http://%s:%s%s"
          (get-in test-config [:webserver :activity :host])
          (get-in test-config [:webserver :activity :port])
          activity-url-prefix))

(def authn-base-url
  (format "http://%s:%s/auth"
          (get-in test-config [:webserver :rbac :host])
          (get-in test-config [:webserver :rbac :port])))

(defn post-login!
  [un pw]
  (http/post  (format "%s/login" authn-base-url)
             {:content-type "application/x-www-form-urlencoded"
              :body  (format "username=%s&password=%s" un pw)
              :throw-exceptions false}))

(defn post-logout!
  []
  (http/post  (format "%s/logout" authn-base-url)
             {:content-type "application/x-www-form-url-encoded"
              :throw-exceptions false}))

(defmacro with-login [un pw & body]
  `(binding [clj-http.core/*cookie-store* (clj-http.cookies/cookie-store)]
     (post-login! ~un ~pw)
     ~@body
     (post-logout!)))

(def test-ssl-context
  (ssl/pems->ssl-context "./dev-resources/ssl/cert.pem"
                         "./dev-resources/ssl/key.pem"
                         "./dev-resources/ssl/ca.pem"))

(defn with-activity-db
  "Fixture that sets up a cleanly initialized and migrated database"
  [f]
  (pgs/drop-public-tables! activity-db)
  (pgs/drop-public-functions! activity-db)
  (pgs/migrate! activity-db)
  (f))

(def aclass
  {:name "apache"
   :environment "production"
   :parameters {:confd_dir "/etc/apache2"
                :mod_dir "/etc/apache2/mods-available"
                :logroot "/var/log/apache2"}})

(def agroup
  {:id "ddab2071-26ea-4261-af09-7b6b97fa04c2"
   :name "webservers"
   :environment "production"
   :parent "00000000-0000-4000-8000-000000000000"
   :rule ["~" "name" "\\.www\\.example\\.com$"]
   :classes {:apache {:confd_dir "/opt/mywebapp/etc/apache2"
                      :logroot "/opt/mywebapp/log"}}})

(def bgroup (assoc agroup :name "blobservers"))

(defn get-url [relative-url]
  (http/get (format "%s%s" api-base-url relative-url)
            {:throw-exceptions false
             :content-type     "application/json"
             :follow-redirects false}))

(use-fixtures :once schema.test/validate-schemas)
(use-fixtures :once rbac-fixtures/reset-db with-activity-db db-fixtures)

(deftest ^:acceptance activity-smoke
  (with-app-with-config app services test-config
    (testing "empty response from activity url"
      (with-login "admin" "admin"
        (let [resp (get-url "/v1/events?service_id=classifier")]
          (is (= {"commits" [] "offset" 0 "limit" 0 "total-rows" 0}
                 (json/decode (:body resp)))))))

    (testing "submit class and group through classifier "
      (let [resp1 (phttp/put (str test-base-url "/v1/environments/production/classes/apache")
                             {:ssl-context test-ssl-context :body (json/encode aclass)})
            resp2 (phttp/put (str test-base-url "/v1/groups/ddab2071-26ea-4261-af09-7b6b97fa04c2")
                             {:ssl-context test-ssl-context :body (json/encode agroup)})]
        (is (= (:status resp1) 201))
        (is (= (:status resp2) 201))))

    (testing "activity service reflects the new group"
      (with-login "admin" "admin"
        (let [resp (get-url "/v1/events?service_id=classifier")]
          (is (= {"commits"  [{"events"  []
                               "object" "webservers"
                               "subject" "api_user"}]
                  "offset" 0 "limit" 1 "total-rows" 1}
                 (-> (json/decode (:body resp))
                     (update-in ["commits"]
                                (fn [x] (map #(dissoc % "timestamp") x)))))))))

    (testing "update the group"
      (let [resp (phttp/put
                   (str test-base-url "/v1/groups/ddab2071-26ea-4261-af09-7b6b97fa04c2")
                   {:ssl-context test-ssl-context :body (json/encode bgroup)})]
        (is (= (:status resp) 201))))

    (testing "check activity service for updates"
      (with-login "admin" "admin"
        (let [resp (get-url "/v1/events?service_id=classifier")]
          (is (= (-> (json/decode (:body resp))
                     (update-in ["commits"]
                                (fn [x] (map #(dissoc % "timestamp") x))))
                 {"commits" [{"events" [] "object" "blobservers" "subject" "api_user"}
                             {"events" [] "object" "webservers" "subject" "api_user"}]
                  "offset" 0 "limit" 2 "total-rows" 2})))))))
