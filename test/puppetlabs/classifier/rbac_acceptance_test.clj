(ns puppetlabs.classifier.rbac-acceptance-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clj-http.client :as http]
            [schema.test]
            [puppetlabs.certificate-authority.core :as ssl]
            [puppetlabs.http.client.sync :as phttp]
            [puppetlabs.kitchensink.core :refer [deep-merge]]
            [puppetlabs.trapperkeeper.config :refer [load-config]]
            [puppetlabs.classifier.acceptance-test :refer [config-path
                                                           with-classifier-instance-fixture]]))

(def pos-test-config
  (deep-merge (load-config config-path)
              {:classifier {:access-control true}
               :global {:certificate-whitelist "./dev-resources/ssl/certs.txt"}}))

(def neg-test-config
  (assoc-in pos-test-config [:global :certificate-whitelist] "./dev-resources/ssl/bogus-certs.txt"))

(defn- origin-url
  [app-config]
  (let [{{{host :ssl-host port :ssl-port} :classifier} :webserver} app-config]
    (str "https://" (if (= host "0.0.0.0") "localhost" host) ":" port)))

(defn- base-url
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

(deftest ^:acceptance rbac-smoke
  (testing "request using certs not in whitelist gets a 403"
    ((with-classifier-instance-fixture neg-test-config)
     (fn []
       (let [resp (phttp/get (str test-base-url "/v1/environments")
                             {:ssl-context test-ssl-context})]
         (is (= 403 (:status resp)))))))

  (testing "request using certs in whitelist gets a 200"
    ((with-classifier-instance-fixture pos-test-config)
     (fn []
       (let [resp (phttp/get (str test-base-url "/v1/environments")
                             {:ssl-context test-ssl-context})]
         (is (= 200 (:status resp))))))))
