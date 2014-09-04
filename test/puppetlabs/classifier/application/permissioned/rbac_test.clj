(ns puppetlabs.classifier.application.permissioned.rbac-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]
            [compojure.core :refer [routes GET]]
            [compojure.route :refer [not-found]]
            [ring.util.servlet :refer [servlet]]
            [puppetlabs.rbac.services.authn :refer [rbac-authn-service]]
            [puppetlabs.rbac.services.authz :as authz :refer [rbac-authz-service]]
            [puppetlabs.rbac.services.http.middleware :refer [rbac-authn-middleware
                                                              wrap-authenticated]]
            [puppetlabs.rbac.services.rbac :refer [rbac-service hash-password]]
            [puppetlabs.rbac.storage :as rbac-storage]
            [puppetlabs.rbac.testutils.db :refer [get-app-config]]
            [puppetlabs.rbac.testutils.fixtures :refer [make-login-fixture reset-db test-db]]
            [puppetlabs.rbac.testutils.services.dev-login :refer [dev-login-service]]
            [puppetlabs.trapperkeeper.app :refer [get-service]]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :refer [add-servlet-handler
                                                                                jetty9-service]]
            [puppetlabs.trapperkeeper.services.webrouting.webrouting-service :refer [webrouting-service]]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-config]]
            [puppetlabs.classifier.application.permissioned.rbac :refer :all])
  (:import java.util.UUID))

(def test-config-path "./dev-resources/test-conf.d")

(def test-config
  (let [env-rbac-config {:subname (System/getenv "RBAC_DBSUBNAME")
                         :user (System/getenv "RBAC_DBUSER")
                         :password (System/getenv "RBAC_DBPASS")}]
    (update-in (get-app-config test-config-path) [:rbac-database]
               merge (into {} (remove (comp nil? second) env-rbac-config)))))

(def test-base-url (let [{:keys [host port]} (get-in test-config
                                                     [:webserver :classifier-rbac-authz-test])]
                     (str "http://" host ":" port)))

(def auth-base-url (let [{:keys [host port]} (get-in test-config [:webserver :rbac])]
                     (str "http://" host ":" port "/auth")))

(def test-subj {:display_name "Spock"
                :login "spock@enterprise.starfleet.org"
                :email "spock@enterprise.starfleet.org"
                :password "tiberius"
                :is_revoked false
                :is_group false
                :is_remote false
                :is_superuser false
                :last_login nil
                :role_ids []}) ; filled in by insert-test-subject-and-role fixture

(def test-role {:display_name "NC RBAC test"
                :description ""
                :permissions []
                :user_ids []}) ; filled in by insert-test-subject-and-role fixture

(defn insert-test-subject-and-role
  [f]
  (rbac-storage/create-role! test-db test-role)
  (let [role-id (-> (rbac-storage/get-role test-db :display_name "NC RBAC test") :id)]
    (rbac-storage/create-subject! test-db (-> test-subj
                                            (update-in [:password] hash-password)
                                            (assoc :role_ids [role-id])))
    (let [subject-id (-> (rbac-storage/get-subject test-db :display_name "Spock") :id)]
      (rbac-storage/update-role! test-db role-id (assoc test-role :user_ids [subject-id]))))
  (f))

(use-fixtures :each reset-db insert-test-subject-and-role)

(defmacro with-app-with-config-with-service-bindings
  [app config service-bindings & body]
  `(with-app-with-config ~app
     [jetty9-service webrouting-service rbac-service rbac-authn-service rbac-authz-service rbac-authn-middleware
      dev-login-service]
     ~config
     (let [~@service-bindings]
       ~@body)))

(defmacro with-test-login
  [& body]
  `(let [login-url# (str auth-base-url "/login")
         logout-url# (str auth-base-url "/logout")]
     (binding [clj-http.core/*cookie-store* (clj-http.cookies/cookie-store)]
       (http/post login-url# {:content-type "application/x-www-form-urlencoded"
                              :body (format "username=%s&password=%s"
                                            (:login test-subj)
                                            (:password test-subj))
                              :throw-entire-message? true})
       ~@body
       (http/post logout-url# {:content-type "application/x-www-form-urlencoded"}))))

(defmacro with-perm-fns-bound
  [authz-svc & body]
  `(let [{:keys [~'all-group-access? ~'group-edit-classification? ~'group-edit-environment?
                 ~'group-edit-child-rules? ~'group-modify-children? ~'group-view?
                 ~'permitted-group-actions ~'viewable-group-ids]} (rbac-service-permissions
                                                                    ~authz-svc)]
     ~@body))

(defn get-role-by-name
  [display-name]
  (rbac-storage/get-role test-db :display_name display-name))

(def get-test-role #(get-role-by-name "NC RBAC test"))

(defn handler->authd-servlet
  [handler middleware-service]
  (->> handler
    (wrap-authenticated middleware-service)
    servlet))

(defn gen-prefix
  ([] (gen-prefix nil))
  ([prefix-prefix]
   (str "/" (name (gensym prefix-prefix)))))

(def rbac-action->nc-action
  "Convert the RBAC service's string name for an action to the NC's internal
  keyword corresponding to that action. The inverse of the rbac-actions map
  above."
  (into {} (for [[nc rbac] rbac-actions]
             [rbac nc])))

(deftest ^:acceptance all-group-access-perm-fn
  (testing "all-group-access? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (with-test-login
          (let [prefix (gen-prefix "group-access")
                handler (bound-fn [{:keys [shiro-subject]}]
                          (testing "returns false when subject cannot view all groups"
                            (is (not (all-group-access? shiro-subject)))
                            {:status 200}))]
            (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                 {:server-id :classifier-rbac-authz-test})
            (http/get (str test-base-url prefix)))
          (let [prefix (gen-prefix "group-access")
                view-all-groups-perm (rbac-permission :view "*")
                trole (get-test-role)
                trole' (update-in trole [:permissions] conj view-all-groups-perm)
                handler (bound-fn [{:keys [shiro-subject]}]
                          (testing "returns true when subject can view all groups"
                            (is (all-group-access? shiro-subject))
                            {:status 200}))]
            (rbac-storage/update-role! test-db (:id trole) trole')
            (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                 {:server-id :classifier-rbac-authz-test})
            (http/get (str test-base-url prefix))))))))

(deftest ^:acceptance group-edit-classification-perm-fn
  (testing "group-edit-classification? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (let [id (UUID/randomUUID)
              ancs (repeatedly 5 #(UUID/randomUUID))]
          (with-test-login
            (let [prefix (gen-prefix "edit-classification")
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns false when subject does not have the permission"
                              (is (not (group-edit-classification? subj id ancs)))
                              {:status 200}))]
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))
            (let [prefix (gen-prefix "edit-classification")
                  edit-3rd-anc-class8n (rbac-permission :edit-classification (nth ancs 3))
                  trole (get-test-role)
                  trole' (update-in trole [:permissions] conj edit-3rd-anc-class8n)
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns true when subject has the permission for an ancestor"
                              (is (group-edit-classification? subj id ancs))))]
              (rbac-storage/update-role! test-db (:id trole) trole')
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))))))))

(deftest ^:acceptance group-edit-environment-perm-fn
  (testing "group-edit-environment? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (let [id (UUID/randomUUID)
              ancs (repeatedly 1 #(UUID/randomUUID))]
          (with-test-login
            (let [prefix (gen-prefix "edit-environment")
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns false when subject does not have the permission"
                              (is (not (group-edit-environment? subj id ancs)))
                              {:status 200}))]
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))
            (let [prefix (gen-prefix "edit-environment")
                  edit-group-env (rbac-permission :edit-environment id)
                  trole (get-test-role)
                  trole' (update-in trole [:permissions] conj edit-group-env)
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns true when subject has the permission for the group"
                              (is (group-edit-environment? subj id ancs))))]
              (rbac-storage/update-role! test-db (:id trole) trole')
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))))))))

(deftest ^:acceptance group-edit-child-rules-perm-fn
  (testing "group-edit-child-rules? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (let [id (UUID/randomUUID)
              ancs (repeatedly 10 #(UUID/randomUUID))]
          (with-test-login
            (let [prefix (gen-prefix "edit-child-rules")
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns false when subject does not have the permission"
                              (is (not (group-edit-child-rules? subj id ancs)))
                              {:status 200}))]
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))
            (let [prefix (gen-prefix "edit-child-rules")
                  edit-1st-anc-child-rules (rbac-permission :edit-child-rules (first ancs))
                  trole (get-test-role)
                  trole' (update-in trole [:permissions] conj edit-1st-anc-child-rules)
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns true when subject has the permission for an ancestor"
                              (is (group-edit-child-rules? subj id ancs))))]
              (rbac-storage/update-role! test-db (:id trole) trole')
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))))))))

(deftest ^:acceptance group-modify-children-perm-fn
  (testing "group-modify-children? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (let [id (UUID/randomUUID)
              ancs (repeatedly 7 #(UUID/randomUUID))]
          (with-test-login
            (let [prefix (gen-prefix "modify-children")
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns false when subject does not have the permission"
                              (is (not (group-modify-children? subj id ancs)))
                              {:status 200}))]
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))
            (let [prefix (gen-prefix "modify-children")
                  modify-roots-children (rbac-permission :modify-children (last ancs))
                  trole (get-test-role)
                  trole' (update-in trole [:permissions] conj modify-roots-children)
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns true when subject has the permission for an ancestor"
                              (is (group-modify-children? subj id ancs))))]
              (rbac-storage/update-role! test-db (:id trole) trole')
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))))))))

(deftest ^:acceptance permitted-group-actions-fn
  (testing "group-modify-children? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (let [id (UUID/randomUUID)
              ancs (repeatedly 3 #(UUID/randomUUID))]
          (with-test-login
            (let [prefix (gen-prefix "permitted-group-actions")
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns the empty set when user has no permissions"
                              (is (empty? (permitted-group-actions subj id ancs)))
                              {:status 200}))]
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))
            (let [prefix (gen-prefix "permitted-group-actions")
                  perms (for [gid (conj ancs id)
                              :let [action (rand-nth (keys rbac-actions))]]
                          (rbac-permission action gid))
                  permitted-nc-actions (into #{} (map (comp rbac-action->nc-action :action) perms))
                  trole (get-test-role)
                  trole' (update-in trole [:permissions] concat perms)
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing (str "returns the union of permissions for the group and"
                                          " its ancestors")
                              (is (= permitted-nc-actions
                                     (permitted-group-actions subj id ancs)))
                              {:status 200}))]
              (rbac-storage/update-role! test-db (:id trole) trole')
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))))))))

(deftest ^:acceptance viewable-group-ids-fn
  (testing "group-modify-children? rbac permission function"
    (with-app-with-config-with-service-bindings
      app
      test-config
      [authz-svc (get-service app :RbacAuthzService)
       mw-svc (get-service app :RbacAuthnMiddleware)
       ws-svc (get-service app :WebserverService)]
      (with-perm-fns-bound authz-svc
        (let [ids (repeatedly 8 #(UUID/randomUUID))]
          (with-test-login
            (let [prefix (gen-prefix "viewable-group-ids")
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing "returns the empty set when user has no permissions"
                              (is (empty? (viewable-group-ids subj ids)))
                              {:status 200}))]
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))
            (let [prefix (gen-prefix "viewable-group-ids")
                  perms (for [gid ids :when (< (rand) 0.5)]
                          (rbac-permission :view gid))
                  perms (if (empty? perms)
                          [(rbac-permission :view (rand-nth ids))]
                          perms)
                  viewable-ids (into #{} (map (comp #(UUID/fromString %) :instance) perms))
                  trole (get-test-role)
                  trole' (update-in trole [:permissions] concat perms)
                  handler (bound-fn [{subj :shiro-subject}]
                            (testing (str "returns only the viewable ids of those passed to it")
                              (is (= viewable-ids (viewable-group-ids subj ids)))
                              {:status 200}))]
              (rbac-storage/update-role! test-db (:id trole) trole')
              (add-servlet-handler ws-svc (handler->authd-servlet handler mw-svc) prefix
                                   {:server-id :classifier-rbac-authz-test})
              (http/get (str test-base-url prefix)))))))))
