(ns puppetlabs.classifier.application.permissioned-test
  (:require [clojure.set :refer [union]]
            [clojure.test :refer :all]
            [clojure.walk :refer [prewalk postwalk]]
            [clj-time.core :as time]
            schema.test
            [slingshot.test]
            [puppetlabs.kitchensink.core :refer [deep-merge mapvals]]
            [puppetlabs.classifier.application :refer [Classifier]]
            [puppetlabs.classifier.application.default :refer [default-application]]
            [puppetlabs.classifier.classification :refer [collapse-to-inherited inherited-rule]]
            [puppetlabs.classifier.rules :refer [always-matches]]
            [puppetlabs.classifier.schema :refer [group->classification]]
            [puppetlabs.classifier.storage :as storage :refer [root-group-uuid PrimitiveStorage]]
            [puppetlabs.classifier.storage.memory :refer [in-memory-storage]]
            [puppetlabs.classifier.application.permissioned :as perm :refer :all]
            [puppetlabs.classifier.test-util :refer [blank-group blank-group-named extract-classes
                                                     vec->tree]]
            [puppetlabs.classifier.util :refer [merge-and-clean]])
  (:import java.util.UUID))

(use-fixtures :once schema.test/validate-schemas)

(defn- find-first-group-in-tree
  [node]
  (or (:group node)
      (some identity (map find-first-group-in-tree (:children node)))))

(defn- group-perm-fn-for-map
  [perm-map]
  (fn [perm-type]
    (fn [token group-id ancs]
      (if (= perm-type :permitted-group-actions)
        (loop [ids (concat [group-id] (map :id ancs)), actions #{}]
          (if-let [id (first ids)]
            (recur (next ids) (union actions (get-in perm-map [token perm-type id])))
            actions))
        (loop [ids (concat [group-id] (map :id ancs))]
          (if-let [id (first ids)]
            (if (get-in perm-map [token perm-type id])
              true
              (recur (next ids)))
            false))))))

(defn- app-with-mapped-permissions
  "Given both a map of the permissions by rbac token, permission name, and then (if
  the permission is for a particular group) by group id that maps to a boolean
  value indicating whether the permission is granted with, and also an instance
  of Classifier, return an instance of PermissionedClassifier that wraps the
  Classifier instance and enforces the given permissions"
  [permissions-by-token mem-store]
  {:pre [(satisfies? PrimitiveStorage mem-store)]}
  (let [token-perm-fn (fn [perm] (fn [t] (get-in permissions-by-token [t perm])))
        all-ids-perm-fn (fn [perm] (fn [t _] (get-in permissions-by-token [t perm])))
        group-perm-fn (group-perm-fn-for-map permissions-by-token)
        permission-fns (merge {:all-group-access? (token-perm-fn :all-group-access?)
                               :viewable-group-ids (all-ids-perm-fn :viewable-group-ids)}
                              (into {} (map (juxt identity group-perm-fn)
                                            [:group-edit-classification?
                                             :group-edit-environment?
                                             :group-edit-child-rules?
                                             :group-modify-children?
                                             :group-view?
                                             :permitted-group-actions])))]
    (app-with-permissions (default-application {:db mem-store}) permission-fns)))

(def ^:private permission-denied :puppetlabs.classifier.application.permissioned/permission-denied)

;; TODO: add tests for classify-node app method, validate-group
(deftest basic-permissions-behavior
  (let [test-node {:name "testnode", :time (time/now), :explanation {}}
        rocket {:name "rocket"
                :environment "space"
                :parameters {:fueled false, :stages 3, :manned true}}
        payload {:name "payload"
                 :environment "space"
                 :parameters {:contains nil, :weight "0 lbs"}}
        root {:name "default"
              :id root-group-uuid
              :environment "production"
              :environment-trumps false
              :parent root-group-uuid
              :rule ["~" "name" ".*"]
              :classes {}
              :variables {:launch_password "1password1"}}
        spaceship {:name "spaceship"
                   :id (UUID/randomUUID)
                   :environment "space"
                   :environment-trumps false
                   :parent root-group-uuid
                   :rule ["=" ["fact" "engine"] "chemical"]
                   :classes {:rocket {:fueled true}}
                   :variables {}}
        spaceship-id (:id spaceship)
        scientific {:name "scientific"
                    :id (UUID/randomUUID)
                    :environment "space"
                    :environment-trumps false
                    :parent (:id spaceship)
                    :rule ["=" ["fact" "operated-for"] "NASA"]
                    :classes {:payload {:contains "science experiments"}}
                    :variables {}}
        scientific-id (:id scientific)
        military {:name "military"
                  :id (UUID/randomUUID)
                  :environment "space"
                  :environment-trumps false
                  :parent (:id spaceship)
                  :rule ["=" ["fact" "operated-for"] "DoD"]
                  :classes {:payload {:contains "frickin' laser beams"}}
                  :variables {}}
        military-id (:id military)
        blank-id (UUID/randomUUID)
        group-ids (map :id [root spaceship scientific military {:id blank-id}])
        all-groups-denied (zipmap group-ids (repeat false))
        all-groups-allowed (zipmap group-ids (repeat true))
        permissions {:joe-shmoe {:all-group-access? false
                                 :group-edit-classification? all-groups-denied
                                 :group-edit-environment? all-groups-denied
                                 :group-edit-child-rules? all-groups-denied
                                 :group-modify-children? all-groups-denied
                                 :group-view? all-groups-denied
                                 :permitted-group-actions (zipmap group-ids (repeat #{}))
                                 :viewable-group-ids #{}}
                     :intern-irving {:all-group-access? false
                                     :group-edit-classification? {root-group-uuid false
                                                                  spaceship-id false
                                                                  scientific-id true
                                                                  military-id false}
                                     :group-edit-environment? all-groups-denied
                                     :group-edit-child-rules? all-groups-denied
                                     :group-modify-children? all-groups-denied
                                     :group-view? {root-group-uuid false
                                                   spaceship-id true
                                                   scientific-id true
                                                   military-id false}
                                     :permitted-group-actions {root-group-uuid #{}
                                                               spaceship-id #{:view}
                                                               scientific-id #{:edit-classification
                                                                               :view}
                                                               military-id #{}}
                                     :viewable-group-ids #{spaceship-id scientific-id}}
                     :admin-addie {:all-group-access? true
                                   :group-edit-classification? all-groups-allowed
                                   :group-edit-environment? all-groups-allowed
                                   :group-edit-child-rules? all-groups-allowed
                                   :group-modify-children? all-groups-allowed
                                   :group-view? all-groups-allowed
                                   :permitted-group-actions (zipmap group-ids
                                                                    (repeat #{:edit-classification
                                                                              :edit-environment
                                                                              :edit-child-rules
                                                                              :modify-children
                                                                              :view}))
                                   :viewable-group-ids (constantly true)}}]

    (testing "joe shmoe should"
      (let [mem-store (in-memory-storage {:classes [rocket payload]
                                          :groups [root spaceship scientific military]})
            perm-app (app-with-mapped-permissions permissions mem-store)
            test-class {:environment "test", :name "prototype alpha", :parameters {:flammable true}}
            tropical-env {:name "tropical"}]

        (testing "have permission to do anything not related to a group or classification history"
          (is (create-class perm-app :joe-shmoe test-class))
          (is (get-class perm-app :joe-shmoe "test" "prototype alpha"))
          (is (get-classes perm-app :joe-shmoe "test"))
          (is (synchronize-classes perm-app :joe-shmoe [rocket payload]))
          (is (nil? (delete-class perm-app :joe-shmoe "test" "prototype alpha")))
          (is (create-environment perm-app :joe-shmoe tropical-env))
          (is (get-environment perm-app :joe-shmoe "tropical"))
          (is (get-environments perm-app :joe-shmoe))
          (is (nil? (delete-environment perm-app :joe-shmoe "tropical"))))

        (testing "not have permission to do anything with a group or classification history"
          (is (thrown+? [:kind permission-denied] (get-check-ins perm-app :joe-shmoe "test-node")))
          (is (thrown+? [:kind permission-denied] (get-nodes perm-app :joe-shmoe)))
          (is (thrown+? [:kind permission-denied] (create-group perm-app :joe-shmoe spaceship)))
          (is (thrown+? [:kind permission-denied] (get-group perm-app :joe-shmoe spaceship-id)))
          (is (empty? (get-groups perm-app :joe-shmoe)))
          (is (thrown+? [:kind permission-denied] (delete-group perm-app :joe-shmoe spaceship-id)))
          (is (thrown+? [:kind permission-denied]
                        (update-group perm-app :joe-shmoe {:id spaceship-id
                                                           :environment "deep space"}))))))

    (testing "intern irving"
      (let [mem-store (in-memory-storage {:classes [rocket payload]
                                          :groups [root spaceship scientific military]})
            perm-app (app-with-mapped-permissions permissions mem-store)]

        (testing "can view most things, and edit the scientific group"
          (is (create-class perm-app, :intern-irving
                            {:name "testclass", :environment "test", :parameters {}}))
          (is (get-class perm-app :intern-irving "test" "testclass"))
          (is (get-classes perm-app :intern-irving "test"))
          (is (synchronize-classes perm-app, :intern-irving
                                   [rocket, payload
                                    {:name "testclass", :environment "test", :parameters {}}]))
          (is (nil? (delete-class perm-app :intern-irving "test" "testclass")))
          (is (create-environment perm-app :intern-irving {:name "staging"}))
          (is (get-environment perm-app :intern-irving "staging"))
          (is (get-environments perm-app :intern-irving))
          (is (nil? (delete-environment perm-app :intern-irving "staging")))

          (is (= spaceship (get-group perm-app :intern-irving spaceship-id)))
          (is (= #{spaceship scientific military} (set (get-groups perm-app :intern-irving))))
          (is (update-group perm-app :intern-irving {:id scientific-id
                                                       :classes {:payload {:weight "6000 lb"}}})))

        (testing "cannot view classification history for any or all nodes"
          (is (thrown+? [:kind permission-denied]
                        (get-check-ins perm-app :intern-irving "testnode")))
          (is (thrown+? [:kind permission-denied] (get-nodes perm-app :intern-irving))))

        (testing "cannot do anything to groups besides view & edit scientific's classification"
          (is (thrown+? [:kind permission-denied]
                        (delete-group perm-app :intern-irving scientific-id)))
          (is (thrown+? [:kind permission-denied]
                        (create-group perm-app :intern-irving spaceship)))
          (is (thrown+? [:kind permission-denied]
                        (update-group perm-app :intern-irving {:id scientific-id
                                                                 :environment "uncharted_space"})))
          (is (thrown+? [:kind permission-denied]
                        (update-group perm-app :intern-irving {:id scientific-id
                                                                 :parent root-group-uuid})))
          (is (thrown+? [:kind permission-denied]
                        (update-group perm-app :intern-irving {:id scientific-id
                                                                 :rule ["~" "name" ".*"]}))))))

    (testing "admin addie should have permission to do everything"
      (let [classes [rocket payload]
            uncharted-space-classes (map #(assoc % :environment "uncharted_space") classes)
            all-classes (concat classes uncharted-space-classes)
            temp-class {:name "testclass", :environment "test", :parameters {}}
            mem-store (in-memory-storage {:groups [root spaceship scientific military]
                                          :classes all-classes})
            perm-app (app-with-mapped-permissions permissions mem-store)]
        (is (get-check-ins perm-app :admin-addie "testnode"))
        (is (get-nodes perm-app :admin-addie))
        (is (create-class perm-app, :admin-addie temp-class))
        (is (= temp-class (get-class perm-app :admin-addie "test" "testclass")))
        (is (some #{temp-class} (get-classes perm-app :admin-addie "test")))
        (is (synchronize-classes perm-app, :admin-addie
                                 (conj all-classes temp-class)))
        (is (nil? (delete-class perm-app :admin-addie "test" "testclass")))
        (is (create-environment perm-app :admin-addie {:name "staging"}))
        (is (get-environment perm-app :admin-addie "staging"))
        (is (get-environments perm-app :admin-addie))
        (is (nil? (delete-environment perm-app :admin-addie "staging")))
        (is (= root (get-group perm-app :admin-addie root-group-uuid)))
        (is (= #{root spaceship scientific military} (set (get-groups perm-app :admin-addie))))
        (let [blank (merge (blank-group) {:id blank-id})]
          (is (create-group perm-app :admin-addie blank))
          (is (nil? (delete-group perm-app :admin-addie blank-id)))
          (is (nil? (get-group perm-app :admin-addie blank-id))))
        (is (update-group perm-app :admin-addie {:id scientific-id
                                                   :classes {:payload {:weight "6000 lb"}}}))
        (is (update-group perm-app :admin-addie {:id scientific-id
                                                   :environment "uncharted_space"}))
        (is (update-group perm-app :admin-addie {:id scientific-id
                                                   :parent root-group-uuid}))
        (is (update-group perm-app :admin-addie {:id scientific-id
                                                   :rule ["~" "name" ".*"]}))))))

(deftest inheritance-and-permissions
  (let [redact (constantly "puppetlabs.classifier/redacted")
        redact-classification (fn [{:keys [classes variables] :as g}]
                                (assoc g
                                       :classes (into {} (for [[class-kw params] classes]
                                                           [class-kw (mapvals redact params)]))
                                       :variables (mapvals redact variables)))
        classes [{:name "oval-office-tapes", :environment "test", :parameters {:transcript ""}}
                 {:name "ze-missiles", :environment "test", :parameters {:launch-code "00000"}}
                 {:name "cafeteria", :environment "test", :parameters {:lunch "pink slime"}}]
        root (merge (blank-group-named "default")
                    {:id root-group-uuid
                     :classes {:oval-office-tapes
                               {:transcript "So first, break into the Watergate Hotel..."}}
                     :variables {:secret-handshake "shake grab bump rocket behind-the-back-bump"}})
        child (merge (blank-group-named "the child")
                     {:parent root-group-uuid
                      :classes {:ze-missiles {:launch-code "00000"}}})
        grandchild (merge (blank-group-named "the grandchild")
                          {:parent (:id child)
                           :classes {:cafeteria {:lunch "meatloaf"}}
                           :variables {:dns_servers ["kohaku.example.com" "ged.example.com"]}})
        ids (map :id [root child grandchild])
        all-groups-denied (zipmap ids (repeat false))]

    (testing "permissions inherit"
      (let [only-root (zipmap ids [true false false])
            admin-permissions {:admin-addie
                               {:all-group-access? true
                                :group-edit-classification? only-root
                                :group-edit-environment? only-root
                                :group-edit-child-rules? only-root
                                :group-modify-children? only-root
                                :group-view? only-root
                                :permitted-group-actions {(:id root) #{:edit-classification
                                                                       :edit-environment
                                                                       :edit-child-rules
                                                                       :modify-children, :view}
                                                          (:id child) #{}
                                                          (:id grandchild) #{}}
                                :viewable-group-ids (constantly true)}}
            sterile-env-classes (map #(assoc % :environment "sterile") classes)
            mem-store (in-memory-storage {:groups [root child grandchild]
                                          :classes (concat classes sterile-env-classes)})
            perm-app (app-with-mapped-permissions admin-permissions mem-store)]
        (is (= child (get-group perm-app :admin-addie (:id child))))
        (is (= grandchild (get-group perm-app :admin-addie (:id grandchild))))
        (is (= #{root child grandchild} (set (get-groups perm-app :admin-addie))))
        (let [other-grandchild (merge (blank-group-named "new grandchild")
                                      {:parent (:id child)})]
          (is (create-group perm-app :admin-addie other-grandchild))
          (is (nil? (delete-group perm-app :admin-addie (:id other-grandchild)))))
        (is (update-group perm-app :admin-addie {:id (:id grandchild)
                                                   :classes {:cafeteria {:lunch "casserole"}}}))
        (is (update-group perm-app :admin-addie {:id (:id grandchild)
                                                   :environment "sterile"}))
        (is (update-group perm-app :admin-addie {:id (:id grandchild)
                                                   :parent root-group-uuid}))
        (is (update-group perm-app :admin-addie {:id (:id grandchild)
                                                   :rule ["~" "name" ".*"]}))))

    (testing "when a group is viewed with inherited parameters and variables, values from groups
             that the subject does not have permission to see are redacted."
      (let [irving-permissions {:intern-irving
                                {:all-group-access? false
                                 :group-edit-classification? all-groups-denied
                                 :group-edit-child-rules? all-groups-denied
                                 :group-modify-children? all-groups-denied
                                 :group-view? {(:id root) false
                                               (:id child) false
                                               (:id grandchild) true}
                                 :permitted-group-actions {(:id root) #{}
                                                           (:id child) #{}
                                                           (:id grandchild) #{:view}}
                                 :viewable-group-ids #{(:id grandchild)}}}
            mem-store (in-memory-storage {:groups [root child grandchild], :classes classes})
            perm-app (app-with-mapped-permissions irving-permissions mem-store)
            redacted-ancestor-class8ns (map (comp group->classification redact-classification)
                                            [root child])
            grandchild-class8n (group->classification grandchild)
            chain (concat [grandchild child root])
            grandchild-with-redacted-values (merge grandchild
                                                   (collapse-to-inherited
                                                     grandchild-class8n
                                                     redacted-ancestor-class8ns)
                                                   {:rule (inherited-rule chain)})]
        (is (= grandchild-with-redacted-values
               (get-group-as-inherited perm-app :intern-irving (:id grandchild))))))))

(deftest child-creation-constraints
  (testing "creating a child group"
    (let [root (merge (blank-group-named "default") {:id root-group-uuid})
          child (merge (blank-group-named "child") {:environment "airless"})
          ids (map :id [root child])
          all-groups-denied (zipmap ids (repeat false))
          only-child (zipmap ids [false true])
          permissions {:operator-owen
                       {:all-group-access? false
                        :group-edit-classification? all-groups-denied
                        :group-edit-environment? all-groups-denied
                        :group-edit-child-rules? all-groups-denied
                        :group-modify-children? only-child
                        :group-view? only-child
                        :permitted-group-actions {(:id root) #{}
                                                  (:id child) #{:modify-children
                                                                :view}}
                        :viewable-group-ids #{(:id child)}}}
          mem-store (in-memory-storage {:groups [root child]})
          perm-app (app-with-mapped-permissions permissions mem-store)]

      (testing "is denied if it has a different environment than its parent but the user does not
               have permission to edit the created group's environment"
        (let [diff-env (merge (blank-group-named "env grandchild")
                              {:parent (:id child)
                               :environment "high-altitude"
                               :rule always-matches})
              same-env (assoc diff-env :environment (:environment child))]
          (is (thrown+? [:kind permission-denied]
                        (create-group perm-app :operator-owen diff-env)))
          (is (= same-env (create-group perm-app :operator-owen same-env)))))

      (testing "is allowed if it has a rule that doesn't match all nodes but the user does not have
               permission to edit the rules of the children of the to-be-created group's parent"
        (let [custom-rule (merge (blank-group-named "rule grandchild")
                                 {:parent (:id child)
                                  :rule [">" "'tude" "3.65"]
                                  :environment (:environment child)})]
          (is (= custom-rule (create-group perm-app :operator-owen custom-rule)))))

      (testing "is allowed if it has classification values but the user does not have permission
               to edit the created group's classification"
        (let [w-class8n (merge (blank-group-named "class8n grandchild")
                               {:parent (:id child)
                                :variables {:foo "bar"}
                                :environment (:environment child)
                                :rule always-matches})
              wout-class8n (assoc w-class8n :variables {})]
          (is (= w-class8n (create-group perm-app :operator-owen w-class8n))))))))

(deftest implicit-permissions
  (testing "if you have modify-children permissions for the ancestor of a group"
    (let [root (merge (blank-group-named "default") {:id root-group-uuid})
          child (blank-group-named "child")
          grandchild (merge (blank-group-named "grandchild")
                            {:parent (:id child)})
          ids (map :id [root child grandchild])
          all-groups-denied (zipmap ids (repeat false))
          only-child (zipmap ids [false true false])
          permissions {:operator-owen
                       {:classifier-access? true
                        :group-edit-classification? all-groups-denied
                        :group-edit-environment? all-groups-denied
                        :group-edit-child-rules? all-groups-denied
                        :group-modify-children? only-child
                        :group-view? only-child
                        :permitted-group-actions {(:id root) #{}
                                                  (:id child) #{:modify-children
                                                                :view}
                                                  (:id grandchild) #{}}
                        :viewable-group-ids #{(:id child)}}}
          new-perm-app #(app-with-mapped-permissions
                          permissions
                          (in-memory-storage {:groups [root child grandchild]}))]

      (testing "editing the group's rule is allowed"
        (let [rule-delta {:id (:id grandchild)
                          :rule [">" "'tude" "35"]}
              grandchild' (merge-and-clean grandchild rule-delta)]
          (is (= grandchild' (update-group (new-perm-app) :operator-owen rule-delta)))))

      (testing "editing the group's classification is allowed"
        (let [class8n-delta {:id (:id grandchild)
                             :variables {:too_cool_for_school true}}
              grandchild' (merge-and-clean grandchild class8n-delta)]
          (is (= grandchild' (update-group (new-perm-app) :operator-owen class8n-delta))))))))

(deftest spurious-update-errors
  (let [root (merge (blank-group-named "default") {:id root-group-uuid})
        child (blank-group-named "child")
        ids (map :id [root child])
        all-groups-denied (zipmap ids (repeat false))
        only-child (zipmap ids [false true])
        permissions {:operator-owen
                     {:classifier-access? true
                      :group-edit-classification? only-child
                      :group-edit-environment? all-groups-denied
                      :group-edit-child-rules? all-groups-denied
                      :group-modify-children? all-groups-denied
                      :group-view? only-child
                      :permitted-group-actions {(:id root) #{}
                                                (:id child) #{:edit-classification :view}}
                      :viewable-group-ids #{(:id child)}}}
        mem-store (in-memory-storage {:groups [root child]})
        perm-app (app-with-mapped-permissions permissions mem-store)]

    (testing "Updates that don't actually change a value don't throw a spurious permissions error"
      (let [spurious-environment-delta {:id (:id child)
                                        :environment (:environment child)
                                        :variables {:foo "bar"}}
            spurious-parent-delta {:id (:id child)
                                   :parent (:parent child)
                                   :variables {:foo "baz"}}
            spurious-rule-delta {:id (:id child)
                                 :rule (:rule child)
                                 :variables {:foo "qux"}}]
        (is (= (merge child spurious-environment-delta)
               (update-group perm-app :operator-owen spurious-environment-delta)))
        (is (= (merge child spurious-parent-delta)
               (update-group perm-app :operator-owen spurious-parent-delta)))
        (is (= (merge child spurious-rule-delta)
               (update-group perm-app :operator-owen spurious-rule-delta)))))))

(deftest explanation-scrubbing
  (let [node {:name "test-node", :fact {}, :trusted {}}
        root (assoc (blank-group-named "default")
                    :id root-group-uuid
                    :rule ["~" "name" ".*"])
        invis-gp (assoc (blank-group-named "elusive septuagenarian")
                        :parent root-group-uuid
                        :variables {:ancient-wisdom "ineffable"}
                        :classes {:the-bomb {:activation-code "00000"}}
                        :rule ["=" "name" "test-node"])
        vis-parent (assoc (blank-group-named "helicopter dad")
                          :parent (:id invis-gp)
                          :variables {:jokes "bad"}
                          :classes {:wardrobe {:decade "nineties"}}
                          :rule ["=" "name" "test-node"])
        vis-child (assoc (blank-group-named "wrathful toddler")
                         :parent (:id vis-parent)
                         :variables {:volume 95}
                         :classes {:waste-disposal {:method "diapers"}}
                         :rule ["=" "name" "test-node"])
        invis-leaf (assoc (blank-group-named "spook country")
                          :parent root-group-uuid
                          :variables {:courts "we can't even tell ya"}
                          :classes {:surveillance {:things :all}}
                          :rule ["=" "name" "test-node"])
        groups [root invis-gp vis-parent vis-child invis-leaf]
        ids (map :id groups)
        all-groups-denied (zipmap ids (repeat false))
        permissions {:operator-owen
                     {:classifier-access? true
                      :group-edit-classification? all-groups-denied
                      :group-edit-environment? all-groups-denied
                      :group-edit-child-rules? all-groups-denied
                      :group-modify-children? all-groups-denied
                      :permitted-group-actions {(:id root) #{}
                                                (:id invis-gp) #{}
                                                (:id vis-parent) #{:view}
                                                (:id vis-child) #{:view}
                                                (:id invis-leaf) #{}}
                      :group-view {(:id root) false
                                   (:id invis-gp) false
                                   (:id vis-parent) true
                                   (:id vis-child) true
                                   (:id invis-leaf) false}
                      :viewable-group-ids #{(:id vis-parent) (:id vis-child)}}}
        perm-app-w-groups #(app-with-mapped-permissions
                             permissions
                             (in-memory-storage {:groups % :classes (extract-classes %)}))]

    (let [perm-app (perm-app-w-groups groups)
          exp (explain-classification perm-app :operator-owen node)
          {:keys [leaf-groups inherited-classifications final-classification]} exp]

      (testing "leaf groups token can't see are redacted"
        (is (= {:name redacted-str
                :environment redacted-str
                :classes {:surveillance {:things redacted-str}}
                :variables {:courts redacted-str}}
               (-> (get leaf-groups (:id invis-leaf))
                 (select-keys [:name :environment :classes :variables])))))

      (testing "values in leaf classifications inherited from invisible groups are redacted"
        (is (= {:environment (:environment vis-child)
                :environment-trumps false
                :classes {:the-bomb {:activation-code redacted-str}
                          :wardrobe {:decade "nineties"}
                          :waste-disposal {:method "diapers"}}
                :variables {:ancient-wisdom redacted-str
                            :jokes "bad"
                            :volume 95}}
               (get inherited-classifications (:id vis-child)))))

      (testing "values in final classification inherited from invisible groups are redacted"
        (is (= {:environment (:environment vis-child)
                :classes {:the-bomb {:activation-code redacted-str}
                          :wardrobe {:decade "nineties"}
                          :waste-disposal {:method "diapers"}
                          :surveillance {:things redacted-str}}
                :variables {:ancient-wisdom redacted-str
                            :jokes "bad"
                            :volume 95
                            :courts redacted-str}}
               final-classification))))

    (let [conflictor (assoc invis-leaf
                            :classes {:the-bomb {:activation-code "99999"}
                                      :wardrobe {:decade "sixties"}}
                            :variables {:volume 10
                                        :jokes "chuckle-worthy"})
          groups' [root invis-gp vis-parent vis-child conflictor]
          perm-app (perm-app-w-groups groups')
          {:keys [conflicts]} (explain-classification perm-app :operator-owen node)]

      (testing "conflicting values defined by invisible groups are redacted"
        (is (= {:classes
                {:the-bomb {:activation-code
                            #{{:value redacted-str
                               :from (#'perm/redact-group conflictor)
                               :defined-by (#'perm/redact-group conflictor)}
                              {:value redacted-str
                               :from vis-child
                               :defined-by (#'perm/redact-group invis-gp)}}}
                 :wardrobe {:decade
                            #{{:value redacted-str
                               :from (#'perm/redact-group conflictor)
                               :defined-by (#'perm/redact-group conflictor)}
                              {:value "nineties", :from vis-child, :defined-by vis-parent}}}}
                :variables {:jokes
                            #{{:value redacted-str
                               :from (#'perm/redact-group conflictor)
                               :defined-by (#'perm/redact-group conflictor)}
                              {:value "bad", :from vis-child, :defined-by vis-parent}}
                            :volume
                            #{{:value redacted-str
                               :from (#'perm/redact-group conflictor)
                               :defined-by (#'perm/redact-group conflictor)}
                              {:value 95, :from vis-child, :defined-by vis-child}}}}
               conflicts))))))
