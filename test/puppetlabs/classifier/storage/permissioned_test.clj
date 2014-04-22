(ns puppetlabs.classifier.storage.permissioned-test
  (:require [clojure.test :refer :all]
            [slingshot.test]
            [puppetlabs.classifier.storage :refer [root-group-uuid]]
            [puppetlabs.classifier.storage.memory :refer [in-memory-storage]]
            [puppetlabs.classifier.storage.permissioned :refer :all])
  (:import java.util.UUID))

(defn- find-first-group-in-tree
  [node]
  (or (:group node)
      (some identity (map find-first-group-in-tree (:children node)))))

(deftest permission-checking
  (let [rocket {:name "rocket"
                :environment "space"
                :parameters {:fueled false, :stages 3, :manned true}}
        payload {:name "payload"
                 :environment "space"
                 :parameters {:contains nil}}
        root {:name "default"
              :id root-group-uuid
              :environment "production"
              :parent root-group-uuid
              :rule ["~" "name" ".*"]
              :classes {}
              :variables {}}
        spaceship {:name "spaceship"
                   :id (UUID/randomUUID)
                   :environment "space"
                   :parent root-group-uuid
                   :rule ["=" ["fact" "engine"] "chemical"]
                   :classes {:rocket {:fueled true}}
                   :variables {}}
        spaceship-id (:id spaceship)
        scientific {:name "scientific"
                    :id (UUID/randomUUID)
                    :environment "space"
                    :parent (:id spaceship)
                    :rule ["=" ["fact" "operated-for"] "NASA"]
                    :classes {:payload {:contains "science experiments"}}
                    :variables {}}
        scientific-id (:id scientific)
        military {:name "military"
                  :id (UUID/randomUUID)
                  :environment "space"
                  :parent (:id spaceship)
                  :rule ["=" ["fact" "operated-for"] "DoD"]
                  :classes {:payload {:contains "frickin' laser beams"}}
                  :variables {}}
        military-id (:id military)
        group-ids (map :id [root spaceship scientific military])
        mem-store (in-memory-storage {:classes [rocket payload]
                                      :groups [root spaceship scientific]})
        all-groups-denied (zipmap group-ids (repeat false))
        all-groups-allowed (zipmap group-ids (repeat true))
        permissions {:joe-shmoe {:classifier-access? false
                                 :group-create? false
                                 :group-delete? all-groups-denied
                                 :group-edit-classification? all-groups-denied
                                 :group-edit-environment? all-groups-denied
                                 :group-edit-parent? all-groups-denied
                                 :group-edit-rules? all-groups-denied
                                 :group-view? all-groups-denied
                                 :permitted-group-actions []
                                 :viewable-group-ids []}
                     :intern-irving {:classifier-access? true
                                     :group-create? false
                                     :group-delete? all-groups-denied
                                     :group-edit-classification? {spaceship-id false
                                                                  scientific-id true
                                                                  military-id false}
                                     :group-edit-environment? all-groups-denied
                                     :group-edit-parent? all-groups-denied
                                     :group-edit-rules? all-groups-denied
                                     :group-view? {spaceship-id true
                                                   scientific-id true
                                                   military-id false}
                                     :permitted-group-actions {spaceship-id [:view]
                                                               scientific-id [:view :configure]
                                                               military-id []}
                                     :viewable-group-ids [spaceship-id scientific-id]}
                     :admin-addie {:classifier-access? true
                                   :group-create? true
                                   :group-delete? all-groups-allowed
                                   :group-edit-classification? all-groups-allowed
                                   :group-edit-environment? all-groups-allowed
                                   :group-edit-rules? all-groups-allowed
                                   :group-view? all-groups-allowed
                                   :permitted-group-actions (zipmap group-ids [:admin :configure
                                                                               :create :edit_env
                                                                               :edit_parent
                                                                               :edit_rules :view])
                                   :viewable-group-ids group-ids}}
        token-perm-fn (fn [perm] (fn [t] (get-in permissions [t perm])))
        group-perm-fn (fn [perm] (fn [t id] (get-in permissions [t perm id])))
        permission-fns (merge (into {} (map (juxt identity token-perm-fn)
                                            [:classifier-access?
                                             :group-create?
                                             :viewable-group-ids]))
                              (into {} (map (juxt identity group-perm-fn)
                                            [:group-delete?
                                             :group-edit-classification?
                                             :group-edit-environment?
                                             :group-edit-rules?
                                             :group-view?
                                             :permitted-group-actions])))
        perm-store (storage-with-permissions mem-store permission-fns)
        permission-denied :puppetlabs.classifier.storage.permissioned/permission-denied]

    (testing "joe shmoe should not have permission to do anything"
      (is (thrown+? [:kind permission-denied] (store-check-in perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (get-check-ins perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (get-nodes perm-store :joe-shmoe)))
      (is (thrown+? [:kind permission-denied] (create-class perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (get-class perm-store :joe-shmoe nil nil)))
      (is (thrown+? [:kind permission-denied] (get-classes perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (synchronize-classes perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (delete-class perm-store :joe-shmoe nil nil)))
      (is (thrown+? [:kind permission-denied] (get-rules perm-store :joe-shmoe)))
      (is (thrown+? [:kind permission-denied] (create-environment perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (get-environment perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (get-environments perm-store :joe-shmoe)))
      (is (thrown+? [:kind permission-denied] (delete-environment perm-store :joe-shmoe nil)))
      (is (thrown+? [:kind permission-denied] (create-group perm-store :joe-shmoe spaceship)))
      (is (thrown+? [:kind permission-denied] (get-group perm-store :joe-shmoe spaceship-id)))
      (is (= [] (get-groups perm-store :joe-shmoe)))
      (is (= [] (get-ancestors perm-store :joe-shmoe scientific-id)))
      (is (nil? (find-first-group-in-tree (get-subtree perm-store :joe-shmoe spaceship))))
      (is (thrown+? [:kind permission-denied] (delete-group perm-store :joe-shmoe spaceship-id)))
      (is (thrown+? [:kind permission-denied]
                    (update-group perm-store :joe-shmoe {:id spaceship-id
                                                         :environment "deep space"}))))


    ))
