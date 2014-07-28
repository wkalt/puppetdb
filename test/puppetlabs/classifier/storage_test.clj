(ns puppetlabs.classifier.storage-test
  (:require [clojure.test :refer :all]
            [puppetlabs.classifier.storage :refer :all]
            [puppetlabs.classifier.storage.memory :refer [in-memory-storage]]
            [puppetlabs.classifier.test-util :refer [blank-group-named]]))

(deftest ^:database get-multiple-groups-ancestors
  (let [root (assoc (blank-group-named "default") :id root-group-uuid)
        ;;   (root)
        ;;   /  | \        ; This is the hierarchy for this test.
        ;; (a) (b) (c)     ; We pass all the leaves to the get-groups-ancestor
        ;;     /   / \     ; storage method, and expect the map we get back to
        ;;   (d) (e) (f)   ; contain all the groups.
        ;;           / \
        ;;         (g) (h)
        a (blank-group-named "a")
        b (blank-group-named "b")
        c (blank-group-named "c")
        d (assoc (blank-group-named "d") :parent (:id b))
        e (assoc (blank-group-named "e") :parent (:id c))
        f (assoc (blank-group-named "f") :parent (:id c))
        g (assoc (blank-group-named "g") :parent (:id f))
        h (assoc (blank-group-named "h") :parent (:id f))
        leaves [a d e g h]
        leaf->ancestors {a [root]
                         d [b root]
                         e [c root]
                         g [f c root]
                         h [f c root]}
        db (in-memory-storage {:groups [root a b c d e f g h]})]

    (testing "getting multiple groups' ancestors at once returns the expected result"
      (dotimes [_ 1e2]
        (is (= leaf->ancestors (get-groups-ancestors db (shuffle leaves))))))))
