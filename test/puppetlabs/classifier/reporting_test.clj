(ns puppetlabs.classifier.reporting-test
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [puppetlabs.classifier.reporting :refer :all]))

(defn- event
  [t what desc msg]
  {:type t :what what :description desc :message msg})

(deftest classes-events
  (let [delta {:classes
               {:desert {:island {}}
                :tropical {:paradise nil}
                :taiga {:eastern-siberian-taiga {:pines nil
                                                 :spruces nil
                                                 :larches true}}}}]
    (testing "class activity events from a delta"
      (is (= #{(event "edit" "node_group_class" "add_node_group_class" "Add the \"island\" class")
               (event "edit" "node_group_class" "delete_node_group_class" "Remove the \"paradise\" class")
               (event "edit" "node_group_class_parameter" "delete_node_group_class_parameter"
                      "Remove the \"pines\" parameter from the \"eastern-siberian-taiga\" class")
               (event "edit" "node_group_class_parameter" "delete_node_group_class_parameter"
                      "Remove the \"spruces\" parameter from the \"eastern-siberian-taiga\" class")
               (event "edit" "node_group_class_parameter" "edit_node_group_class_parameter"
                      "Change the value of the \"larches\" parameter of the \"eastern-siberian-taiga\" class")}
             (set (delta->events delta)))))))

(deftest variables-events
  (let [delta {:variables {:w nil, :x 42, :y 69, :z nil}}]
    (testing "variable activity events from a delta"
      (is (= #{(event "edit" "node_group_variable" "delete_node_group_variable" "Remove the \"w\" variable")
               (event "edit" "node_group_variable" "edit_node_group_variable" "Change the value of the \"x\" variable")
               (event "edit" "node_group_variable" "edit_node_group_variable" "Change the value of the \"y\" variable")
               (event "edit" "node_group_variable" "delete_node_group_variable" "Remove the \"z\" variable")}
             (set (delta->events delta)))))))
