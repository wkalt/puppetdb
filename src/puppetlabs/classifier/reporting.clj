(ns puppetlabs.classifier.reporting)

(def service-id "classifier")

(defn- delta-parent-event
  [delta]
  (if (contains? delta :parent)
    {:type "edit", :what "node_group_parent", :description "edit_node_group"
     :message (str "Change the parent to " (:parent delta))}))

(defn- delta-rule-event
  [delta]
  (if (contains? delta :rule)
    {:type "edit", :what "node_group_rule", :description "edit_node_group"
     :message (str "Change the rule to " (pr-str (:rule delta)))}))

(defn- delta-environment-event
  [delta]
  (if (contains? delta :environment)
    {:type "edit", :what "node_group_environment", :description "edit_node_group"
     :message (str "Change the environment to " (pr-str (:environment delta)))}))

(defn- delta-name-event
  [delta]
  (if (contains? delta :name)
    {:type "edit", :what "node_group_name", :description "edit_node_group"
     :message (str "Change the name to " (pr-str (:name delta)))}))

(defn- delta-classes-events
  [delta]
  (->> (for [[env classes] (:classes delta)
             [class params] classes
             :let [class-name (name class)]]
         (cond
           (nil? params)
           [{:type "edit" :what "node_group_class" :description "delete_node_group_class"
             :message (str "Remove the " (pr-str class-name) " class")}]

           (empty? params)
           [{:type "edit" :what "node_group_class" :description "add_node_group_class"
             :message (str "Add the " (pr-str class-name) " class")}]

           :else (for [[param value] params]
                   (if (nil? value)
                     {:type "edit" :what "node_group_class_parameter"
                      :description "delete_node_group_class_parameter"
                      :message (str "Remove the " (pr-str (name param)) " parameter from the "
                                    (pr-str class-name) " class")}
                     {:type "edit" :what "node_group_class_parameter"
                      :description "edit_node_group_class_parameter"
                      :message (str "Change the value of the " (pr-str (name param))
                                    " parameter of the " (pr-str class-name) " class")}))))
    (apply concat)))

(defn- delta-variables-events
  [delta]
  (for [[variable value] (:variables delta)
        :let [var-name (name variable)]]
    (if (nil? value)
      {:type "edit" :what "node_group_variable" :description "delete_node_group_variable"
       :message (str "Remove the " (pr-str var-name) " variable")}
      {:type "edit" :what "node_group_variable" :description "edit_node_group_variable"
       :message (str "Change the value of the " (pr-str var-name) " variable")})))

(defn delta->events
  [delta]
  (if (nil? delta)
    [{:type "delete", :what "node_group", :description "delete_node_group"
      :message "Delete the group"}]
    (let [primitive-field-events (juxt delta-parent-event delta-rule-event delta-environment-event
                                       delta-name-event)]
      (->> (concat (primitive-field-events delta)
                   (delta-classes-events delta)
                   (delta-variables-events delta))
        (keep identity)))))

(defn bundle-besides-events
  [shiro-subject group]
  {:commit
   {:service {:id service-id}
    :subject shiro-subject
    :object {:type "node_group" :id (-> (:id group) str) :name (:name group)}}})

(defn delta-report
  [shiro-subject delta group]
  (assoc (bundle-besides-events shiro-subject group)
         :events (delta->events delta)))

(defn creation-report
  [shiro-subject group]
  (assoc (bundle-besides-events shiro-subject group)
         :events [{:type "create", :what "node_group", :description "create_node_group"
                   :message (str "Create the " (pr-str (:name group)) " group"
                                 " with id " (:id group))}]))

(defn deletion-report
  [shiro-subject group]
  (assoc (bundle-besides-events shiro-subject group)
         :events [{:type "delete" :what "node_group" :description "delete_node_group"
                   :message (str "Delete the " (pr-str (:name group) " group")
                                 " with id " (:id group))}]))
