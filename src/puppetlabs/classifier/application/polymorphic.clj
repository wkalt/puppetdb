(ns puppetlabs.classifier.application.polymorphic
  "A polymorphic wrapper around Classifier and PermissionedClassifier."
  (:require [puppetlabs.classifier.application :as app :refer [Classifier]]
            [puppetlabs.classifier.application.permissioned :as permd-app
                                                            :refer [PermissionedClassifier]]))

(defmacro defpolymorphic
  [method-name]
  (let [plain-method (symbol "puppetlabs.classifier.application" (str method-name))
        permd-method (symbol "puppetlabs.classifier.application.permissioned" (str method-name))]
    `(defn ~method-name
       [~'app ~'token & ~'args]
       (if (satisfies? PermissionedClassifier ~'app)
         (apply ~permd-method ~'app ~'token ~'args)
         (apply ~plain-method ~'app ~'args)))))

(defn get-config
  [app]
  (if (satisfies? Classifier app)
    (app/get-config app)
    (permd-app/get-config app)))

(defpolymorphic classify-node)

(defpolymorphic explain-classification)

(defpolymorphic get-check-ins)

(defpolymorphic get-nodes)

(defpolymorphic validate-group)

(defpolymorphic create-group)

(defpolymorphic get-group)

(defpolymorphic get-group-as-inherited)

(defpolymorphic get-groups)

(defpolymorphic update-group)

(defpolymorphic delete-group)

(defpolymorphic import-hierarchy)

(defpolymorphic create-class)

(defpolymorphic get-class)

(defpolymorphic get-classes)

(defpolymorphic get-all-classes)

(defpolymorphic synchronize-classes)

(defpolymorphic get-last-sync)

(defpolymorphic delete-class)

(defpolymorphic create-environment)

(defpolymorphic get-environment)

(defpolymorphic get-environments)

(defpolymorphic delete-environment)
