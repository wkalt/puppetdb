(ns puppetlabs.classifier.http
  (:require [compojure.core :refer [defroutes GET]]
            [compojure.route :as route]))

(defroutes app
  (GET "/v1/node/:node" [node]
    {:status 200
     :headers {"content-type" "text/plain"}
     :body (str "node: " node "\n" "class: Foo")})
  )
