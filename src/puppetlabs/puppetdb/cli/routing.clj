(ns puppetlabs.puppetdb.cli.routing
  (:require [puppetlabs.puppetdb.scf.storage :as scf-store]
            [puppetlabs.trapperkeeper.core :refer [defservice main]]
            [puppetlabs.trapperkeeper.services :refer [service-id]]
            [compojure.core :refer :all]
            [compojure.route :as route]))

(defroutes app2
  (GET "/" [] "hello world")
  (route/not-found "not found"))

