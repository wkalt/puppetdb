(ns puppetlabs.puppetdb-sync.services
  (:require [clojure.tools.logging :as log]
            [puppetlabs.trapperkeeper.services :refer [get-services]]
            [puppetlabs.puppetdb.cheshire :as json]  
            [puppetlabs.puppetdb-sync.command :as command]
            [puppetlabs.trapperkeeper.core :refer [defservice main]]))

(defn foo-pred
  [x]
  (= (:command x) "sync"))

(defn foo-listener
  [{:keys [payload version]}]
  (command/puppetdb-sync* "localhost" 8080 payload)
  (log/info "hit the endpoint"))

(defservice foo-service
  [[:PuppetDBServer shared-globals]
   [:MessageListenerService register-listener]]
  (start [this context]
         (let [{:keys [scf-write-db catalog-hash-debug-dir]} (shared-globals)]
           (register-listener foo-pred
                              foo-listener)
                              context)))
