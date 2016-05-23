(ns puppetlabs.pe-puppetdb-extensions.command
  (:require [puppetlabs.trapperkeeper.core :refer [defservice]]
            [clojure.tools.logging :as log]
             [clj-time.core :refer [now]]
             [puppetlabs.puppetdb.command :as cmd]
             [puppetlabs.puppetdb.scf.storage :as storage]))

(defn report-command?
  [x]
  (= (:command x) "store report"))

(defn report-listener
  [db]
  (fn [command]
    (log/info "received pe report")
    (storage/add-report! command (now))))


(defprotocol PeCommandService)

(defservice pe-command-service
  PeCommandService
  [[:PuppetDBServer shared-globals]
   [:DefaultedConfig get-config]
   [:MessageListenerService register-listener]]

  (start [this context]
         (log/info "starting pe command service")
         (let [{:keys [scf-write-db]} (shared-globals)]
           (register-listener report-command? (report-listener scf-write-db)))
         context))

