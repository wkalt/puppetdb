(ns puppetlabs.pe-puppetdb-extensions.command
  (:require [puppetlabs.trapperkeeper.core :refer [defservice]]
            [puppetlabs.pe-puppetdb-extensions.storage :as st]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.data :as data]
            [clojure.java.jdbc :as sql]
            [com.rpl.specter :as sp]
            [puppetlabs.puppetdb.command.constants :refer [command-names]]
            [puppetlabs.puppetdb.scf.hash :as shash]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.puppetdb.reports :as reports]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.time :refer [to-timestamp]]
            [clj-time.core :refer [now]]
            [schema.core :as s]
            [puppetlabs.puppetdb.schema :as pls :refer [defn-validated]]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.command :as cmd]
            [puppetlabs.puppetdb.scf.storage :as storage]))

(defn report-command?
  [x]
  (= (:command x) "store report"))

(def timings (atom []))

(defn avg
  [nums]
  (/ (apply + nums) (count nums)))

#_(avg (deref timings))

#_(reset! timings [])

#_(count (deref timings))

(defn parse-timing-str
  [s]
  (read-string (re-find #"\d*\.?\d+" s)))

(defn report-listener
  [db]
  (fn [command]
    (log/info "received pe report")
    (let [timing (with-out-str (time (st/store-report command db)))]
      (swap! timings conj (parse-timing-str (str timing)))
      (log/info (str timing)))))

(defprotocol PeCommandService)

(defservice pe-command-service
  PeCommandService
  [[:PuppetDBServer shared-globals]
   [:DefaultedConfig get-config]
   [:MessageListenerService register-listener]
   PuppetDBCommandDispatcher]

  (start [this context]
         (log/info "starting pe command service")
         (let [{:keys [scf-write-db]} (shared-globals)]
           (register-listener report-command? (report-listener scf-write-db)))
         context))
