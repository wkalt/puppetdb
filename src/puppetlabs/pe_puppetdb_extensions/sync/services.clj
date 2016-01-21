(ns puppetlabs.pe-puppetdb-extensions.sync.services
  (:import [org.joda.time Period]
           [java.net URI])
  (:require [clj-time.core :as time]
            [clojure.tools.logging :as log]
            [overtone.at-at :as atat]
            [metrics.reporters.jmx :as jmx-reporter]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.structured-logging.core :refer [maplog]]
            [puppetlabs.puppetdb.time :refer [to-millis periods-equal? parse-period period?]]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.pe-puppetdb-extensions.sync.core :refer [sync-from-remote! with-trailing-slash]]
            [puppetlabs.pe-puppetdb-extensions.sync.bucketed-summary :as bucketed-summary]
            [puppetlabs.pe-puppetdb-extensions.sync.events :as events]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [puppetlabs.trapperkeeper.services :refer [get-service service-context]]
            [puppetlabs.puppetdb.utils :as utils :refer [throw+-cli-error!]]
            [schema.core :as s]
            [clj-time.coerce :refer [to-date-time]]
            [slingshot.slingshot :refer [throw+ try+]]
            [com.rpl.specter :as sp]
            [clojure.core.async :as async]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.comidi :as cmdi]
            [puppetlabs.puppetdb.middleware :as mid]))

(defprotocol PuppetDBSync
  (bucketed-summary-query [this entity]))

(def currently-syncing (atom false))

(def sync-request-schema {:remote_host_path s/Str})

(defn remote-url->server-url
  "Given a 'remote-url' of form <path>/pdb/query/v4, return <path>. Currently
   used for logging and tests."
  [remote-url]
  (let [uri (URI. remote-url)
        prefix (str (.getScheme uri) "://" (.getHost uri) ":" (.getPort uri))
        suffix (re-find #"^\/[a-zA-Z0-9-_\/]*(?=/pdb/query/v4)" (.getPath uri))]
    (str prefix suffix)))

(defn- query-endpoint
  "Given a base server-url, put the query endpoint onto the end of it. Don't do
  this if it looks like one is already there. "
  [^String server-url]
  (if (.contains (str server-url) "/v4")
    server-url
    (str (with-trailing-slash (str server-url)) "pdb/query/v4")))

(defn make-remote-server
  "Create a remote-server structure out the given url, pulling ssl options from
   jetty-config."
  [remote-config jetty-config]
  (let [server-url (str (:server-url remote-config))]
    (-> (select-keys jetty-config [:ssl-cert :ssl-key :ssl-ca-cert])
        (assoc :url (query-endpoint server-url)))))

(defn- sync-with!
  [remote-server
   query-fn
   bucketed-summary-query-fn
   enqueue-command-fn
   node-ttl]
  (if (compare-and-set! currently-syncing false true)
    (try (sync-from-remote! query-fn bucketed-summary-query-fn
                            enqueue-command-fn remote-server node-ttl)
         {:status 200 :body "success"}
         (catch Exception ex
           (let [err "Remote sync from %s failed"
                 url (:url remote-server)]
             (maplog [:sync :error] ex
                            {:remote url :phase "sync"}
                            (format err url))
             (log/errorf ex err url)
             {:status 200 :body (format err url)}))
         (finally (swap! currently-syncing (constantly false))))
    (let [err "Refusing to sync from %s. Sync already in progress."
          url (:url remote-server)]
      (maplog [:sync :info]
                     {:remote url :phase "sync"}
                     (format err url))
      (log/infof err url)
      {:status 200 :body (format err url)})))

(defn enable-periodic-sync? [remotes]
  (cond
    (or (nil? remotes) (zero? (count remotes)))
    (do
      (log/warn "No remotes specified, sync disabled")
      false)

    :else
    (let [interval (-> remotes first (get :interval ::none))]
      (cond
        (= interval ::none)
        false

        (not (period? interval))
        (throw+-cli-error!
          (format "Specified sync interval %s does not appear to be a time period" interval))

        (neg? (time/in-millis interval))
        (throw+-cli-error! (str "Sync interval must be positive or zero: " interval))

        (periods-equal? interval Period/ZERO)
        (do (log/warn "Zero sync interval specified, disabling sync.")
            false)

        :else true))))

(defn validate-trigger-sync
  "Validates `remote-server' as a valid sync target given user config items"
  [allow-unsafe-sync-triggers remotes-config jetty-config remote-server]
  (let [valid-remotes (map #(make-remote-server % jetty-config) remotes-config)]
    (or allow-unsafe-sync-triggers
        (ks/seq-contains? valid-remotes remote-server))))

(defn wait-for-sync
  [submitted-commands-chan processed-commands-chan process-command-timeout-ms]
  (async/go-loop [pending-commands #{}
                  done-submitting-commands false]
    (cond
      (not done-submitting-commands)
      (async/alt! submitted-commands-chan
                  ([message]
                   (if message
                     ;; non-nil: a command was submitted
                     (recur (conj pending-commands (:id message)) done-submitting-commands)
                     ;; the channel was closed, so we're done submitting commands
                     (recur pending-commands true)))

                  processed-commands-chan
                  ([message]
                   (if message
                     (recur (disj pending-commands (:id message)) done-submitting-commands)
                     :shutting-down))

                  :priority true)

      (empty? pending-commands)
      :done

      :else
      (async/alt! processed-commands-chan
                  ([message]
                   (if message
                     (recur (disj pending-commands (:id message)) done-submitting-commands)
                     :shutting-down))

                  (async/timeout process-command-timeout-ms)
                  :timed-out

                  :priority true))))

(defn blocking-sync
  [remote-server query-fn bucketed-summary-query-fn
   enqueue-command-fn node-ttl response-mult]
  (let [remote-url (:url remote-server)
        submitted-commands-chan (async/chan)
        processed-commands-chan (async/chan 10000)
        _ (async/tap response-mult processed-commands-chan)
        finished-sync (wait-for-sync submitted-commands-chan processed-commands-chan 15000)]
    (try
      (sync-from-remote! query-fn bucketed-summary-query-fn enqueue-command-fn remote-server node-ttl submitted-commands-chan)
      (async/close! submitted-commands-chan)
      (maplog [:sync :info] {:remote remote-url}
              "Done submitting local commands for blocking sync. Waiting for commands to finish processing...")
      (let [result (async/<!! finished-sync)]
        (case result
          :done
          (maplog [:sync :info] {:remote remote-url :result (name result)}
                  "Successfully finished blocking sync")
          :shutting-down
          (maplog [:sync :warn] {:remote remote-url :result (name result)}
                  "blocking sync interrupted by shutdown")
          :timed-out
          (throw+ {:type ::message-processing-timeout}
                  "The blocking sync with the remote system timed out because of slow message processing.")))
      (finally
        (async/close! submitted-commands-chan)
        (async/untap response-mult processed-commands-chan)))))

(defn sync-handler
  "Top level route for PuppetDB sync"
  [get-config query-fn bucketed-summary-query-fn
   enqueue-command-fn response-mult get-shared-globals]
  (let [{{node-ttl :node-ttl} :database
         sync-config :sync
         jetty-config :jetty} (get-config)
        allow-unsafe-sync-triggers (:allow-unsafe-sync-triggers sync-config)
        remotes-config (:remotes sync-config)
        validate-sync-fn (partial validate-trigger-sync
                                  allow-unsafe-sync-triggers
                                  remotes-config
                                  jetty-config)]
    (mid/make-pdb-handler
      (cmdi/context "/v1"
                    (cmdi/GET "/reports-summary" []
                              (let [summary-map (bucketed-summary-query-fn :reports)]
                                (http/json-response (ks/mapkeys str summary-map))))

                    (cmdi/POST
                      "/trigger-sync" {:keys [body params] :as request}
                      ;; Note: This handler is only used in tests.
                      (let [sync-request (json/parse-string (slurp body) true)
                            remote-url (:remote_host_path sync-request)
                            completion-timeout-ms (some-> params
                                                          (get "secondsToWaitForCompletion")
                                                          Double/parseDouble
                                                          (* 1000))]
                        (s/validate sync-request-schema sync-request)
                        (let [server-url (remote-url->server-url remote-url)
                              remote-server (make-remote-server
                                              {:server-url remote-url}
                                              jetty-config)]
                          (cond
                            (not (validate-sync-fn remote-server))
                            {:status 503
                             :body (format "Refusing to sync. PuppetDB is not configured to sync with %s" remote-url)}
                            completion-timeout-ms
                            (do
                              (maplog [:sync :info] {:remote (:url remote-server)}
                                      "Performing blocking sync with timeout of %s ms" completion-timeout-ms)
                              (async/alt!!
                                (async/go (blocking-sync
                                            remote-server query-fn
                                            bucketed-summary-query-fn
                                            enqueue-command-fn node-ttl response-mult))
                                (http/json-response {:timed_out false} 200)

                                (async/timeout completion-timeout-ms)
                                (http/json-response {:timed_out true} 503)))

                            :else
                            (sync-with! remote-server query-fn
                                        bucketed-summary-query-fn
                                        enqueue-command-fn node-ttl)))))))))

(defn start-bucketed-summary-cache-invalidation-job [response-mult cache-atom]
  (let [processed-commands-chan (async/chan)]
    (async/tap response-mult processed-commands-chan)
    (async/go-loop []
      (when-let [processed-command (async/<!! processed-commands-chan)]
        (bucketed-summary/invalidate-cache-for-command cache-atom processed-command)
        (recur)))
    #(do (async/untap response-mult processed-commands-chan)
         (async/close! processed-commands-chan))))

(defn attempt-initial-sync
  "Attempt an initial sync against a remote-server. On success, return :success,
   on exception other than message-processing-timeout, log warning and return
   nil."
  [remote-server query-fn shared-globals cache-atom response-mult node-ttl
   enqueue-command-fn]
  (let [server-url (remote-url->server-url (:url remote-server))]
    (try+
      (maplog [:sync :info] {:remote (str server-url)}
              "Performing initial blocking sync from {remote}...")
      (blocking-sync remote-server query-fn
                     (partial bucketed-summary/bucketed-summary-query
                              (:scf-read-db (shared-globals))
                              cache-atom)
                     enqueue-command-fn node-ttl response-mult)
      :success
      (catch [:type ::message-processing-timeout] _
        ;; Something is very wrong if we hit this timeout; rethrow the
        ;; exception to crash the server.
        (throw+))
      ;; any other exception is basically ok; just log a warning and
      ;; keep on going.
      (catch [] ex
        (log/warn ex (format "Could not perform initial sync with %s" server-url)))
      (catch Exception ex
        (log/warn ex (format "Could not perform initial sync with %s" server-url))))))

(defservice puppetdb-sync-service
  PuppetDBSync
  [[:DefaultedConfig get-config]
   [:PuppetDBServer query shared-globals]
   [:PuppetDBCommandDispatcher enqueue-command response-mult]]

  (init [this context]
        (jmx-reporter/start (:reporter events/sync-metrics))
        context)

  (start [this context]
         (let [{{node-ttl :node-ttl} :database
                sync-config :sync
                jetty-config :jetty} (get-config)
               remotes-config (:remotes sync-config)
               response-mult (response-mult)
               cache-atom (atom {})
               context (assoc context
                              :bucketed-summary-cache-atom
                              cache-atom
                              :cache-invalidation-job-stop-fn
                              (start-bucketed-summary-cache-invalidation-job response-mult
                                                                         cache-atom))]
           (jdbc/with-db-connection (:scf-read-db (shared-globals))
             (when-not (sutils/pg-extension? "pgcrypto")
               (throw+-cli-error!
                (str "Missing PostgreSQL extension `pgcrypto`\n\n"
                     "Puppet Enterprise requires the pgcrypto extension to be installed. \n"
                     "Run the command:\n\n"
                     "    CREATE EXTENSION pgcrypto;\n\n"
                     "as the database super user on the PuppetDB database install it,\n"
                     "then restart PuppetDB.\n"))))

           (if (enable-periodic-sync? remotes-config)
             (let [remote-servers (map #(make-remote-server % jetty-config) remotes-config)
                   _ (some #(attempt-initial-sync % query shared-globals
                                                  cache-atom response-mult
                                                  node-ttl enqueue-command)
                           remote-servers)]
               (let [pool (atat/mk-pool)
                     nremotes (count remote-servers)]
                 (assoc context
                        :scheduled-sync
                        (doall
                          (map (fn [interval remote-server]
                                 (let [interval-millis (to-millis interval)]
                                   {:job (atat/interspaced
                                           interval-millis
                                           #(sync-with!
                                              remote-server query
                                              (partial bucketed-summary-query this)
                                              enqueue-command node-ttl)
                                           pool :initial-delay (rand-int interval-millis))
                                    :remote (remote-url->server-url (:url remote-server))}))
                               (map :interval remotes-config)
                               remote-servers)))))
             context)))

  (stop [this context]
        (jmx-reporter/stop (:reporter events/sync-metrics))
        (when-let [scheduled-syncs (:scheduled-sync context)]
          (doseq [{:keys [job remote]} scheduled-syncs]
            (log/infof "Stopping pe-puppetdb sync with %s" remote)
            (atat/stop job)
            (log/infof "Stopped pe-puppetdb sync with %s" remote)))
        (when-let [stop-fn (:cache-invalidation-job-stop-fn context)]
          (stop-fn))
        (dissoc context
                :scheduled-sync
                :cache-invalidation-job-stop-fn))

  (bucketed-summary-query [this entity]
    (bucketed-summary/bucketed-summary-query
      (:scf-read-db (shared-globals))
      (:bucketed-summary-cache-atom (service-context this))
      entity)))
