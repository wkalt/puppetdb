(ns puppetlabs.puppetdb.command
  "PuppetDB command handling

   Commands are the mechanism by which changes are made to PuppetDB's
   model of a population. Commands are represented by `command
   objects`, which have the following JSON wire format:

       {\"command\": \"...\",
        \"version\": 123,
        \"payload\": <json object>}

   `payload` must be a valid JSON string of any sort. It's up to an
   individual handler function how to interpret that object.

   More details can be found in [the spec](../spec/commands.md).

   The command object may also contain an `annotations` attribute
   containing a map with arbitrary keys and values which may have
   command-specific meaning or may be used by the message processing
   framework itself.

   Commands should include a `received` annotation containing a
   timestamp of when the message was first seen by the system. If this
   is omitted, it will be added when the message is first parsed, but
   may then be somewhat inaccurate.

   Commands should include an `id` annotation containing a unique,
   string identifier for the command. If this is omitted, it will be
   added when the message is first parsed.

   Failed messages will have an `attempts` annotation containing an
   array of maps of the form:

       {:timestamp <timestamp>
        :error     \"some error message\"
        :trace     <stack trace from :exception>}

   Each entry corresponds to a single failed attempt at handling the
   message, containing the error message, stack trace, and timestamp
   for each failure. PuppetDB may discard messages which have been
   attempted and failed too many times, or which have experienced
   fatal errors (including unparseable messages).

   Failed messages will be stored in files in the \"dead letter
   office\", located under the MQ data directory, in
   `/discarded/<command>`. These files contain the annotated message,
   along with each exception that occured while trying to handle the
   message.

   We currently support the following wire formats for commands:

   1. Java Strings

   2. UTF-8 encoded byte-array

   In either case, the command itself, once string-ified, must be a
   JSON-formatted string with the aforementioned structure."
  (:require [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.scf.storage :as scf-storage]
            [puppetlabs.puppetdb.catalogs :as cat]
            [puppetlabs.puppetdb.reports :as report]
            [puppetlabs.puppetdb.facts :as facts]
            [puppetlabs.puppetdb.command.dlo :as dlo]
            [puppetlabs.puppetdb.mq :as mq]
            [puppetlabs.kitchensink.core :as kitchensink]
            [clj-http.client :as client]
            [clj-time.coerce :refer [to-timestamp]]
            [puppetlabs.puppetdb.config :as conf]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [puppetlabs.puppetdb.random :refer [random-string]]
            [clojure.walk :as walk]
            [puppetlabs.puppetdb.utils :as utils]
            [slingshot.slingshot :refer [try+ throw+]]
            [cheshire.custom :refer [JSONable]]
            [puppetlabs.puppetdb.command.constants :refer [command-names]]
            [puppetlabs.trapperkeeper.services :refer [defservice]]
            [schema.core :as s]))


(def doc
  {:timestamp 0
   :catalog-hashes (take 20 (repeatedly random-string))
   :report-hashes (take 20 (repeatedly random-string))
   :factset-hashes (take 20 (repeatedly random-string))})

(def catalog-endpoint "/v4/catalogs")
(def report-endpoint "/v4/reports")
(def factsets-endpoint "/v4/factsetss")
(def sync-endpoint "/ha/v4/sync")
(def remotehost "mbp.local")

(defn get-hashes
  []

  
  )

(defn puppetdb-sync*
  [{:keys [timestamp catalog-hashes report-hashes factset-hashes]}]
  (doseq [h catalog-hashes]
    (let [{:keys [body]} (query-db a catalogs-endpoint ["=" "hash" h])
          query-result (json/parse-stream body)]
      ))
  )
