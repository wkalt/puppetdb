(ns puppetlabs.puppetdb.command.dlo
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [metrics.counters :as counters]
   [puppetlabs.i18n.core :refer [trs]]
   [puppetlabs.kitchensink.core :refer [timestamp]]
   [puppetlabs.puppetdb.nio :refer [copts copt-atomic copt-replace oopts]]
   [puppetlabs.puppetdb.queue :as queue
    :refer [cmdref->entry metadata-str metadata-parser]]
   [stockpile :as stock])
  (:import
   [java.nio.file AtomicMoveNotSupportedException
    FileAlreadyExistsException Files LinkOption]
   [java.nio.file.attribute FileAttribute]))

(def command-names (cons "unknown" queue/metadata-command-names))

(def ^:private parse-cmd-filename
  (let [parse-metadata (metadata-parser command-names)]
    (fn [s]
      (let [rx #"([0-9]+)-(.*)"]
        (when-let [[_ id qmeta] (re-matches rx s)]
          (parse-metadata qmeta))))))

(defn- cmd-counters
  "Adds gauges to the dlo for the given category (e.g. \"all\"
  \"replace command\").  The gauges will pull their values from the
  dlo atom's :stats."
  [registry category]
  (let [dlo-ns "puppetlabs.puppetdb.dlo"]
    {:count (counters/counter registry [dlo-ns category "messages"])
     :size (counters/counter registry [dlo-ns category "filesize"])}))

(defn- update-metrics
  "Updates stats to reflect the receipt of the named command."
  [metrics command size]
  (let [all (metrics "global")
        cmd (metrics command)]
    (counters/inc! (:count all))
    (counters/inc! (:size all) size)
    (counters/inc! (:count cmd))
    (counters/inc! (:size cmd) size)))

(defn- update-metrics-from-path
  "Updates metrics to reflect the discarded commands directly inside
  the path; does not recurse."
  [metrics path]
  (with-open [path-stream (Files/newDirectoryStream path)]
    (doseq [path path-stream]
      ;; Assume the trailing .json here and in entry-cmd-err-filename below.
      (when-let [cmd (:command
                      (and (.endsWith path ".json")
                           (parse-cmd-filename (-> path .getFileName str))))]
        (update-metrics metrics cmd (Files/size path))))))

(defn- write-failure-metadata
  "Given a (possibly empty) sequence of command attempts and an exception,
  writes a summary of the failure to out."
  [out attempts exception]
  (let [write-header (fn [out i timestamp]
                       (.format out "%sAttempt %d @ %s\n"
                                (into-array Object [(if (zero? i) "" "\n")
                                                    (inc i)
                                                    timestamp])))]
    (dorun (map-indexed (fn [i {:keys [timestamp error trace]}]
                          (write-header out i timestamp)
                          (.print out trace))
                        attempts))
    (write-header out (count attempts) (timestamp))
    (.printStackTrace exception out)))

(defn- entry-cmd-data-filename
  [entry]
  ;; 10291-1469469689-cat-4-foo.org.json
  (let [id (stock/entry-id entry)
        meta (stock/entry-meta entry)]
    (str id \- meta)))

(defn- err-filename
  [id metadata]
  ;; 10291-1469469689-cat-4-foo.org-err.txt
  (assert (str/ends-with? metadata ".json"))
  (str id \- (subs metadata 0 (- (count metadata) 5)) "-err.txt"))

(defn- store-failed-command-info
  [id metadata command exception attempts dir]
  ;; We want the metdata and command so we don't have to reparse, and
  ;; we want the command because id isn't unique by itself (given
  ;; unknown commands).
  (let [tmp (Files/createTempFile dir
                                  (str "tmp-err-" id \- command) ""
                                  (make-array FileAttribute 0))]
    ;; Leave the temp file if something goes wrong.
    (with-open [out (java.io.PrintWriter. (.toFile tmp))]
      (write-failure-metadata out attempts exception))
    (let [dest (.resolve dir (err-filename id metadata))
          moved? (try
                   (Files/move tmp dest (copts [copt-atomic]))
                   (catch FileAlreadyExistsException ex
                     true)
                   (catch UnsupportedOperationException ex
                     false)
                   (catch AtomicMoveNotSupportedException ex
                     false))]
      (when-not moved?
        (Files/move tmp dest (copts [copt-replace])))
      dest)))


;;; Public interface

(defn initialize
  "Initializes the dead letter office (DLO), at the given path (a Path),
   creating the directory if it doesn't exist, adds related metrics to
   the registry, and then returns a representation of the DLO."
  [path registry]
  (try
    (Files/createDirectory path (make-array FileAttribute 0))
    (catch FileAlreadyExistsException ex
      (when-not (Files/isDirectory path (make-array LinkOption 0))
        (throw (Exception. (trs "DLO path {0} is not a directory" path))))))
  (let [metrics (into {} (for [x (cons "global" command-names)]
                           [x (cmd-counters registry x)]))]
    (update-metrics-from-path metrics path)
    {:path path :metrics metrics}))

(defn discard-cmdref
  "Stores information about the failed `stockpile` `cmdref` to the
  `dlo` (a Path) for later inspection.  Saves two files named like
  this:
    10291-1469469689-cat-4-foo.org.json
    10291-1469469689-cat-4-foo.org-err.txt
  The first contains the failed command itself, and the second details
  the cause of the failure.  The command will be moved from the
  `stockpile` queue to the `dlo` directory (a Path) via
  stockpile/discard.  Returns {:info Path :command Path}."
  [cmdref exception stockpile dlo]
  (let [{:keys [path metrics]} dlo
        entry (cmdref->entry cmdref)
        cmd-dest (.resolve path (entry-cmd-data-filename entry))]
    ;; We're going to assume that our moves will be atomic, and if
    ;; they're not, that we don't care about the possibility of
    ;; partial dlo messages.  If needed, the existence of the err file
    ;; can be used as an indicator that the dlo message is complete.
    (stock/discard stockpile entry cmd-dest)
    (let [attempts (get-in cmdref [:annotations :attempts])
          id (stock/entry-id entry)
          metadata (stock/entry-meta entry)
          command (:command cmdref)
          info-dest (store-failed-command-info id metadata command
                                               exception attempts
                                               path)]
      (update-metrics metrics command (Files/size cmd-dest))
      {:info info-dest
       :command cmd-dest})))

(defn discard-bytes
  "Stores information about a failed command to the `dlo` (a Path) for
  later inspection.  `attempts` must be a list of exceptions in reverse
  chronological order.  Saves two files named like this:
    10291-1469469689-unknown-0-BYTESHASH
    10291-1469469689-unknown-0-BYTESHASH.txt
  The first contains the bytes provided, and the second details the
  cause of the failure.  Returns {:info Path :command Path}."
  [bytes id received attempts dlo]
  (assert (= 1 (count attempts)))  ;; Until store-failed-command-info overhaul
  ;; For now, we assume that we don't need durability, and that we
  ;; don't care about the possibility of partial dlo messages.  If
  ;; needed, the existence of the err file can be used as an
  ;; indicator that the unknown message may be complete.
  (let [{:keys [path metrics]} dlo
        digest (digest/sha1 [bytes])
        metadata (metadata-str received "unknown" 0 digest)
        cmd-dest (.resolve path (str id \- metadata))]
    (Files/write cmd-dest bytes (oopts []))
    (let [info-dest (store-failed-command-info id metadata "unknown"
                                               (first attempts)
                                               []
                                               path)]
      (update-metrics metrics "unknown" (Files/size cmd-dest))
      {:info info-dest :command cmd-dest})))
