(ns puppetlabs.puppetdb.status-test
  (:require [clojure.test :refer :all]
            [puppetlabs.puppetdb.testutils :as tu]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.testutils.services :as svc-utils]))

(defn status-base-url->str
  [{:keys [protocol host port prefix] :as base-url}]
  (-> (java.net.URL. protocol host port prefix)
      .toURI
      .toASCIIString))

(deftest status-test
  (svc-utils/call-with-puppetdb-instance
    (fn []
      (let [pdb-resp (client/get (status-base-url->str
                                   (assoc svc-utils/*base-url* :prefix "/status")))]
        (tu/assert-success! pdb-resp)
        (println "PDB RESP " pdb-resp)))))
