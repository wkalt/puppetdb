(ns com.puppetlabs.puppetdb.http.nodes
  (:require [com.puppetlabs.puppetdb.query.paging :as paging]
            [com.puppetlabs.cheshire :as json]
            [com.puppetlabs.puppetdb.query :as query]
            [com.puppetlabs.puppetdb.query.nodes :as node]
            [com.puppetlabs.puppetdb.http.facts :as f]
            [com.puppetlabs.puppetdb.http.resources :as r]
            [com.puppetlabs.puppetdb.http.query :as http-q]
            [com.puppetlabs.puppetdb.query-eng.handler :as pb]
            [com.puppetlabs.http :as pl-http]
            [net.cgrand.moustache :refer [app]]
            [com.puppetlabs.middleware :refer [verify-accepts-json validate-query-params
                                               wrap-with-paging-options]]
            [com.puppetlabs.jdbc :as jdbc]
            [com.puppetlabs.puppetdb.http :as http]
            [com.puppetlabs.puppetdb.query-eng.engine :as qe]))

(defn current-status
  "Given a node's name, return the current status of the node.  Results
  include whether it's active and the timestamp of its most recent catalog, facts,
  and report."
  [version node]
  {:pre  [string? node]}
  (let [sql     (qe/query->sql version ["=" (case version (:v2 :v3) "name" "certname") node] {} :nodes)
        results (:result (node/query-nodes version sql))]
    (first results)))

(defn node-status
  "Produce a response body for a request to obtain status information
  for the given node."
  [version node db]
  (if-let [status (jdbc/with-transacted-connection db
                    (current-status version node))]
    (pl-http/json-response status)
    (pl-http/json-response {:error (str "No information is known about " node)} pl-http/status-not-found)))

(defn routes
  [version]
  (app
    []
    {:get (comp
            (fn [{:keys [params globals paging-options]}]
              (pb/produce-streaming-body
                :nodes
                version
                (params "query")
                paging-options
                (:scf-read-db globals)))
            http-q/restrict-query-to-active-nodes)}

    [node]
    {:get
     (-> (fn [{:keys [globals]}]
           (node-status version node (:scf-read-db globals)))
         ;; Being a singular item, querying and pagination don't really make
         ;; sense here
         (validate-query-params {}))}

    [node "facts" &]
    (comp (f/facts-app version) (partial http-q/restrict-query-to-node node))

    [node "resources" &]
    (comp (r/resources-app version) (partial http-q/restrict-query-to-node node))))

(defn node-app
  [version]
  (case version
    :v1 (throw (IllegalArgumentException. "api v1 is retired"))
    :v2 (-> (routes version)
            verify-accepts-json
            (validate-query-params {:optional ["query"]}))
    (-> (routes version)
        verify-accepts-json
        (validate-query-params
         {:optional (cons "query" paging/query-params)})
        wrap-with-paging-options)))
