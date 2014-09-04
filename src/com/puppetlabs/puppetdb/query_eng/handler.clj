(ns com.puppetlabs.puppetdb.query-eng.handler
  (:require [com.puppetlabs.http :as pl-http]
            [com.puppetlabs.puppetdb.query.facts :as facts]
            [com.puppetlabs.puppetdb.query.event-counts :as event-counts]
            [com.puppetlabs.puppetdb.query.fact-contents :as fact-contents]
            [com.puppetlabs.puppetdb.query.events :as events]
            [com.puppetlabs.puppetdb.query.aggregate-event-counts :as aggregate-event-counts]
            [com.puppetlabs.puppetdb.query.nodes :as nodes]
            [com.puppetlabs.puppetdb.query.environments :as environments]
            [com.puppetlabs.puppetdb.query.reports :as reports]
            [com.puppetlabs.puppetdb.query.factsets :as factsets]
            [com.puppetlabs.puppetdb.query.resources :as resources]
            [puppetlabs.kitchensink.core :as kitchensink]
            [com.puppetlabs.cheshire :as json]
            [com.puppetlabs.puppetdb.query :as query]
            [net.cgrand.moustache :refer [app]]
            [com.puppetlabs.jdbc :as jdbc]
            [com.puppetlabs.puppetdb.query-eng.engine :as qe]
            [com.puppetlabs.puppetdb.http :as http]))

(defn produce-streaming-body
  "Given a query, and database connection, return a Ring response with the query
  results.
  If the query can't be parsed, a 400 is returned."
  [entity version query {:keys [paging-options query-options summarize-by] :as options} db]
  (let [munge-fn (:munge-fn (qe/entity-attributes entity version options))]
    (try
      (jdbc/with-transacted-connection db
        (let [parsed-query (json/parse-strict-string query true)
              {[sql & params] :results-query
               count-query :count-query} (qe/query->sql entity version parsed-query options)
              resp (pl-http/stream-json-response
                     (fn [f]
                       (jdbc/with-transacted-connection db
                         (query/streamed-query-result version sql params
                                                      (comp f munge-fn)))))]
          (if count-query
            (http/add-headers resp {:count (jdbc/get-result-count count-query)})
            resp)))
      (catch com.fasterxml.jackson.core.JsonParseException e
        (pl-http/error-response e))
      (catch IllegalArgumentException e
        (pl-http/error-response e)))))
