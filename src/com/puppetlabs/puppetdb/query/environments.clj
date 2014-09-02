(ns com.puppetlabs.puppetdb.query.environments
  (:require [com.puppetlabs.jdbc :as jdbc]
            [com.puppetlabs.puppetdb.query :as query]
            [com.puppetlabs.puppetdb.query.paging :as paging]))

(defn query-environments
  "Search for environments satisfying the given SQL filter."
  [version query-sql]
  {:pre  [(map? query-sql)
          (jdbc/valid-jdbc-query? (:results-query query-sql))]
   :post [(map? %)
          (sequential? (:result %))]}
  (let [{[sql & params] :results-query
         count-query    :count-query} query-sql
         result {:result (query/streamed-query-result
                          version sql params doall)}]
    (if count-query
      (assoc result :count (jdbc/get-result-count count-query))
      result)))
