(ns puppetlabs.puppetdb.admin
  (:require [puppetlabs.puppetdb.export :as export]
            [puppetlabs.puppetdb.import :as import]
            [puppetlabs.puppetdb.http :as http]
            [ring.middleware.multipart-params :as mp]
            [puppetlabs.puppetdb.query.summary-stats :as ss]
            [clj-time.core :refer [now]]
            [ring.util.io :as rio]
            [puppetlabs.comidi :as cmdi]
            [puppetlabs.puppetdb.middleware :as mid]
            [bidi.schema :as bidi-schema]
            [puppetlabs.puppetdb.schema :as pls]
            [schema.core :as s]))

(pls/defn-validated admin-routes :- bidi-schema/RoutePair
  [submit-command-fn :- (s/pred fn?)
   query-fn :- (s/pred fn?)
   get-shared-globals :- (s/pred fn?)]
  (cmdi/context "/v1"
                (cmdi/context "/archive"
                              (cmdi/wrap-routes
                                (cmdi/POST "" request
                                           (import/import! (get-in request [:multipart-params "archive" :tempfile])
                                                           submit-command-fn)
                                           (http/json-response {:ok true}))
                                mp/wrap-multipart-params)
                              (cmdi/GET "" [anonymization_profile]
                                        (http/streamed-tar-response #(export/export! % query-fn anonymization_profile)
                                                                    (format "puppetdb-export-%s.tgz" (now)))))

                (cmdi/ANY "/summary-stats" []
                          (ss/collect-metadata get-shared-globals))))

(defn build-app
  [submit-command-fn query-fn get-shared-globals]
  (mid/make-pdb-handler (admin-routes submit-command-fn query-fn get-shared-globals)))
