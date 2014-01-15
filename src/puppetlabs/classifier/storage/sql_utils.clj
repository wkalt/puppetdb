(ns puppetlabs.classifier.storage.sql-utils)

(defn aggregate-into
  "Aggregates a sequence of maps, grouping by the complement of group-keys and
  merging the result into a map of the remaining keys under into-key"
  [into-key group-keys results]
  {:pre [(= 2 (count group-keys))]}
  (for [group (group-by #(apply dissoc % group-keys) results)]
    (let [[grouped all] group]
      (assoc grouped into-key (->> all
                                   (map (apply juxt group-keys))
                                   (remove (comp nil? first))
                                   (into {}))))))

