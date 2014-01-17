(ns puppetlabs.classifier.storage.sql-utils)

(defn aggregate-submap-by
  "Given a sequence of maps in results where each map contains agg-key and
  agg-val as keys, groups the maps that are identical except for the values in
  agg-key or agg-val. The values of agg-key and agg-val are turned into a map
  and stored in the resulting map under under-key."
  [agg-key agg-val under-key results]
  (for [[grouped all] (group-by #(dissoc % agg-key agg-val) results)]
    (assoc grouped under-key (->> all
                               (map (juxt agg-key agg-val))
                               (remove (comp nil? first))
                               (into {})))))

