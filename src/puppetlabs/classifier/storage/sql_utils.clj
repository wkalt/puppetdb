(ns puppetlabs.classifier.storage.sql-utils)

(defn ordered-group-by
  [f coll]
  (let [grouped-w-index (loop [i 0, groups (transient {}), coll (seq coll)]
                          (if-let [x (first coll)]
                            (let [k (f x)
                                  group (get groups k [i])
                                  groups' (assoc! groups k (conj group x))]
                              (recur (inc i) groups' (next coll)))
                            ;; else (nothing left in coll)
                            (persistent! groups)))]
    (->> (seq grouped-w-index)
      ; sort the groups by the index where the first member appeared
      (sort-by #(get-in % [1 0])))))

(defn aggregate-submap-by
  "Given a sequence of maps in results where each map contains agg-key and
  agg-val as keys, groups the maps that are identical except for the values in
  agg-key or agg-val. The values of agg-key and agg-val are turned into a map
  and stored in the resulting map under under-key."
  [agg-key agg-val under-key results]
  (for [[combined [_ & all]] (ordered-group-by #(dissoc % agg-key agg-val) results)]
    (assoc combined under-key (->> all
                               (map (juxt agg-key agg-val))
                               (remove (comp nil? first))
                               (into {})))))

(defn aggregate-column
  "Given a sequence of rows as maps, aggregate the values of `column` into
  a sequence under `under`, combining rows that are equal except for the value
  of `column`. Useful for consolidating the results of an outer join."
  [column under results]
  (for [[combined [_ & all]] (ordered-group-by #(dissoc % column) results)]
    (assoc combined under (map #(get % column) all))))
