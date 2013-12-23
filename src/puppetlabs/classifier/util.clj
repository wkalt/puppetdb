(ns puppetlabs.classifier.util
  (:require puppetlabs.trapperkeeper.core))

(defn ini->map
  "Given a path to an ini file, parses the file into a map, returning the map."
  [ini-file-path]
  (#'puppetlabs.trapperkeeper.core/parse-config-file ini-file-path))

(defn map->ini
  "Returns the .ini format representation of the map `m` as a string"
  [m]
  (apply str (for [[k v] m]
               (if (associative? v)
                 (str "[" (name k) "]\r\n"
                      (map->ini v))
                 (str (name k) " = " v "\r\n")))))

(defn write-map-as-ini!
  "Serialize the map `m` to a .ini file at the given path."
  [m output-path]
  (spit output-path (map->ini m)))
