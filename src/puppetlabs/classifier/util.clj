(ns puppetlabs.classifier.util
  (:require [clojure.string :as str]
            [clojure.walk :refer [postwalk]]
            [schema.utils :refer [validation-error-explain]]
            puppetlabs.trapperkeeper.core))

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

(defn ->client-explanation
  "Transforms a schema explanation into one that makes more sense to javascript
  client by removing any java.lang. prefixes and changing any Keyword symbols to
  String."
  [explanation]
  (let [strip-sym-prefix (fn [x]
                           (let [xstr (str x)]
                             (cond
                               (not (symbol? x)) x
                               (not (re-find #"^java\.lang\." xstr)) x
                               :else
                                 (symbol (str/replace xstr "java.lang." "")))))
        keyword->string (fn [x]
                          (if (= x 'Keyword)
                            'String
                            x))
        error-explainer (fn [x]
                          (if (= (class x) schema.utils.ValidationError)
                            (validation-error-explain x)
                            x))]
    (postwalk (comp error-explainer keyword->string strip-sym-prefix)
              explanation)))
