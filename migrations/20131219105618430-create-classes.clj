;; migrations/20131219105618430-create-classes.clj

(defn up []
  ["CREATE TABLE classes (name TEXT PRIMARY KEY)"
   "CREATE TABLE class_parameters (parameter TEXT, default_value TEXT, class_name TEXT REFERENCES classes(name) ON DELETE CASCADE, PRIMARY KEY(class_name, parameter))"])

(defn down []
  ["DROP TABLE class_parameters"
   "DROP TABLE classes"])
