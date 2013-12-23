CREATE TABLE classes (name TEXT PRIMARY KEY);
--;;
CREATE TABLE class_parameters (
    parameter TEXT NOT NULL,
    default_value TEXT,
    class_name TEXT REFERENCES classes(name) ON DELETE CASCADE,
    PRIMARY KEY(class_name, parameter)
);
