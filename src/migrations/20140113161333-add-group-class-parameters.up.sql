CREATE UNIQUE INDEX ON group_classes (group_name, class_name, environment_name);
--;;
CREATE UNIQUE INDEX ON class_parameters (class_name, environment_name, parameter);
--;;
CREATE TABLE group_class_parameters (
    parameter TEXT NOT NULL,
    class_name TEXT NOT NULL,
    environment_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (group_name, class_name, environment_name, parameter),
    FOREIGN KEY (group_name, class_name, environment_name) REFERENCES group_classes (group_name, class_name, environment_name),
    FOREIGN KEY (class_name, environment_name, parameter) REFERENCES class_parameters (class_name, environment_name, parameter)
);
