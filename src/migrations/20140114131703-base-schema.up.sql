CREATE TABLE nodes (
    name TEXT PRIMARY KEY
);
--;;

CREATE TABLE environments (
    name TEXT PRIMARY KEY
);
--;;

CREATE TABLE groups (
    name TEXT PRIMARY KEY,
    environment_name TEXT NOT NULL REFERENCES environments(name)
);
--;;
CREATE UNIQUE INDEX ON groups (name, environment_name);
--;;

CREATE TABLE classes (
    name TEXT NOT NULL,
    environment_name TEXT NOT NULL REFERENCES environments(name),
    PRIMARY KEY (name, environment_name)
);
--;;
CREATE UNIQUE INDEX ON classes (name, environment_name);
--;;

CREATE TABLE class_parameters (
    parameter TEXT NOT NULL,
    default_value TEXT,
    environment_name TEXT NOT NULL,
    class_name TEXT NOT NULL,
    PRIMARY KEY(class_name, parameter, environment_name),
    FOREIGN KEY (class_name, environment_name) REFERENCES classes (name, environment_name) ON DELETE CASCADE
);
--;;
CREATE UNIQUE INDEX ON class_parameters (class_name, environment_name, parameter);
--;;

CREATE TABLE group_classes (
    group_name TEXT NOT NULL,
    class_name TEXT NOT NULL,
    environment_name TEXT NOT NULL,
    PRIMARY KEY (group_name, class_name),
    FOREIGN KEY (group_name, environment_name) REFERENCES groups (name, environment_name),
    FOREIGN KEY (class_name, environment_name) REFERENCES classes (name, environment_name)
);
--;;
CREATE UNIQUE INDEX ON group_classes (group_name, class_name, environment_name);
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
--;;

CREATE TABLE rules (
    id BIGSERIAL PRIMARY KEY,
    match TEXT NOT NULL
);
--;;

CREATE TABLE rule_groups (
    rule_id BIGINT REFERENCES rules(id) ON DELETE CASCADE,
    group_name TEXT REFERENCES groups(name) ON DELETE CASCADE,
    PRIMARY KEY (rule_id, group_name)
);
--;;

INSERT INTO environments (name) VALUES ('production')
