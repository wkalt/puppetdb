CREATE TABLE nodes (
    name TEXT PRIMARY KEY
);
--;;

CREATE TABLE environments (
    name TEXT PRIMARY KEY
);
--;;

CREATE TABLE groups (
    id UUID PRIMARY KEY,
    name TEXT,
    environment_name TEXT NOT NULL REFERENCES environments (name) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
    parent_id UUID NOT NULL REFERENCES groups (id) ON UPDATE CASCADE ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
);
--;;
CREATE UNIQUE INDEX ON groups (id, environment_name);
--;;
CREATE UNIQUE INDEX ON groups (name, environment_name);
--;;

CREATE TABLE group_variables (
    variable TEXT NOT NULL,
    group_id UUID NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (variable, group_id),
    FOREIGN KEY (group_id) REFERENCES groups (id) ON DELETE CASCADE ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
);
--;;

CREATE TABLE classes (
    name TEXT NOT NULL,
    environment_name TEXT NOT NULL REFERENCES environments (name) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
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
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (class_name, parameter, environment_name),
    FOREIGN KEY (class_name, environment_name) REFERENCES classes (name, environment_name) ON DELETE CASCADE ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
);
--;;
CREATE UNIQUE INDEX ON class_parameters (class_name, environment_name, parameter);
--;;

CREATE TABLE group_classes (
    group_id UUID NOT NULL REFERENCES groups (id) ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    class_name TEXT NOT NULL,
    environment_name TEXT NOT NULL,
    PRIMARY KEY (group_id, class_name),
    FOREIGN KEY (class_name, environment_name) REFERENCES classes (name, environment_name) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (group_id, environment_name) REFERENCES groups (id, environment_name) ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
--;;

CREATE TABLE group_class_parameters (
    parameter TEXT NOT NULL,
    class_name TEXT NOT NULL,
    environment_name TEXT NOT NULL,
    group_id UUID NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (group_id, class_name, parameter),
    FOREIGN KEY (group_id, class_name) REFERENCES group_classes (group_id, class_name) ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (group_id, environment_name) REFERENCES groups (id, environment_name) ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (class_name, environment_name, parameter) REFERENCES class_parameters (class_name, environment_name, parameter) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
);
--;;

CREATE TABLE rules (
    id BIGSERIAL PRIMARY KEY,
    match TEXT NOT NULL,
    group_id UUID REFERENCES groups (id) on DELETE CASCADE ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
);
--;;

INSERT INTO environments (name) VALUES ('production')
--;;
INSERT INTO groups (name, id, environment_name, parent_id) VALUES ('default', '00000000-0000-4000-8000-000000000000', 'production', '00000000-0000-4000-8000-000000000000')
--;;
INSERT INTO rules (group_id, match) VALUES ('00000000-0000-4000-8000-000000000000', '["and",["=","notarealfact","notarealvalue"]]')
