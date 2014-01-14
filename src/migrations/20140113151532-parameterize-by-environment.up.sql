ALTER TABLE groups ADD COLUMN environment_name TEXT REFERENCES environments(name);
--;;
ALTER TABLE group_classes DROP constraint group_classes_class_name_fkey;
--;;
ALTER TABLE group_classes DROP constraint group_classes_group_name_fkey ;
--;;
ALTER TABLE group_classes ADD COLUMN environment_name TEXT NOT NULL;
--;;
CREATE UNIQUE INDEX ON groups (name, environment_name);
--;;
ALTER TABLE group_classes ADD CONSTRAINT group_classes_group_fkey FOREIGN KEY (group_name, environment_name) REFERENCES groups (name, environment_name) ON DELETE CASCADE
--;;
CREATE UNIQUE INDEX ON classes (name, environment_name);
--;;
ALTER TABLE group_classes ADD CONSTRAINT group_classes_class_fkey FOREIGN KEY (class_name, environment_name) REFERENCES classes (name, environment_name) ON DELETE CASCADE;
--;;
ALTER TABLE class_parameters ADD COLUMN environment_name TEXT NOT NULL;
--;;
ALTER TABLE class_parameters ADD CONSTRAINT class_parameters_class_fkey FOREIGN KEY (class_name, environment_name) REFERENCES classes (name, environment_name) ON DELETE CASCADE;
