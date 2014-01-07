ALTER TABLE classes ADD COLUMN environment_name TEXT REFERENCES environments(name) NOT NULL
