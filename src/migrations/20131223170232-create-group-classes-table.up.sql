CREATE TABLE group_classes (
    group_name TEXT REFERENCES groups(name) ON DELETE CASCADE,
    class_name TEXT REFERENCES classes(name) ON DELETE CASCADE
);
