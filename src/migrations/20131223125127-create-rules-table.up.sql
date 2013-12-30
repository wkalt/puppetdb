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
