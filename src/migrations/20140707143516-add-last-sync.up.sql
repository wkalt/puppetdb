CREATE TABLE last_sync (
    time TIMESTAMP WITH TIME ZONE NOT NULL
)
--;;
CREATE UNIQUE INDEX last_sync_one_row ON last_sync((TRUE));
