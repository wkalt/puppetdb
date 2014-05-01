CREATE TABLE node_check_ins (
  node TEXT NOT NULL,
  time TIMESTAMP WITH TIME ZONE NOT NULL,
  matches TEXT NOT NULL,
  PRIMARY KEY (node, time)
)
