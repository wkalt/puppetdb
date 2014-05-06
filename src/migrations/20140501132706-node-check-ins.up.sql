CREATE TABLE node_check_ins (
  node TEXT NOT NULL,
  time TIMESTAMP WITH TIME ZONE NOT NULL,
  explanation TEXT NOT NULL,
  PRIMARY KEY (node, time)
)
