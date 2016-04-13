CREATE TABLE  bb 
CREATE TABLE full_reports (
  
  params JSONB NOT NULL,
  report_id SERIAL PRIMARY KEY REFERENCES reports(id)
);
GRANT ALL ON TABLE full_reports TO puppetdb;
