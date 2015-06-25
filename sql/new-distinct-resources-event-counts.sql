SELECT count(1),resource_type,resource_title,status FROM
(SELECT DISTINCT on (certname, resource_type, resource_title, property) *
  FROM
(SELECT reports.certname AS certname,
reports.configuration_version AS configuration_version,
containing_class AS containing_class, containment_path AS containment_path,
environments.name AS environment,
file AS file,
line AS line,
message AS message,
new_value AS new_value,
old_value AS old_value,
property AS property,
trim(leading '\x' from reports.hash::text) AS report,
reports.receive_time AS report_receive_time,
resource_title AS resource_title,
resource_type AS resource_type,
reports.end_time AS run_end_time,
reports.start_time AS run_start_time,
status AS status,
timestamp AS timestamp FROM resource_events events
INNER JOIN reports ON events.report_id = reports.id
LEFT JOIN environments ON reports.environment_id = environments.id
WHERE (reports.certname = 'host-1') order by timestamp) foo) bar
GROUP BY resource_type,resource_title,status
