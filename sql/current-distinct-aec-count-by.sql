WITH latest_events AS
(SELECT certnames.certname,
  resource_events.resource_type,
  resource_events.resource_title,
  resource_events.property,
  MAX(resource_events.timestamp) AS timestamp
  FROM resource_events
  JOIN certnames ON resource_events.certname_id = certnames.id
  WHERE resource_events.timestamp >= '2014-12-31 16:00:00.0'
  AND resource_events.timestamp <= '2015-07-31 17:00:00.0'
  GROUP BY certname, resource_type, resource_title, property)
(SELECT 'resource' as summarize_by,
  SUM(CASE WHEN successes > 0 THEN 1 ELSE 0 END) as successes,
  SUM(CASE WHEN failures > 0  THEN 1 ELSE 0 END) as failures,
  SUM(CASE WHEN noops > 0 THEN 1 ELSE 0 END) as noops,
  SUM(CASE WHEN skips > 0 THEN 1 ELSE 0 END) as skips,
  COUNT(*) as total
  FROM (SELECT resource_type,
    resource_title,
    SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failures,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes,
    SUM(CASE WHEN status = 'noop' THEN 1 ELSE 0 END) AS noops,
    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skips
    FROM (SELECT DISTINCT certname,
      status,
      resource_type,
      resource_title FROM
      (SELECT resource_events.message,
        resource_events.old_value,
        reports.receive_time AS report_receive_time,
        resource_events.containment_path,
        reports.certname,
        trim(leading '\x' from reports.hash::text) AS report,
        resource_events.timestamp,
        environments.name AS environment,
        reports.configuration_version,
        resource_events.new_value,
        resource_events.resource_title,
        resource_events.status,
        resource_events.property,
        resource_events.resource_type,
        resource_events.line,
        reports.end_time AS run_end_time,
        resource_events.containing_class,
        resource_events.file,
        reports.start_time AS run_start_time
        FROM resource_events
        JOIN reports ON resource_events.report_id = reports.id
        LEFT OUTER JOIN environments ON reports.environment_id = environments.id
        JOIN latest_events
        ON reports.certname = latest_events.certname
        AND resource_events.resource_type = latest_events.resource_type
        AND resource_events.resource_title = latest_events.resource_title
        AND ((resource_events.property = latest_events.property) OR
          (resource_events.property IS NULL AND latest_events.property IS NULL))
        AND resource_events.timestamp = latest_events.timestamp
        WHERE resource_events.timestamp > '1969-12-31 16:00:00.0') distinct_events) events
    GROUP BY resource_type, resource_title) event_counts)
