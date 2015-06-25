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
      resource_title FROM (SELECT reports.certname AS certname,
        reports.configuration_version AS configuration_version,
        containing_class AS containing_class,
        containment_path AS containment_path,
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
        timestamp AS timestamp
        FROM resource_events events
        INNER JOIN reports ON events.report_id = reports.id
        LEFT JOIN environments ON reports.environment_id = environments.id
        WHERE (timestamp > '1969-12-31 16:00:00.0')) distinct_events) events
    GROUP BY resource_type, resource_title) event_counts);
