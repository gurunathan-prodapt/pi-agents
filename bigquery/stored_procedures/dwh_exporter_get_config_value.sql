-- BigQuery Stored Procedure to retrieve a configuration value.
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE OR REPLACE PROCEDURE dwh_exporter.get_config_value(
    IN p_job_name STRING,
    IN p_config_key STRING,
    OUT p_config_value STRING
)
BEGIN
    SELECT config_value
    INTO p_config_value
    FROM dwh_exporter.config_kv
    WHERE job_name = p_job_name AND config_key = p_config_key
    LIMIT 1;
END;