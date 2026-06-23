-- BigQuery SQL Stored Procedure for determining file partitioning logic
-- Replaces: 'getsubintervalls' function in vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This Stored Procedure will calculate file partitions based on input parameters (e.g., date ranges, partition interval).
-- It can output a table of partition details to be consumed by Airflow.
-- This example provides a basic structure, and the actual logic will depend on specific partitioning rules.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset.get_file_partitions_bq`(
    IN p_start_date DATE,
    IN p_end_date DATE,
    IN p_partition_unit STRING, -- e.g., 'DAY', 'MONTH', 'YEAR'
    OUT p_partition_details ARRAY<STRUCT<partition_start_date DATE, partition_end_date DATE, partition_label STRING>>
)
BEGIN
    DECLARE current_date DATE DEFAULT p_start_date;
    DECLARE partitions ARRAY<STRUCT<partition_start_date DATE, partition_end_date DATE, partition_label STRING>>;

    SET partitions = ARRAY<STRUCT<partition_start_date DATE, partition_end_date DATE, partition_label STRING>>[];

    WHILE current_date <= p_end_date DO
        CASE p_partition_unit
            WHEN 'DAY' THEN
                SET partitions = ARRAY_CONCAT(partitions, [STRUCT(current_date AS partition_start_date, current_date AS partition_end_date, FORMAT_DATE('%Y%m%d', current_date) AS partition_label)]);
                SET current_date = DATE_ADD(current_date, INTERVAL 1 DAY);
            WHEN 'MONTH' THEN
                SET partitions = ARRAY_CONCAT(partitions, [STRUCT(DATE_TRUNC(current_date, MONTH) AS partition_start_date, LAST_DAY(current_date) AS partition_end_date, FORMAT_DATE('%Y%m', current_date) AS partition_label)]);
                SET current_date = DATE_ADD(current_date, INTERVAL 1 MONTH);
            WHEN 'YEAR' THEN
                SET partitions = ARRAY_CONCAT(partitions, [STRUCT(DATE_TRUNC(current_date, YEAR) AS partition_start_date, LAST_DAY(current_date, YEAR) AS partition_end_date, FORMAT_DATE('%Y', current_date) AS partition_label)]);
                SET current_date = DATE_ADD(current_date, INTERVAL 1 YEAR);
            ELSE
                -- Default to day if unit is not recognized
                SET partitions = ARRAY_CONCAT(partitions, [STRUCT(current_date AS partition_start_date, current_date AS partition_end_date, FORMAT_DATE('%Y%m%d', current_date) AS partition_label)]);
                SET current_date = DATE_ADD(current_date, INTERVAL 1 DAY);
        END CASE;
    END WHILE;

    SET p_partition_details = partitions;
END;

-- Example Usage:
-- DECLARE p_details ARRAY<STRUCT<partition_start_date DATE, partition_end_date DATE, partition_label STRING>>;
-- CALL `your_gcp_project_id.your_bigquery_dataset.get_file_partitions_bq`(DATE('2023-01-01'), DATE('2023-01-05'), 'DAY', p_details);
-- SELECT * FROM UNNEST(p_details);