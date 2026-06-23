-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
--
-- BigQuery SQL script to perform the final INSERT into SOF_TA_DISC_ZUSGF.
-- This script leverages the output from the PySpark job for concatenated discounts.

INSERT INTO `{{ params.project_id }}.{{ params.bigquery_dataset_target }}.SOF_TA_DISC_ZUSGF`
(
    cntrct_id,
    cntrct_obj_version,
    disc_vector_ty,
    rabatt_alle
)
SELECT
    source_disc.cntrct_id,
    source_disc.cntrct_obj_version,
    source_disc.disc_vector_ty,
    spark_output.rabatt_alle
FROM
    (
        SELECT DISTINCT
            cntrct_id,
            cntrct_obj_version,
            disc_vector_ty
        FROM
            `{{ params.project_id }}.{{ params.bigquery_dataset_source }}.SOF_TA_DISCOUNT`
    ) AS source_disc
LEFT JOIN
    `{{ params.project_id }}.{{ params.bigquery_dataset_staging }}.ta_disc_zusgf_spark_staging` AS spark_output
ON
    source_disc.cntrct_id = spark_output.cntrct_id
    AND source_disc.cntrct_obj_version = spark_output.cntrct_obj_version;

-- Note on parameters: Airflow will render `{{ params.project_id }}` etc.
-- Ensure these parameters are passed correctly by the BigQueryOperator in the DAG.