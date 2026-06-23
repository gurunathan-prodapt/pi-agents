-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure to construct a log filename string.
-- This procedure mimics the `DWMSG_Logdateiname` shell function.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_Logdateiname_SP`(
    OUT p_log_datei STRING,
    IN p_job_kennung STRING,
    IN p_dw_eintrags_nr INT64
)
BEGIN
    -- Construct a representative log file name. In BigQuery, this would typically
    -- refer to a logical log stream or a log table entry, not an actual file.
    SET p_log_datei = CONCAT('log_', p_job_kennung, '_', CAST(p_dw_eintrags_nr AS STRING), '_', FORMAT_DATE('%Y%m%d', CURRENT_DATE()), '.jsonl');
END;