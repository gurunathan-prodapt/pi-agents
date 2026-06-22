-- Migrated BigQuery Stored Procedure from d_ausd_v_ta_notice.sql
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.sp_d_ausd_v_ta_notice`(
    IN p_process_date DATE,
    OUT p_records_processed INT64
)
BEGIN
    -- This procedure encapsulates the core data transformation logic.
    -- It expects 'cds_ta_notice' to be available in the same dataset.

    -- Declare a variable to store the count of processed records
    DECLARE records_inserted INT64 DEFAULT 0;

    -- Truncate the target table 'ta_notice' before insertion
    TRUNCATE TABLE `your_project_id.your_dataset_id.ta_notice`;

    -- Insert data into the target table based on the logic from the original SQL script
    INSERT INTO `your_project_id.your_dataset_id.ta_notice`(
        cntrct_id,
        valid_from,
        valid_to,
        entry_date_of_notice,
        insert_at,
        modified_at,
        is_production
    )
    SELECT
        FORMAT("%d", n.cntrct_id), -- Assuming cntrct_id is numeric in source, converted to STRING
        n.valid_from,
        n.valid_to,
        n.entry_date_of_notice,
        n.insert_at,
        n.modified_at,
        n.is_production
    FROM
        `your_project_id.your_dataset_id.cds_ta_notice` AS n
    WHERE
        n.insert_at <= TIMESTAMP(p_process_date)
        AND (n.modified_at IS NULL OR n.modified_at > TIMESTAMP(p_process_date))
        AND (n.valid_to IS NULL OR n.valid_to > TIMESTAMP(p_process_date))
        AND n.is_production = 1;

    SET records_inserted = ROW_COUNT();

    -- Set the OUT parameter with the number of processed records
    SET p_records_processed = records_inserted;

    -- No explicit COMMIT needed in BigQuery as DML statements are atomic.
    -- Error handling can be done with EXCEPTION blocks in a calling procedure if needed.
END;