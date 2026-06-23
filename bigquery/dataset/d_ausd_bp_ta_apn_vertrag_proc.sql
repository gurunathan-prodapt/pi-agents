-- Migrated from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

-- This stored procedure processes APN contracts and aggregates related information.
-- It replaces the Oracle SQL script d_ausd_bp_ta_apn_vertrag.sql.

CREATE OR REPLACE PROCEDURE `dataset.d_ausd_bp_ta_apn_vertrag_proc`(
    p_dataset_name STRING, -- The BigQuery dataset where tables like sof$ta_bpr_apn and sof$ta_apn_vertrag reside.
    p_isbert_schema_dataset STRING -- The BigQuery dataset where tables like dwtk_meldungen reside.
)
BEGIN
    -- Declare variables for dynamic SQL if needed, or to hold intermediate values.
    -- In this BigQuery migration, the original Oracle DEFINE and COLUMN commands are not directly applicable.
    -- v_datum equivalent can be directly derived or passed as a parameter if needed for partitioning/filtering.
    -- For now, we assume direct table access.

    -- Original: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    -- This step retrieved a date from a meldungen table. For BigQuery, we'll assume the process date
    -- is either passed in or determined by `CURRENT_DATE()`.
    -- If `isbert_schema.dwtk_meldungen` is needed, it would be referenced as `p_isbert_schema_dataset.dwtk_meldungen`.
    -- For this direct translation, we'll focus on the core PL/SQL logic.

    -- Step10: Erstellung einer lokalen apn-verträge mit allen apn-s und vertragsreferenzen pro vertrag
    -- Original: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_apn_vertrag');
    -- Replaced by direct TRUNCATE.

    EXECUTE IMMEDIATE FORMAT("""TRUNCATE TABLE `%s.sof$ta_apn_vertrag`;""", p_dataset_name);

    -- Original PL/SQL loop logic:
    -- for rec_cn in ( SELECT cntrct_id_ref, bpr_id, cntrct_id, access_point_name FROM sof$ta_bpr_apn ORDER BY cntrct_id) loop ... end loop;
    -- This aggregated `access_point_name` and `cntrct_id_ref` per `cntrct_id`.
    -- This can be efficiently done using STRING_AGG in BigQuery SQL.

    INSERT INTO `dataset.sof$ta_apn_vertrag` (contract_id, access_point_names, contract_refs)
    SELECT
        cntrct_id,
        SUBSTR(RTRIM(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), ', '), 1, 100),
        SUBSTR(RTRIM(STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref), ', '), 1, 100)
    FROM
        `dataset.sof$ta_bpr_apn`
    GROUP BY
        cntrct_id;

    -- Original: dbms_output.put_line('OK');
    SELECT 'd_ausd_bp_ta_apn_vertrag_proc completed successfully.' AS status;

END;