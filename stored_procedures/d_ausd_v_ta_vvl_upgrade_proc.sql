-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_upgrade.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
--
-- BigQuery Stored Procedure to encapsulate the core SQL logic of d_ausd_v_ta_vvl_upgrade.sql.
--
-- IMPORTANT: The original SQL content is commented out below. It must be manually
--            migrated from its original Oracle SQL dialect to BigQuery SQL syntax.
--            This procedure currently serves as a placeholder.
--
-- The procedure is expected to return the number of records it processed.

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_vvl_upgrade_proc`(
    OUT processed_records INT64
)
BEGIN
    -- Initialize processed_records to 0
    SET processed_records = 0;

    -- Original SQL content from d_ausd_v_ta_vvl_upgrade.sql (Oracle SQL dialect):
    /*
    prompt variablendefinitionen
    DEFINE v_carmen = "@pcrs1"

    COLUMN s_datum new_value v_datum noprint
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
      FROM isbert_schema.dwtk_meldungen m
     WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    prompt tracing und settings
    START ../trace.sql.cfg
    SPOOL ./tmp/trace_d_ausd_v_ta_vvl_upgrade

    WHENEVER SQLERROR CONTINUE
      SET TIMING ON
      SET SERVEROUTPUT ON
    WHENEVER SQLERROR EXIT FAILURE

    prompt tabelle von vorherigem lauf leeren
    WHENEVER SQLERROR CONTINUE
    begin
    isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_vvl_upgrade');
    end;
    /
    WHENEVER SQLERROR EXIT FAILURE

    prompt zieltabelle anlegen
    INSERT INTO sof$ta_vvl_upgrade(
              vertrags_id,
              upgradegrund,
              upgradedatum)
       SELECT --+ parallel(vvl,4)  parallel(vvl2,4)
              vvl.vertrags_id,
              CASE
                 WHEN ba.beschreibung = 'DPPS Diensttyp A13 (EG-Upgrade)'
                 THEN 'Endger\xc3\xa4teupgrade'
                 ELSE ba.beschreibung
              END                 upgradegrund,
              vvl2.upgr_datum     upgradedatum
       FROM   sof$ta_vvl_dwh       vvl,
              dwh$ta_l_bindefr_aendgr_carm  ba,
              (select --+ parallel(vvlt,4)
                       vertrags_id,
                       max(aenderung_am) upgr_datum
                 from sof$ta_vvl_dwh vvlt
               group by vertrags_id)  vvl2
       WHERE
              ba.vvl_aendgrund_id   = vvl.vvl_aendgrund_id
       AND    vvl.vertrags_id       = vvl2.vertrags_id
       AND    vvl.aenderung_am      = vvl2.upgr_datum;

    commit;

    prompt Verarbeitung fehlerfrei beendet.
    spool off
    */

    -- Placeholder for the migrated BigQuery SQL logic.
    -- Example:
    -- INSERT INTO `project.dataset.ta_vvl_upgrade` (vertrags_id, upgradegrund, upgradedatum)
    -- SELECT
    --     t1.contract_id,
    --     CASE WHEN t2.description = 'DPPS Diensttyp A13 (EG-Upgrade)' THEN 'Endger\xc3\xa4teupgrade' ELSE t2.description END,
    --     t3.upgrade_date
    -- FROM
    --     `project.dataset.sof_ta_vvl_dwh` t1
    -- JOIN
    --     `project.dataset.dwh_ta_l_bindefr_aendgr_carm` t2 ON t1.vvl_aendgrund_id = t2.vvl_aendgrund_id
    -- JOIN
    -- (
    --     SELECT
    --         contract_id,
    --         MAX(change_date) AS upgrade_date
    --     FROM
    --         `project.dataset.sof_ta_vvl_dwh`
    --     GROUP BY
    --         contract_id
    -- ) t3 ON t1.contract_id = t3.contract_id AND t1.change_date = t3.upgrade_date;
    --
    -- SET processed_records = @@row_count; -- Or similar method to get affected rows.

    -- For demonstration, let's simulate some records processed
    -- This should be replaced by the actual count from your migrated DML statement.
    SET processed_records = 12345; -- Dummy value, replace with actual affected row count from DML.

END;