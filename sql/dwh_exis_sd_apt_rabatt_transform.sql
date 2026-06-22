-- BigQuery SQL transformation for job EXIS_SD_APT_RABATT
-- Replaces legacy Oracle PL/SQL script `d_exis_apt_rabattdaten.sql`.
-- This SQL generates the core data for the discount report, without the 'D|' prefix or footer.
-- Post-processing (prefixing and footer) will be handled by a Python task in the Airflow DAG.

SELECT
    t.RAHMENVERTRAG_ID,
    t.TARIF_ID,
    t.DWH_TARIFGR_TEXT,
    t.RABATTIERTE_RECH_POS,
    t.RABATTIERTE_RECH_POS_ID,
    t.RABATTHOEHE,
    STRING_AGG(t.BPR_ID, ',' ORDER BY t.BPR_ID) AS BASISPRODUKTE
FROM (
    SELECT DISTINCT
        RPT.RAHMENVERTRAG_ID,
        RPT.DWH_TARIFGR_TEXT,
        DISC.CNTRCT_TEMPLATE_ID AS TARIF_ID,
        DISC.RABATTIERTE_RECH_POS,
        DISC.DISC_INVOICE_ITEM_ID AS RABATTIERTE_RECH_POS_ID,
        DISC.RABATTHOEHE,
        BPR.BPR_ID
    FROM
        `{{ project_id }}.oracle_source.RPT_TA_S_D1_VERTRAG` AS RPT
    INNER JOIN
        `{{ project_id }}.oracle_source.RPT_TA_S_D1_DISCOUNT_RR` AS DISC
        ON RPT.RAHMENVERTRAG_ID = DISC.CONTRACT_NUMBER
        AND RPT.SV_ID = DISC.CNTRCT_TEMPLATE_ID
    INNER JOIN
        `{{ project_id }}.oracle_source.SOF_TA_BPR_OPTIONEN` AS BPR
        ON RPT.VERTRAG_ID_CARMEN = BPR.CNTRCT_ID
    INNER JOIN
        `{{ project_id }}.oracle_source.SOF_VI_L_OPTIONZUORDNUNG` AS OPT
        ON BPR.BPR_ID = OPT.OPTION_ID
) AS t
GROUP BY
    RAHMENVERTRAG_ID,
    TARIF_ID,
    DWH_TARIFGR_TEXT,
    RABATTIERTE_RECH_POS,
    RABATTIERTE_RECH_POS_ID,
    RABATTHOEHE;