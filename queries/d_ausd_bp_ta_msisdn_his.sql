-- queries/d_ausd_bp_ta_msisdn_his.sql
-- Target: BigQuery
-- Purpose: Rebuild MSISDN history table from replicated Carmen source data

-- Truncate target table to support reruns on the same day
TRUNCATE TABLE `gcp-project-id.sof_dataset.ta_msisdn_his`;

-- Insert valid MSISDN records into the target table
INSERT INTO `gcp-project-id.sof_dataset.ta_msisdn_his`
(
    BPRI_COM_ID,
    MSISDN,
    CALLNUMBER_ROLE_ID,
    VALID_TO
)
SELECT
    cn1.bpri_com_id AS BPRI_COM_ID,
    -- Preserve Oracle-style NULL concatenation semantics using COALESCE
    CONCAT(
        COALESCE(cn1.cc, ''),
        COALESCE(cn1.ndc, ''),
        COALESCE(cn1.sn, '')
    ) AS MSISDN,
    cn1.callnumber_role_id AS CALLNUMBER_ROLE_ID,
    cn1.valid_to AS VALID_TO
FROM `gcp-project-id.pds_dataset.ta_callnumber` cn1
WHERE cn1.insert_at <= DATE(
        COALESCE(
            (
                SELECT MAX(DATE(m.timecreated))
                FROM `gcp-project-id.isbert_dataset.dwtk_meldungen` m
                WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            ),
            DATE '1900-01-01'
        )
    )
  AND (
        cn1.modified_at IS NULL
        OR cn1.modified_at > DATE(
            COALESCE(
                (
                    SELECT MAX(DATE(m.timecreated))
                    FROM `gcp-project-id.isbert_dataset.dwtk_meldungen` m
                    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
                ),
                DATE '1900-01-01'
            )
        )
      )
  AND cn1.valid_from <= DATE(
        COALESCE(
            (
                SELECT MAX(DATE(m.timecreated))
                FROM `gcp-project-id.isbert_dataset.dwtk_meldungen` m
                WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            ),
            DATE '1900-01-01'
        )
    )
  AND cn1.is_production = 1
  -- Filter for restart mechanism (defaults to 0 if not set)
  AND cn1.bpri_com_id > @wiederanlaufwert;