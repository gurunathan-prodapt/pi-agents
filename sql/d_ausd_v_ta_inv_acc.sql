-- Legacy source: vobs/dw_source/isrpt/isbert/aufbereitung/sql/d_ausd_v_ta_inv_acc.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

-- This script performs data insertion into the target table `isrpt_isbert_prod.sof_ta_inv_acc`.
-- The TRUNCATE operation is handled by a separate Airflow task.
-- BigQuery automatically handles parallelism, so Oracle-specific PARALLEL hints are removed.

INSERT INTO `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` (
       cntrct_id,
       inv_definition_id,
       inv_pay_ty_cv,
       inv_media_cv,
       billcycle_id,
       sales_tax_freed,
       account_reference,
       rechn_inh_konfig_text
)
  SELECT
       ia.cntrct_id,
       id.inv_definition_id,
       id.inv_pay_ty_cv,
       id.inv_media_cv,
       id.billcycle_id,
       id.sales_tax_freed,
       ar.account_reference,
       id.rechn_inh_konfig_text
  FROM
        `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_assign`   ia,
        `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_def`      id,
        `gcp-project-id.isrpt_isbert_prod.sof_ta_acc_ref`      ar
  WHERE
        ia.inv_definition_id = id.inv_definition_id
  AND   id.acc_ref_id        = ar.acc_ref_id;