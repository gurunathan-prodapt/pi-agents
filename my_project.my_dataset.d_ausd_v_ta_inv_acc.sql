-- BigQuery Stored Procedure for core data transformation
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_v_ta_inv_acc.sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.d_ausd_v_ta_inv_acc`(
    p_EintragsNr STRING,
    p_JobKennung STRING
)
BEGIN
    -- No direct use of p_EintragsNr and p_JobKennung in the original SQL,
    -- but included for interface consistency as per design document.

    -- Truncate the target table
    TRUNCATE TABLE `my_project.sof_dataset.sof$ta_inv_acc`;

    -- Insert data into the target table
    INSERT INTO `my_project.sof_dataset.sof$ta_inv_acc`(
           cntrct_id,
           inv_definition_id,
           inv_pay_ty_cv,
           inv_media_cv,
           billcycle_id,
           sales_tax_freed,
           account_reference,
           rechn_inh_konfig_text)
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
            `my_project.sof_dataset.sof$ta_inv_assign`   ia,
            `my_project.sof_dataset.sof$ta_inv_def`      id,
            `my_project.sof_dataset.sof$ta_acc_ref`      ar
      WHERE
            ia.inv_definition_id = id.inv_definition_id
      AND   id.acc_ref_id        = ar.acc_ref_id;

END;