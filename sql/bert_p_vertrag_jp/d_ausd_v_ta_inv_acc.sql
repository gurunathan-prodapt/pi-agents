CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_inv_acc` AS
SELECT
  ia.cntrct_id,
  id.inv_definition_id,
  id.inv_pay_ty_cv,
  id.inv_media_cv,
  id.billcycle_id,
  id.sales_tax_freed,
  ar.account_reference,
  id.rechn_inh_konfig_text
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_inv_assign` ia
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_inv_def` id
  ON ia.inv_definition_id = id.inv_definition_id
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_acc_ref` ar
  ON id.acc_ref_id = ar.acc_ref_id;