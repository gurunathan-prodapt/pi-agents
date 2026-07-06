CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_vvl_upgrade` AS
WITH vvl2 AS (
  SELECT
    vertrags_id,
    MAX(aenderung_am) AS upgr_datum
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_vvl_dwh`
  GROUP BY vertrags_id
)
SELECT
  vvl.vertrags_id,
  CASE
    WHEN ba.beschreibung = 'DPPS Diensttyp A13 (EG-Upgrade)' THEN 'Endgeräteupgrade'
    ELSE ba.beschreibung
  END AS upgradegrund,
  vvl2.upgr_datum AS upgradedatum
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_vvl_dwh` vvl
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_dwh.dwh_ta_l_bindefr_aendgr_carm` ba
  ON ba.vvl_aendgrund_id = vvl.vvl_aendgrund_id
INNER JOIN vvl2
  ON vvl.vertrags_id = vvl2.vertrags_id
 AND vvl.aenderung_am = vvl2.upgr_datum;