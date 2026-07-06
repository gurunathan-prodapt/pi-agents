DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_barrier` AS
SELECT
  b.cntrct_id,
  bc.barrier_kind_id,
  dk.cds_description AS sperrart,
  bc.barrier_init_cv,
  bc.barrier_reason_cv,
  COALESCE(b.net_barr_on_date, b.valid_from) AS sperr_beginn,
  COALESCE(b.net_barr_off_date, b.valid_to) AS sperr_ende,
  CASE bc.barrier_reason_cv
    WHEN 1 THEN 'Kartenverlust'
    WHEN 2 THEN 'Kundenwunsch'
    WHEN 3 THEN 'Betreiberinterne Sperre'
    WHEN 4 THEN 'Betreiberinterne Sperre'
    WHEN 7 THEN 'Betreiberinterne Sperre'
    WHEN 9 THEN 'wegen Kartenlieferung (LZE)'
    WHEN 10 THEN 'vorzeitige Aktivierung /Stillgelegt'
    WHEN 11 THEN 'Serviceproviderwunsch'
    WHEN 13 THEN 'Betreiberinterne Sperre'
    WHEN 14 THEN 'Betreiberinterne Sperre'
    WHEN 15 THEN 'Sterbefall/Stillgelegt'
    WHEN 16 THEN 'Telefonische Aktivierung'
    WHEN 17 THEN 'Betreiberinterne Sperre'
    WHEN 18 THEN 'Betreiberinterne Sperre'
    WHEN 19 THEN 'Stillgelegt'
    WHEN 20 THEN 'Verspaetete Endgeraetelieferung'
    WHEN 21 THEN 'Betreiberinterne Sperre'
    WHEN 22 THEN 'Kartenrücksendung'
    WHEN 23 THEN 'Betreiberinterne Sperre'
    WHEN 24 THEN 'Betreiberinterne Sperre'
    WHEN 25 THEN 'Betreiberinterne Sperre'
    WHEN 26 THEN 'Betreiberinterne Sperre'
    WHEN 27 THEN 'Aufhebung/Auslauf des Vertrages'
    WHEN 28 THEN 'Vertragsübernahme/ neuer Vertrag'
    WHEN 29 THEN 'Beauftragungsprozess'
    WHEN 30 THEN 'Endgeraet nicht zuordenbar'
    WHEN 31 THEN 'RV-Wunsch'
    WHEN 32 THEN 'Betreiberinterne Sperre'
    ELSE 'Betreiberinterne Sperre'
  END AS sperrgrund,
  CASE WHEN b.insert_at > bc.insert_at THEN b.insert_at ELSE bc.insert_at END AS bfc_age,
  CASE WHEN bc.closure = 1 THEN 1 ELSE 0 END AS ist_stillegung
FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_barrier` b
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_barrier_class` bc
  ON b.barrier_class_id = bc.barrier_class_id
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_barrier_kind` bk
  ON bk.barrier_kind_id = bc.barrier_kind_id
INNER JOIN `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_carmen_mirror.cds_ta_care_description` dk
  ON dk.cds_description_id = bk.cds_description_id
WHERE
  FORMAT_TIMESTAMP('%Y%m%d', b.insert_at) <= v_datum
  AND (b.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', b.modified_at) > v_datum)
  AND FORMAT_TIMESTAMP('%Y%m%d', b.valid_from) <= v_datum
  AND (b.valid_to IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', b.valid_to) > v_datum)
  AND b.is_production = 1
  AND FORMAT_TIMESTAMP('%Y%m%d', bk.insert_at) <= v_datum
  AND (bk.modified_at IS NULL OR FORMAT_TIMESTAMP('%Y%m%d', bk.modified_at) > v_datum);