-- BigQuery Stored Procedure for core SQL logic
-- Legacy Source: d_ausd_bp_ta_bpr_basis_his.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_basis_his`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING, -- Included for consistency with orchestrator, though not explicitly used in KSH call
  IN p_datum_heute STRING,
  IN p_datum_gestern STRING
)
BEGIN
  -- Placeholder for the actual logic from d_ausd_bp_ta_bpr_basis_his.sql
  -- This procedure should implement the data transformation and insertion
  -- into `project.dataset.PoolBasisprodukt`.

  -- Example: Insert a dummy record (replace with actual logic)
  INSERT INTO `project.dataset.PoolBasisprodukt` (id, produkt_name, stichtag, eintrags_nr, job_kennung, last_update_ts)
  VALUES (
    CAST(FARM_FINGERPRINT(GENERATE_UUID()) AS INT64), -- Dummy ID
    'DummyProdukt',
    PARSE_DATE('%d%m%Y', p_Stichtag),
    p_EintragsNr,
    p_JobKennung,
    CURRENT_TIMESTAMP()
  );

  -- In a real scenario, this would contain complex SQL DML statements,
  -- potentially involving other tables, joins, and aggregations.
  -- The `p_datum_heute` and `p_datum_gestern` parameters might be used for
  -- filtering or partitioning.

  SELECT 'Core SQL logic executed for d_ausd_bp_ta_bpr_basis_his.sql' AS status;

END;