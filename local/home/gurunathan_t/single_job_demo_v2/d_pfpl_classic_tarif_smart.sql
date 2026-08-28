DECLARE c_anz_differenzen INT64;
DECLARE c_exit INT64;

SET (c_anz_differenzen, c_exit) = (
  WITH v_target AS
    (
    SELECT parameter_text AS target_database
    FROM `GCP_PROJECT.BQ_DATASET.dwh$ta_l_map_plato_param`
    WHERE parameter_name = 'TARGET_DATABASE_SMART'
      AND guelig_bis = DATETIME '4712-12-31 00:00:00'
    ),
  v_version_smart AS
    (
    SELECT MAX(version_smart) AS max_version_smart
    FROM `GCP_PROJECT.BQ_DATASET.dwh$ta_l_map_plato_tarif_smart`
    ),
  aktuell AS
    (
    SELECT 
      dwh_m.tarif_id,
      dwh_m.tarif_bez,
      dwh_m.mp_marktprodukt_id,
      dwh_m.mp_marktprodukt_bez,
      dwh_m.mp_familie_id,
      dwh_m.mp_familie_bez,
      dwh_m.mp_typ_id,
      dwh_m.mp_typ_bez,
      dwh_m.mp_tarifart_id,
      dwh_m.mp_tarifart_bez,
      dwh_m.mp_laufzeit_id,
      dwh_m.mp_laufzeit_bez,
      dwh_m.mp_bestandsrelevanz_id,
      dwh_m.mp_bestandsrelevanz_bez,
      dwh_m.mp_tarifsparte_id,
      dwh_m.mp_tarifsparte_bez,
      dwh_m.mp_vertragsbesonderheit_id,
      dwh_m.mp_vertragsbesonderheit_bez,
      dwh_m.mp_taktung_id,
      dwh_m.mp_taktung_bez,
      dwh_m.mp_sonderkartenart_id,
      dwh_m.mp_sonderkartenart_bez,
      dwh_m.mp_sonderkartentyp_id,
      dwh_m.mp_sonderkartentyp_bez,
      dwh_m.mp_eg_jn_id,
      dwh_m.mp_eg_jn_bez,
      dwh_m.mp_geschaeftsfeld_id,
      dwh_m.mp_geschaeftsfeld_bez,
      dwh_m.mp_provider_id,
      dwh_m.mp_provider_bez,
      dwh_m.mp_generation_id,
      dwh_m.mp_generation_bez,
      dwh_m.mp_standard_grundpreis_id,
      dwh_m.mp_standard_grundpreis_bez,
      dwh_m.mp_startguthaben_id,
      dwh_m.mp_startguthaben_bez,
      plato.plato_sparte_id,
      plato.plato_sparte_text,
      plato.plato_tarifart_id,
      plato.plato_tarifart_text,
      plato.plato_geschaeftsfeld_id,
      plato.plato_geschaeftsfeld_text,
      plato.plato_eg_jn_id,
      plato.plato_eg_jn_text,
      plato.mp_plato_id,
      plato.mp_plato_text,
      plato.plato_tarif_id,
      plato.plato_tarif_text,
      plato.plato_plan_tarif_vertrieb_id,
      plato.plato_plan_tarif_vertrieb_text,
      plato.plato_plan_tarif_market_id,
      plato.plato_plan_tarif_market_text,
      plato.plato_tariffamilie_id,
      plato.plato_tariffamilie_text,
      plato.plato_tariftyp_id,
      plato.plato_tariftyp_text,
      plato.gueltig_von,
      plato.gueltig_bis,
      plato.target_database
    FROM `GCP_PROJECT.BQ_DATASET.d_tarif` AS dwh_m
    CROSS JOIN v_target
    INNER JOIN `GCP_PROJECT.BQ_DATASET.dwh$ta_l_map_plato_mp_tarif` AS plato
      ON plato.mp_plato_id = CONCAT(
        CAST(dwh_m.mp_marktprodukt_id AS STRING), '-',
        CAST(dwh_m.mp_tarifsparte_id AS STRING), '-',
        CAST(dwh_m.mp_geschaeftsfeld_id AS STRING), '-',
        CAST(dwh_m.mp_eg_jn_id AS STRING), '-',
        CAST(dwh_m.mp_generation_id AS STRING)
      )
      AND plato.gueltig_bis = DATETIME '4712-12-31 00:00:00'
      AND plato.target_database = v_target.target_database
    ),
  smart AS
    (
    SELECT 
      tarif_id,
      tarif_bez,
      mp_marktprodukt_id,
      mp_marktprodukt_bez,
      mp_familie_id,
      mp_familie_bez,
      mp_typ_id,
      mp_typ_bez,
      mp_tarifart_id,
      mp_tarifart_bez,
      mp_laufzeit_id,
      mp_laufzeit_bez,
      mp_bestandsrelevanz_id,
      mp_bestandsrelevanz_bez,
      mp_tarifsparte_id,
      mp_tarifsparte_bez,
      mp_vertragsbesonderheit_id,
      mp_vertragsbesonderheit_bez,
      mp_taktung_id,
      mp_taktung_bez,
      mp_sonderkartenart_id,
      mp_sonderkartenart_bez,
      mp_sonderkartentyp_id,
      mp_sonderkartentyp_bez,
      mp_eg_jn_id,
      mp_eg_jn_bez,
      mp_geschaeftsfeld_id,
      mp_geschaeftsfeld_bez,
      mp_provider_id,
      mp_provider_bez,
      mp_generation_id,
      mp_generation_bez,
      mp_standard_grundpreis_id,
      mp_standard_grundpreis_bez,
      mp_startguthaben_id,
      mp_startguthaben_bez,
      plato_sparte_id,
      plato_sparte_text,
      plato_tarifart_id,
      plato_tarifart_text,
      plato_geschaeftsfeld_id,
      plato_geschaeftsfeld_text,
      plato_eg_jn_id,
      plato_eg_jn_text,
      mp_plato_id,
      mp_plato_text,
      plato_tarif_id,
      plato_tarif_text,
      plato_plan_tarif_vertrieb_id,
      plato_plan_tarif_vertrieb_text,
      plato_plan_tarif_market_id,
      plato_plan_tarif_market_text,
      plato_tariffamilie_id,
      plato_tariffamilie_text,
      plato_tariftyp_id,
      plato_tariftyp_text,
      gueltig_von,
      gueltig_bis,
      target_database
    FROM `GCP_PROJECT.BQ_DATASET.dwh$ta_l_map_plato_tarif_smart`
    CROSS JOIN v_version_smart
    WHERE `GCP_PROJECT.BQ_DATASET.dwh$ta_l_map_plato_tarif_smart`.version_smart = v_version_smart.max_version_smart
    ),
  differenz_aktuell_smart AS
    (
    SELECT * FROM aktuell
    EXCEPT DISTINCT
    SELECT * FROM smart
    ),
  differenz_smart_aktuell AS
    (
    SELECT * FROM smart
    EXCEPT DISTINCT
    SELECT * FROM aktuell
    ),
  summe_differenz AS
    (
    SELECT (
        (SELECT COUNT(*) FROM differenz_aktuell_smart)
        +
        (SELECT COUNT(*) FROM differenz_smart_aktuell)
      ) AS val_differenz
    )
  SELECT AS STRUCT 
    val_differenz AS col_anz_differenzen,
    CASE 
      WHEN val_differenz = 0 THEN 0
      ELSE 100
    END AS col_exit
  FROM summe_differenz
);

SELECT CONCAT('Anzahl Differenzen : ', CAST(c_anz_differenzen AS STRING)) AS execution_log;

IF c_exit != 0 THEN
  ERROR(
    CONCAT('Data validation check failed. Validation code: ', CAST(c_exit AS STRING), '. Differences detected: ', CAST(c_anz_differenzen AS STRING))
  );
END IF;