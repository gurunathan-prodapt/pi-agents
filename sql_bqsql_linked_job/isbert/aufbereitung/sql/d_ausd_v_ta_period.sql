DECLARE v_datum STRING;

SET v_datum = ( 
  SELECT COALESCE(FORMAT_DATETIME('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `isbert_schema.sof$ta_period`;

INSERT INTO `isbert_schema.sof$ta_period` (
  period_id,
  number_time_measurement,
  time_meas_cv,
  einheit,
  bfc_age
)
SELECT
  p.period_id,
  p.number_time_measurement,
  p.time_meas_cv,
  d.description,
  p.insert_at
FROM
  `carmen_replicated.cds$ta_period` p
  INNER JOIN `carmen_replicated.cds$ta_time_meas_cv` tm 
    ON tm.time_meas_cv = p.time_meas_cv
  INNER JOIN `carmen_replicated.cds$ta_description` d 
    ON tm.description_id = d.description_id
WHERE
  p.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)
  AND (
    p.modified_at IS NULL
    OR p.modified_at > PARSE_DATETIME('%Y%m%d', v_datum)
  );