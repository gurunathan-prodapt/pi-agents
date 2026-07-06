CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_barrier_zusgf` AS
WITH prepped_barriers AS (
  SELECT DISTINCT
    cntrct_id,
    REGEXP_REPLACE(REGEXP_REPLACE(sperrart, 'Rufnummern', ''), r'\s+', '') AS norm_sperrart,
    sperrgrund,
    CASE ist_stillegung
      WHEN 1 THEN
        CASE
          WHEN sperr_ende IS NULL THEN CONCAT('ab ', FORMAT_TIMESTAMP('%d.%m.%Y', sperr_beginn))
          ELSE CONCAT(FORMAT_TIMESTAMP('%d.%m.%Y', sperr_beginn), ' - ', FORMAT_TIMESTAMP('%d.%m.%Y', sperr_ende))
        END
      ELSE NULL
    END AS st_zeitraum,
    CASE barrier_reason_cv WHEN 2 THEN 2 ELSE 3 END AS numerical_reason
  FROM `{{ var.value.gcp_project }}.{{ var.value.env_prefix }}_staging.sof_ta_barrier`
)
SELECT
  cntrct_id,
  STRING_AGG(norm_sperrart, ',' ORDER BY norm_sperrart) AS sperrart_alle,
  STRING_AGG(sperrgrund, ',' ORDER BY sperrgrund) AS sperrgrund_alle,
  STRING_AGG(st_zeitraum, ', ' ORDER BY st_zeitraum) AS stilllegungszeitraum_alle,
  MAX(numerical_reason) AS sperrgrund_zusgf
FROM prepped_barriers
GROUP BY cntrct_id;