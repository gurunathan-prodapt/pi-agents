--
-- BigQuery SQL for transforming and loading barrier data
-- Replaces: d_ausd_v_ta_barrier_zusgf.sql (invoked by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh)
--
-- This script truncates the target table and inserts transformed data
-- from the source barrier table.
-- Placeholders for project_id, source_dataset, and target_dataset need to be replaced.
--

TRUNCATE TABLE `{{ project_id }}.{{ target_dataset }}.sof_ta_barrier_zusgf`;

INSERT INTO `{{ project_id }}.{{ target_dataset }}.sof_ta_barrier_zusgf`
  (cntrct_id,
   sperrart_alle,
   sperrgrund_alle,
   stilllegungszeitraum_alle,
   sperrgrund_zusgf)
WITH barrier_src AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    REPLACE(REPLACE(sperrart, 'Rufnummern', ''), ' ', '') AS sperrart,
    sperrgrund,
    CASE
      WHEN ist_stillegung = 1 THEN
        CASE
          WHEN sperr_ende IS NULL THEN
            CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)))
          ELSE
            CONCAT(
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)),
              ' - ',
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_ende))
            )
        END
      ELSE NULL
    END AS stilllegungszeitraum_alle,
    CASE
      WHEN barrier_reason_cv = 2 THEN 2
      ELSE 3
    END AS sperrgrund_zusgf
  FROM `{{ project_id }}.{{ source_dataset }}.sof_ta_barrier`
),
agg AS (
  SELECT
    cntrct_id,
    STRING_AGG(sperrart, ',' ORDER BY sperrart) AS sperrart_alle,
    STRING_AGG(sperrgrund, ',' ORDER BY sperrart) AS sperrgrund_alle,
    STRING_AGG(stilllegungszeitraum_alle, ', ' ORDER BY sperrart) AS stilllegungszeitraum_alle,
    CASE
      WHEN COUNTIF(sperrgrund_zusgf != 2) > 0 THEN 3
      ELSE 2
    END AS sperrgrund_zusgf
  FROM barrier_src
  GROUP BY cntrct_id
)
SELECT
  cntrct_id,
  sperrart_alle,
  sperrgrund_alle,
  stilllegungszeitraum_alle,
  sperrgrund_zusgf
FROM agg;