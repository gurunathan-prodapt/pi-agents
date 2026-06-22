-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_barrier_zusgf.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_barrier_zusgf`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  DECLARE v_datum DATE;
  DECLARE records_inserted INT64;
  DECLARE v_error_message STRING;

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_error_message = 'ERROR: p_JobKennung cannot be NULL or empty.';
    INSERT INTO `project.dataset.execution_log` (job_kennung, eintrags_nr, execution_timestamp, status, message)
    VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'FAILED', v_error_message);
    RAISE BQ.INVALID_ARGUMENT_ERROR(v_error_message);
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_error_message = 'ERROR: p_EintragsNr cannot be NULL or empty.';
    INSERT INTO `project.dataset.execution_log` (job_kennung, eintrags_nr, execution_timestamp, status, message)
    VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'FAILED', v_error_message);
    RAISE BQ.INVALID_ARGUMENT_ERROR(v_error_message);
  END IF;

  -- Log start of procedure
  INSERT INTO `project.dataset.execution_log` (job_kennung, eintrags_nr, execution_timestamp, status, message)
  VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'RUNNING', 'Procedure started.');

  BEGIN
    -- Retrieve v_datum from dwtk_meldungen
    SELECT MAX(DATE(timecreated))
    INTO v_datum
    FROM `project.dataset.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';

    IF v_datum IS NULL THEN
      SET v_error_message = 'WARNING: No valid v_datum found from dwtk_meldungen. Proceeding without date filter.';
      INSERT INTO `project.dataset.execution_log` (job_kennung, eintrags_nr, execution_timestamp, status, message)
      VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'WARNING', v_error_message);
    END IF;

    -- Truncate the target table
    TRUNCATE TABLE `project.dataset.sof_ta_barrier_zusgf`;

    -- Core transformation and insert logic
    INSERT INTO `project.dataset.sof_ta_barrier_zusgf`
    (
      cntrct_id,
      sperrart_alle,
      sperrgrund_alle,
      stilllegungszeitraum_alle,
      sperrgrund_zusgf
    )
    WITH src AS (
      SELECT DISTINCT
        cntrct_id,
        REPLACE(REPLACE(sperrart, 'Rufnummern', ''), ' ', '') AS sperrart,
        sperrgrund,
        CASE
          WHEN ist_stillegung = 1 THEN
            CASE
              WHEN sperr_ende IS NULL THEN CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)))
              ELSE CONCAT(
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
      FROM `project.dataset.sof_ta_barrier`
      -- Optionally filter by v_datum if a clear use case emerges from legacy logic
      -- WHERE (v_datum IS NULL OR <some_date_column> <= v_datum)
    ),
    grp AS (
      SELECT
        cntrct_id,
        ARRAY_AGG(sperrart IGNORE NULLS ORDER BY sperrart) AS arr_sperrart, -- Maintain order for consistent STRING_AGG
        ARRAY_AGG(sperrgrund IGNORE NULLS ORDER BY sperrart) AS arr_sperrgrund,
        ARRAY_AGG(stilllegungszeitraum_alle IGNORE NULLS ORDER BY sperrart) AS arr_stilllegung,
        ARRAY_AGG(sperrgrund_zusgf IGNORE NULLS ORDER BY sperrart) AS arr_zusgf
      FROM src
      GROUP BY cntrct_id
    )
    SELECT
      cntrct_id,
      (
        SELECT STRING_AGG(x, ',' ORDER BY off)
        FROM UNNEST(arr_sperrart) AS x WITH OFFSET off
        -- Max length handling can be added here, e.g., SUBSTR(STRING_AGG(...), 1, 500)
      ) AS sperrart_alle,
      (
        SELECT STRING_AGG(x, ',' ORDER BY off)
        FROM UNNEST(arr_sperrgrund) AS x WITH OFFSET off
        -- Max length handling can be added here, e.g., SUBSTR(STRING_AGG(...), 1, 500)
      ) AS sperrgrund_alle,
      (
        SELECT STRING_AGG(x, ', ' ORDER BY off)
        FROM UNNEST(arr_stilllegung) AS x WITH OFFSET off
        -- Max length handling can be added here, e.g., SUBSTR(STRING_AGG(...), 1, 100)
      ) AS stilllegungszeitraum_alle,
      CASE
        WHEN EXISTS (
          SELECT 1
          FROM UNNEST(arr_zusgf) AS x
          WHERE x != 2
        ) THEN 3
        ELSE 2
      END AS sperrgrund_zusgf
    FROM grp;

    SET records_inserted = @@row_count;

    -- Log successful completion
    INSERT INTO `project.dataset.execution_log` (job_kennung, eintrags_nr, execution_timestamp, record_count, status, message)
    VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), records_inserted, 'SUCCESS', 'Procedure completed successfully.');

  EXCEPTION WHEN ERROR THEN
    SET v_error_message = FORMAT("Procedure failed: %s", @@error.message);
    INSERT INTO `project.dataset.execution_log` (job_kennung, eintrags_nr, execution_timestamp, status, message)
    VALUES (p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), 'FAILED', v_error_message);
    RAISE; -- Re-raise the exception
  END;

END;