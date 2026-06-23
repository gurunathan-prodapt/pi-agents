-- BigQuery Stored Procedure for data processing logic from d_ausd_v_ta_barrier_zusgf.sql
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.p_d_ausd_v_ta_barrier_zusgf`()
BEGIN
    -- Truncate target table before inserting new data
    TRUNCATE TABLE `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF`;

    INSERT INTO `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF` (
        cntrct_id,
        sperrart_alle,
        sperrgrund_alle,
        stilllegungszeitraum_alle,
        sperrgrund_zusgf
    )
    SELECT
        cntrct_id,
        -- Aggregate and limit length for sperrart_alle
        SUBSTR(STRING_AGG(DISTINCT sperrart_clean ORDER BY sperrart_clean SEPARATOR ','), 1, 500) AS sperrart_alle,
        -- Aggregate and limit length for sperrgrund_alle
        SUBSTR(STRING_AGG(DISTINCT sperrgrund ORDER BY sperrgrund SEPARATOR ','), 1, 500) AS sperrgrund_alle,
        -- Aggregate and limit length for stilllegungszeitraum_alle
        SUBSTR(STRING_AGG(DISTINCT stilllegungszeitraum_calc ORDER BY stilllegungszeitraum_calc SEPARATOR ', '), 1, 100) AS stilllegungszeitraum_alle,
        -- Determine sperrgrund_zusgf: if any derived value is 3, then 3; otherwise, 2.
        MAX(sperrgrund_zusgf_calc) AS sperrgrund_zusgf
    FROM (
        SELECT DISTINCT
            bar.cntrct_id,
            -- Equivalent to REPLACE(REPLACE(sperrart,'Rufnummern',''),' ','')
            REPLACE(REPLACE(bar.sperrart, 'Rufnummern', ''), ' ', '') AS sperrart_clean,
            bar.sperrgrund,
            -- Equivalent to Oracle's DECODE for stilllegungszeitraum
            CASE
                WHEN bar.ist_stillegung = 1 THEN
                    CASE
                        WHEN bar.sperr_ende IS NULL THEN CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(bar.sperr_beginn)))
                        ELSE CONCAT(FORMAT_DATE('%d.%m.%Y', DATE(bar.sperr_beginn)), ' - ', FORMAT_DATE('%d.%m.%Y', DATE(bar.sperr_ende)))
                    END
                ELSE NULL
            END AS stilllegungszeitraum_calc,
            -- Equivalent to Oracle's DECODE(barrier_reason_cv,2,2,3)
            CASE bar.barrier_reason_cv WHEN 2 THEN 2 ELSE 3 END AS sperrgrund_zusgf_calc
        FROM
            `your_project_id.your_dataset_id.SOF_TA_BARRIER` AS bar
    )
    GROUP BY
        cntrct_id;
END;