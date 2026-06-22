-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Description: BigQuery Stored Procedure encapsulating the orchestration and core logic.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_rn_einzeln`(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING, -- Expected format DDMMYYYY
    p_wiederanlaufWert STRING -- Optional restart value
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_rn_einzeln';
    DECLARE v_stichtag_parsed_date DATE;
    DECLARE v_stichtag_yyyymmdd STRING;
    DECLARE v_record_count INT64;
    DECLARE v_error_message STRING;
    DECLARE v_error_number INT64;
    DECLARE v_status STRING DEFAULT 'SUCCESS';

    -- Start logging for the job
    INSERT INTO `project.dataset.job_table` (
        tab_name, status_a, status_i, stichtag_from, stichtag_to,
        job_type, restart_flag, record_count, description, job_kennung, eintrags_nr, created_ts
    )
    VALUES (
        'sof_ta_rn_einzeln', 'START', 'RUNNING', NULL, NULL,
        'ETL', (p_wiederanlaufWert IS NOT NULL), 0, 'Preparation and execution for rn_einzeln',
        p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP()
    );

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_error_message = 'ERROR: Parameter p_JobKennung is mandatory but was not provided.';
        SET v_error_number = 1001;
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_error_message = 'ERROR: Parameter p_EintragsNr is mandatory but was not provided.';
        SET v_error_number = 1002;
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
        SET v_error_message = 'ERROR: Parameter p_Stichtag is mandatory but was not provided.';
        SET v_error_number = 1003;
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- Date format validation (DDMMYYYY)
    SET v_stichtag_parsed_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_stichtag_parsed_date IS NULL THEN
        SET v_error_message = FORMAT('ERROR: Invalid date format for p_Stichtag: %s. Expected DDMMYYYY.', p_Stichtag);
        SET v_error_number = 1004;
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- Convert validated date to YYYYMMDD string for the SQL statement
    SET v_stichtag_yyyymmdd = FORMAT_DATE('%Y%m%d', v_stichtag_parsed_date);

    BEGIN
        -- Step01: Truncate the target table
        EXECUTE IMMEDIATE 'TRUNCATE TABLE `project.dataset.sof_ta_rn_einzeln`';

        -- Step05: Execute the core SQL logic (INSERT from d_ausd_bp_ta_rn_einzeln.sql)
        -- The following SQL is migrated from d_ausd_bp_ta_rn_einzeln.sql
        INSERT INTO `project.dataset.sof_ta_rn_einzeln`
        (
          CNTRCT_ID,
          TN_MULTI_SINGLE,
          TN_TEL_MSISDN,
          TN_TEL_STATUS,
          TN_TEL_VALID_TO,
          TN_FAX_MSISDN,
          TN_FAX_STATUS,
          TN_FAX_VALID_TO,
          TN_DAT_MSISDN,
          TN_DAT_STATUS,
          TN_DAT_VALID_TO,
          TC_MULTI_SINGLE,
          TC_TEL_MSISDN,
          TC_TEL_STATUS,
          TC_TEL_VALID_TO,
          TC_FAX_MSISDN,
          TC_FAX_STATUS,
          TC_FAX_VALID_TO,
          TC_DAT_MSISDN,
          TC_DAT_STATUS,
          TC_DAT_VALID_TO,
          TB_MULTI_SINGLE,
          TB_TEL_MSISDN,
          TB_TEL_STATUS,
          TB_TEL_VALID_TO,
          TB_FAX_MSISDN,
          TB_FAX_STATUS,
          TB_FAX_VALID_TO,
          TB_DAT_MSISDN,
          TB_DAT_STATUS,
          TB_DAT_VALID_TO,
          DA_RN_MSISDN,
          DA_RN_STATUS,
          DA_RN_VALID_TO,
          VDA_RN_MSISDN,
          VDA_RN_STATUS,
          VDA_RN_VALID_TO,
          TK_RN_MSISDN,
          TK_RN_STATUS,
          TK_RN_VALID_TO,
          MS_RN_1_MSISDN,
          MS_RN_1_STATUS,
          MS_RN_1_VALID_TO,
          MS_RN_2_MSISDN,
          MS_RN_2_STATUS,
          MS_RN_2_VALID_TO
        )
        SELECT
                bp.cntrct_id,
                CASE                        -----Normalkarte-MSISDN-Auswertung
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id = 2
                         THEN 'Multinumbering'
                         ELSE
                            CASE
                               WHEN callnumber_role_id = 1
                               THEN 'Singlenumbering'
                               ELSE null
                            END
                                  END
                           ELSE null
                END                                             as tn_multi_single,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                              CASE
                         WHEN callnumber_role_id = 2
                         THEN ms.msisdn
                         ELSE
                            CASE
                               WHEN callnumber_role_id = 1
                               THEN ms.msisdn
                               ELSE null
                            END
                                  END
                   ELSE null
                    END                      as TN_TEL_msisdn,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id in (1, 2)
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TN_TEL_status,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id in (1,2)
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TN_TEL_valid_to,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                              CASE
                         WHEN callnumber_role_id = 3
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as TN_FAX_msisdn,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id = 3
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TN_FAX_status,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id = 3
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TN_FAX_valid_to,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                              CASE
                         WHEN callnumber_role_id = 5
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as TN_DAT_msisdn,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id = 5
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TN_DAT_status,
                CASE
                           WHEN bp.bpr_id = 31
                           THEN
                      CASE
                         WHEN callnumber_role_id = 5
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TN_DAT_valid_to,
                CASE
                           WHEN bp.bpr_id = 2759          ---------TC-MSISDN-Auswertung
                           THEN
                      CASE
                         WHEN callnumber_role_id = 2
                         THEN 'Multinumbering'
                         ELSE
                            CASE
                               WHEN callnumber_role_id = 1
                               THEN 'Singlenumbering'
                               ELSE null
                            END
                                  END
                           ELSE null
                END                                             as tc_multi_single,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                              CASE
                         WHEN callnumber_role_id = 2
                         THEN ms.msisdn
                         ELSE
                            CASE
                               WHEN callnumber_role_id = 1
                               THEN ms.msisdn
                               ELSE null
                            END
                                  END
                   ELSE null
                    END                      as TC_TEL_msisdn,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                      CASE
                         WHEN callnumber_role_id in (1, 2)
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TC_TEL_status,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                      CASE
                         WHEN callnumber_role_id in (1,2)
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TC_TEL_valid_to,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                              CASE
                         WHEN callnumber_role_id = 3
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as TC_FAX_msisdn,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                      CASE
                         WHEN callnumber_role_id = 3
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TC_FAX_status,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                      CASE
                         WHEN callnumber_role_id = 3
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TC_FAX_valid_to,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                              CASE
                         WHEN callnumber_role_id = 5
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as TC_DAT_msisdn,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                      CASE
                         WHEN callnumber_role_id = 5
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TC_DAT_status,
                CASE
                           WHEN bp.bpr_id = 2759
                           THEN
                      CASE
                         WHEN callnumber_role_id = 5
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TC_DAT_valid_to,
                CASE
                           WHEN bp.bpr_id = 2800                 ----------TB-MSISDN-Auswertung
                           THEN
                      CASE
                         WHEN callnumber_role_id = 2
                         THEN 'Multinumbering'
                         ELSE
                            CASE
                               WHEN callnumber_role_id = 1
                               THEN 'Singlenumbering'
                               ELSE null
                            END
                                  END
                           ELSE null
                END                                             as TB_multi_single,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                              CASE
                         WHEN callnumber_role_id = 2
                         THEN ms.msisdn
                         ELSE
                            CASE
                               WHEN callnumber_role_id = 1
                               THEN ms.msisdn
                               ELSE null
                            END
                                  END
                   ELSE null
                    END                      as TB_TEL_msisdn,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                      CASE
                         WHEN callnumber_role_id in (1, 2)
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TB_TEL_status,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                      CASE
                         WHEN callnumber_role_id in (1,2)
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TB_TEL_valid_to,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                              CASE
                         WHEN callnumber_role_id = 3
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as TB_FAX_msisdn,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                      CASE
                         WHEN callnumber_role_id = 3
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TB_FAX_status,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                      CASE
                         WHEN callnumber_role_id = 3
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TB_FAX_valid_to,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                              CASE
                         WHEN callnumber_role_id = 5
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as TB_DAT_msisdn,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                      CASE
                         WHEN callnumber_role_id = 5
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as TB_DAT_status,
                CASE
                           WHEN bp.bpr_id = 2800
                           THEN
                      CASE
                         WHEN callnumber_role_id = 5
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as TB_DAT_valid_to,
                CASE                               ----------DA-MSISDN-Auswertung
                           WHEN bp.bpr_id = 2835
                           THEN
                              CASE
                         WHEN callnumber_role_id = 7
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as DA_RN_msisdn,
                CASE
                           WHEN bp.bpr_id = 2835
                           THEN
                      CASE
                         WHEN callnumber_role_id = 7
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as DA_RN_status,
                CASE
                           WHEN bp.bpr_id = 2835
                           THEN
                      CASE
                         WHEN callnumber_role_id = 7
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as DA_RN_valid_to,
                CASE                               ----------VDA-MSISDN-Auswertung
                           WHEN bp.bpr_id = 2836
                           THEN
                              CASE
                         WHEN callnumber_role_id = 8
                         THEN ms.msisdn
                         ELSE null
                                  END
                   ELSE null
                    END                     as VDA_RN_msisdn,
                CASE
                           WHEN bp.bpr_id = 2836
                           THEN
                      CASE
                         WHEN callnumber_role_id = 8
                         THEN
                                    CASE
                               WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd)
                               THEN 'L'
                               ELSE 'A'
                                        END
                                         ELSE null
                              END
                           ELSE null
                END                                     as VDA_RN_status,
                CASE
                           WHEN bp.bpr_id = 2836
                           THEN
                      CASE
                         WHEN callnumber_role_id = 8
                         THEN ms.valid_to
                                 ELSE null
                                  END
                           ELSE null
                END                                     as VDA_RN_valid_to,
--      ---------- TK-MSISDN-Auswertung, nur neu-formatiert
                CASE WHEN bp.bpr_id = 2837 THEN
                          CASE WHEN callnumber_role_id = 9 THEN ms.msisdn
                               ELSE null
                          END
                     ELSE null
                END                                                                           as TK_RN_msisdn,
                CASE WHEN bp.bpr_id = 2837 THEN
                          CASE WHEN callnumber_role_id = 9 THEN
                                    CASE WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd) THEN 'L'
                                         ELSE 'A'
                                    END
                               ELSE null
                          END
                     ELSE null
                END                                                                           as TK_RN_status,
                CASE WHEN bp.bpr_id = 2837 THEN
                          CASE WHEN callnumber_role_id = 9 THEN ms.valid_to
                               ELSE null
                          END
                     ELSE null
                END                                                                           as TK_RN_valid_to,
-- -------------------------------------------------------------------------------------------------------
-- NEU, 20070122 ME: MultiSIM, BPR-ID 3848
--      Bereich MS, nur RN (Felder 41 bis 46 fr 2 Slavekarten)
--      Zustzliche Unterscheidung zwischen verschiedenen MultiSIMs anhand der SLAVE_NUMBER, zurzeit (1,2)
--      Bei Einfhrung weiterer MultiSIM-Slavekarten: hier Feldliste analog fortsetzen.
-- -------------------------------------------------------------------------------------------------------
                -- MultiSIM-Karte 1:
                CASE WHEN (bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 1) THEN ms.msisdn
                     ELSE null
                END                                                                           as MS_RN_1_msisdn,
                CASE WHEN (bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 1) THEN
                          CASE WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd) THEN 'L'
                               ELSE 'A'
                          END
                     ELSE null
                END                                                                           as MS_RN_1_status,
                CASE WHEN (bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 1) THEN ms.valid_to
                     ELSE null
                END                                                                           as MS_RN_1_valid_to,
                -- MultiSIM-Karte 2:
                CASE WHEN (bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 2) THEN ms.msisdn
                     ELSE null
                END                                                                           as MS_RN_2_msisdn,
                CASE WHEN (bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 2) THEN
                          CASE WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', v_stichtag_yyyymmdd) THEN 'L'
                               ELSE 'A'
                          END
                     ELSE null
                END                                                                           as MS_RN_2_status,
                CASE WHEN (bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 2) THEN ms.valid_to
                     ELSE null
                END                                                                           as MS_RN_2_valid_to
        FROM   `project.dataset.sof_ta_bpr_basis`        as bp
        JOIN   `project.dataset.sof_ta_msisdn`           as ms
        ON  bp.bpr_instance_id    = ms.bpr_instance_id
        AND    bp.bpr_id             in (31, 2759, 2800, 2835, 2836, 2837, 3848)
        AND    ms.callnumber_role_id in (1, 2, 3, 5, 7, 8, 9, 12);

        -- Get record count
        SELECT COUNT(*) INTO v_record_count FROM `project.dataset.sof_ta_rn_einzeln`;

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_number = @@error.code;
        SET v_status = 'FAILED';

        INSERT INTO `project.dataset.error_log` (
            error_ts, job_name, error_nr, error_arg, message
        )
        VALUES (
            CURRENT_TIMESTAMP(), v_job_name, v_error_number, 'CORE_SQL_EXECUTION', v_error_message
        );
    END;

    -- Update job table with final status and record count
    UPDATE `project.dataset.job_table`
    SET
        status_a = CASE WHEN v_status = 'SUCCESS' THEN 'COMPLETED' ELSE 'FAILED' END,
        status_i = v_status,
        stichtag_from = v_stichtag_parsed_date, -- assuming this is the only date relevant for from/to
        stichtag_to = v_stichtag_parsed_date,
        record_count = COALESCE(v_record_count, 0)
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND created_ts = (SELECT MAX(created_ts) FROM `project.dataset.job_table` WHERE job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr);

    IF v_status = 'FAILED' THEN
        RAISE USING MESSAGE v_error_message;
    END IF;

END;