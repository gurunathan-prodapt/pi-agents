-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/d_ausd_bp_ta_rn_einzeln.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Description: Core SQL logic migrated from d_ausd_bp_ta_rn_einzeln.sql.
--              This file contains only the INSERT...SELECT statement,
--              with `_stichtag_yyyymmdd` as a placeholder for the key date in YYYYMMDD format.

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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                       WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)
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
                            CASE WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd) THEN 'L'
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
                  CASE WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd) THEN 'L'
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
                  CASE WHEN ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd) THEN 'L'
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