-- BigQuery SQL for Cleanup: Truncate Staging Table ta_c_bfc_akt
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This script truncates the staging table after processing.

TRUNCATE TABLE `{{ project_id }}.{{ dataset_id }}.ta_c_bfc_akt`;