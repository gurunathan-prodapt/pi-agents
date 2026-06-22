-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
-- This DDL creates the target table for the discount data.
CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_discount` (
  cntrct_id INT64,
  discount_id INT64,
  disc_vector_ty STRING,
  cntrct_obj_version INT64,
  rabatt STRING,
  rabatthoehe STRING
);