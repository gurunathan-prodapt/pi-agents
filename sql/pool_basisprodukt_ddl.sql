-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

-- This is a placeholder for the BigQuery DDL for the `PoolBasisprodukt` table
-- and any other tables referenced by `d_ausd_bp_ta_msisdn.sql`.
-- The actual schema definition, data types, and partition/cluster keys
-- need to be determined from the source Oracle database and optimized for BigQuery.

CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_bigquery_dataset.PoolBasisprodukt`
(
    -- Define columns based on the original Oracle table schema.
    -- Example columns (REPLACE with actual schema):
    MSISDN STRING NOT NULL OPTIONS(description="Mobile Subscriber ISDN"),
    SUBSCRIPTION_ID STRING OPTIONS(description="Unique identifier for subscription"),
    PRODUCT_CODE STRING OPTIONS(description="Product code associated with MSISDN"),
    ACTIVATION_DATE DATE OPTIONS(description="Date of activation for the product"),
    STATUS STRING OPTIONS(description="Current status of the MSISDN/product"),
    LAST_UPDATE_TIMESTAMP TIMESTAMP OPTIONS(description="Timestamp of the last update"),
    YOUR_DATE_COLUMN DATE NOT NULL OPTIONS(description="Date column used for partitioning/filtering, e.g., processing date or event date")
)
PARTITION BY YOUR_DATE_COLUMN -- Example: Partition by a date column
CLUSTER BY MSISDN -- Example: Cluster by frequently queried columns
OPTIONS(
    description="Migrated PoolBasisprodukt table from Oracle to BigQuery. Schema needs to be fully defined based on source system."
);

-- Add DDL for any other tables if `d_ausd_bp_ta_msisdn.sql` reads from/writes to them.