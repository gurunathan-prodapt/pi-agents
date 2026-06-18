Document: finaltestingrepo analysis

Step-by-step migration plan

1. Inventory the pipeline by domain
- Sales domain:
  - Daily sales extract, product master load, sales rollup, sales aggregation, data quality.
- Finance domain:
  - GL extract, account load, GL close, reconciliation, account processor, GL aggregation.
- Customer domain:
  - Customer extract, customer scoring, customer historization, customer segmentation, lineage tracking.
- Shared dependencies:
  - Sales publishes RETAIL_DAILY_COMPLETE.
  - Finance publishes FINANCE_GL_CLOSE_COMPLETE.
  - Customer workflow waits on both.

2. Identify what can be migrated directly to BigQuery SQL
- Straight SQL transformations:
  - Filters, joins, rollups, MERGE/UPSERT patterns, window functions, CASE logic, aggregations.
- Stored procedure candidates:
  - SCD Type 2 loads.
  - Period close orchestration.
  - Reconciliation summaries.
  - Segment summary generation.
- BigQuery-native equivalents:
  - CREATE TABLE, INSERT, MERGE, DELETE, SELECT, QUALIFY, window functions, scripting variables, stored procedures.

3. Identify unsupported or non-native logic
- Shell orchestration:
  - ksh scripts, retries, file checks, lock files, event waits.
- UC4/Automic scheduling and event publishing.
- SQL*Loader file ingestion.
- Python dynamic SQL builders and lineage/DQ scripts.
- Spark/Scala jobs with JDBC reads/writes and some iterative logic.
- Ab Initio graph orchestration and partitioning semantics.

4. Migration strategy
- Replace Oracle staging and fact tables with BigQuery datasets and tables.
- Replace SQL*Plus scripts with BigQuery SQL scripts or stored procedures.
- Replace PL/SQL packages with BigQuery stored procedures.
- Replace Ab Initio graphs with BigQuery SQL pipelines:
  - staging load procedure
  - transform procedure
  - reconciliation procedure
  - summary procedure
- Replace Spark jobs with BigQuery SQL where logic is relational.
- Move unsupported orchestration to:
  - Cloud Composer / Workflows / Cloud Scheduler
  - or a thin Python runner if needed for retries and event waits.

5. Domain-by-domain migration mapping

Sales
- sales_extract.sql -> BigQuery SQL script:
  - load STG_SALES_TRANSACTIONS
  - merge STG_CUSTOMER_SALES
- sales_rollup.xfr -> BigQuery SQL:
  - filter invalid rows
  - enrich fields
  - left join DIM_PRODUCT
  - aggregate to FACT_REGIONAL_SUMMARY
- pkg_sales_historization.sql -> BigQuery stored procedure:
  - LOAD_DIM_PRODUCT
  - LOAD_DIM_CUSTOMER
  - LOAD_FACT_SALES
  - GENERATE_REGIONAL_SUMMARY
  - MASTER_LOAD
- sales_aggregation.scala -> BigQuery SQL:
  - product rankings
  - customer segmentation
  - daily summary windows
- retail_data_quality.py -> BigQuery SQL + optional Python wrapper:
  - rule-driven checks
  - insert results into audit table

Finance
- gl_period_extract.sql -> BigQuery SQL script:
  - load STG_GL_TRANSACTIONS
  - refresh STG_PERIOD_RATES
- gl_transform.xfr -> BigQuery SQL:
  - normalize GL rows
  - join DIM_ACCOUNT and FX rates
  - aggregate to FACT_GL_BALANCES
- gl_reconcile.pdl -> BigQuery SQL:
  - partitioned reconciliation query
  - write FACT_PERIOD_RECONCILIATION
- pkg_account_history.sql -> BigQuery stored procedure:
  - LOAD_DIM_ACCOUNT
  - LOAD_DIM_COST_CENTRE
  - LOAD_DIM_PERIOD
- pkg_gl_reconciliation.sql -> BigQuery stored procedure:
  - BUILD_PERIOD_BALANCES
  - RECONCILE_SUBLEDGER
  - CLOSE_PERIOD
  - REBUILD_CUMULATIVE_BALANCES
- gl_aggregation.py -> BigQuery SQL:
  - variance analysis
  - YTD balances
  - P&L by cost centre
- account_processor.scala -> BigQuery SQL:
  - hierarchy flattening
  - intercompany elimination
  - consolidated balances

Customer
- customer_segment_extract.sql -> BigQuery SQL script:
  - load STG_CUSTOMER_PROFILE
  - load STG_CAMPAIGN_EVENTS
  - load STG_CUSTOMER_INTERACTIONS
- customer_transform.xfr -> BigQuery SQL:
  - rollup campaign events
  - join retail spend
  - compute scores
  - filter at-risk
  - segment summary
- pkg_customer_historization.sql -> BigQuery stored procedure:
  - LOAD_DIM_CUSTOMER_CRM
  - LOAD_FACT_CUSTOMER_SCORES
  - AGGREGATE_CAMPAIGN_PERFORMANCE
  - GENERATE_SEGMENT_SUMMARY
  - MASTER_CRM_LOAD
- customer_scoring.py -> BigQuery SQL:
  - dynamic feature config can be modeled with config tables and CASE/SQL generation
- customer_segmentation.scala -> BigQuery SQL:
  - composite score
  - micro-segment assignment
  - region distribution
- crm_lineage_tracker.py -> not a core ETL transform:
  - migrate only if lineage output is required
  - otherwise keep as Python metadata job or replace with BigQuery metadata queries

6. Security and credentials
- Current code uses Oracle credentials from environment variables and vault placeholders.
- BigQuery equivalent:
  - use service account / workload identity
  - store secrets in Secret Manager if external runner is used
  - avoid plaintext passwords
- Authorization:
  - dataset-level IAM
  - table-level access where needed
  - service account permissions for BigQuery jobs and storage if loading files

7. Monitoring and error handling
- Current monitoring:
  - log files
  - DBMS_OUTPUT
  - audit tables
  - alert files
  - email notifications
- BigQuery monitoring:
  - Cloud Logging for job logs
  - Cloud Monitoring alerts on job failures
  - audit tables for row counts and DQ results
  - optional error tables instead of flat files
- Error scenarios to preserve:
  - empty source data
  - missing dimension rows
  - rejected rows
  - reconciliation imbalance
  - critical DQ failures
  - retryable transient failures

8. BigQuery execution design
- Use BigQuery SQL scripts and stored procedures for all relational logic.
- Use Python only for unsupported orchestration:
  - event waiting
  - file-based lock handling
  - external notifications
  - dynamic config retrieval if needed
- Prefer BigQuery scripting variables and MERGE over procedural loops where possible.

Assumptions and Additional Notes
- The repository is internally inconsistent in places:
  - some workflows reference objects not fully defined in the provided files.
  - some scripts mention Oracle packages/tables that are not fully present.
- BigQuery cannot directly replicate:
  - UC4 event waits
  - shell PID locks
  - SQL*Loader
  - file outputs
  - cx_Oracle-based metadata scripts
- Those should be isolated into orchestration or Python wrappers, while all data manipulation is moved into BigQuery SQL.
- Sequence objects in Oracle should be replaced with:
  - GENERATE_UUID() where acceptable, or
  - deterministic hash keys, or
  - surrogate key tables maintained by procedure.
- Flat-file outputs like alert files should be replaced with BigQuery tables or exported to Cloud Storage if required.
- The logic is broadly replicable in BigQuery SQL and stored procedures.

Pseudocode: BQ SQL Pseudocode

1) Sales staging extract

```sql
CREATE OR REPLACE PROCEDURE `project.sales.sp_sales_extract`(
  load_date DATE,
  region_code STRING,
  batch_id STRING
)
BEGIN
  DELETE FROM `project.sales.STG_SALES_TRANSACTIONS`
  WHERE TRANSACTION_DATE = load_date
    AND REGION_CODE = region_code
    AND ETL_STATUS = 'PENDING';

  INSERT INTO `project.sales.STG_SALES_TRANSACTIONS` (
    TRANSACTION_ID, CUSTOMER_ID, PRODUCT_ID, STORE_ID, TRANSACTION_DATE,
    QUANTITY, UNIT_PRICE, DISCOUNT_AMT, DISCOUNT_PCT, REGION_CODE,
    CURRENCY_CODE, PAYMENT_METHOD, POS_TERMINAL_ID, LOAD_DATE, LOAD_BATCH_ID,
    SOURCE_FILE_NAME, ETL_STATUS
  )
  SELECT
    t.TXN_ID,
    t.CUST_ID,
    t.PROD_ID,
    t.STORE_ID,
    DATE(t.TXN_DATETIME),
    t.SOLD_QTY,
    t.UNIT_SELL_PRICE,
    IFNULL(t.DISC_AMOUNT, 0),
    CASE
      WHEN t.UNIT_SELL_PRICE > 0 THEN ROUND((IFNULL(t.DISC_AMOUNT,0) / t.UNIT_SELL_PRICE) * 100, 2)
      ELSE 0
    END,
    t.STORE_REGION_CD,
    IFNULL(t.CURRENCY, 'GBP'),
    t.PAYMENT_TYPE_CD,
    t.TERMINAL_REF,
    load_date,
    CAST(batch_id AS INT64),
    CONCAT(region_code, '_txn_', CAST(load_date AS STRING)),
    'PENDING'
  FROM `project.source_ops.SALES_TXN` t
  WHERE DATE(t.TXN_DATETIME) = load_date
    AND t.STORE_REGION_CD = region_code
    AND t.TXN_STATUS_CD NOT IN ('VOID','CANCELLED','TEST')
    AND t.SOLD_QTY > 0
    AND t.UNIT_SELL_PRICE >= 0;

  MERGE INTO `project.sales.STG_CUSTOMER_SALES` tgt
  USING (
    SELECT DISTINCT
      c.CUST_ID AS CUSTOMER_ID,
      c.CUST_REF_CD AS CUSTOMER_CODE,
      c.FIRST_NM AS FIRST_NAME,
      c.LAST_NM AS LAST_NAME,
      LOWER(c.EMAIL_ADDR) AS EMAIL,
      c.MOBILE_PHONE AS PHONE,
      c.HOME_REGION_CD AS REGION_CODE,
      IFNULL(lp.TIER_CD, 'STANDARD') AS LOYALTY_TIER,
      lp.LIFETIME_SPEND AS LIFETIME_VALUE,
      c.REG_DATE AS REGISTRATION_DATE,
      MAX(DATE(t.TXN_DATETIME)) OVER (PARTITION BY c.CUST_ID) AS LAST_PURCHASE_DATE,
      load_date AS LOAD_DATE
    FROM `project.source_ops.CUSTOMER` c
    JOIN `project.source_ops.SALES_TXN` t
      ON t.CUST_ID = c.CUST_ID
     AND DATE(t.TXN_DATETIME) = load_date
     AND t.STORE_REGION_CD = region_code
    LEFT JOIN `project.source_ops.LOYALTY_PROFILE` lp
      ON lp.CUST_ID = c.CUST_ID
  ) src
  ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
  WHEN MATCHED THEN UPDATE SET
    LOYALTY_TIER = src.LOYALTY_TIER,
    LIFETIME_VALUE = src.LIFETIME_VALUE,
    LAST_PURCHASE_DATE = src.LAST_PURCHASE_DATE,
    LOAD_DATE = src.LOAD_DATE
  WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, CUSTOMER_CODE, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
    REGION_CODE, LOYALTY_TIER, LIFETIME_VALUE, REGISTRATION_DATE,
    LAST_PURCHASE_DATE, LOAD_DATE
  ) VALUES (
    src.CUSTOMER_ID, src.CUSTOMER_CODE, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE,
    src.REGION_CODE, src.LOYALTY_TIER, src.LIFETIME_VALUE, src.REGISTRATION_DATE,
    src.LAST_PURCHASE_DATE, src.LOAD_DATE
  );
END;
```

2) Sales rollup

```sql
CREATE OR REPLACE PROCEDURE `project.sales.sp_sales_rollup`(
  load_date DATE,
  region_code STRING
)
BEGIN
  INSERT INTO `project.sales.FACT_REGIONAL_SUMMARY` (
    SUMMARY_KEY, REGION_CODE, SUMMARY_DATE, TOTAL_TRANSACTIONS, TOTAL_QUANTITY,
    TOTAL_REVENUE, AVG_BASKET_SIZE, DISTINCT_CUSTOMERS, DISTINCT_PRODUCTS,
    LOAD_DATE, LOAD_BATCH_ID
  )
  SELECT
    FARM_FINGERPRINT(CONCAT(region_code, CAST(load_date AS STRING))) AS SUMMARY_KEY,
    region_code,
    load_date,
    COUNT(*),
    SUM(quantity),
    SUM(net_amount),
    AVG(net_amount),
    COUNT(DISTINCT dim_customer_key),
    COUNT(DISTINCT dim_product_key),
    CURRENT_DATE(),
    CAST(FORMAT_DATE('%Y%m%d', load_date) AS INT64)
  FROM `project.sales.FACT_DAILY_SALES`
  WHERE transaction_date = load_date
    AND region_code = region_code
  GROUP BY region_code;
END;
```

3) Sales SCD historization and master load

```sql
CREATE OR REPLACE PROCEDURE `project.sales.sp_load_dim_product`(
  load_date DATE,
  batch_id INT64,
  OUT rows_loaded INT64
)
BEGIN
  MERGE `project.sales.DIM_PRODUCT` tgt
  USING (
    SELECT * FROM `project.sales.STG_PRODUCT_MASTER`
    WHERE LOAD_DATE = load_date
      AND IS_ACTIVE = 'Y'
  ) src
  ON tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt.IS_CURRENT = 'Y'
  WHEN MATCHED AND (
    IFNULL(tgt.PRODUCT_NAME, 'X') != IFNULL(src.PRODUCT_NAME, 'X')
    OR IFNULL(tgt.CATEGORY_CODE, 'X') != IFNULL(src.CATEGORY_CODE, 'X')
    OR IFNULL(tgt.LIST_PRICE, 0) != IFNULL(src.LIST_PRICE, 0)
    OR IFNULL(tgt.COST_PRICE, 0) != IFNULL(src.COST_PRICE, 0)
    OR IFNULL(tgt.SUPPLIER_ID, 0) != IFNULL(src.SUPPLIER_ID, 0)
  ) THEN
    UPDATE SET
      VALID_TO = DATE_SUB(load_date, INTERVAL 1 DAY),
      IS_CURRENT = 'N',
      UPDATED_BY = 'ETL_PROCESS',
      UPDATED_DATE = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (...) VALUES (...);
END;
```

4) Finance GL extract and transform

```sql
CREATE OR REPLACE PROCEDURE `project.finance.sp_gl_extract`(
  period_name STRING,
  entity_code STRING,
  period_date DATE
)
BEGIN
  DELETE FROM `project.finance.STG_GL_TRANSACTIONS`
  WHERE PERIOD_NAME = period_name
    AND ENTITY_CODE = entity_code
    AND ETL_STATUS = 'PENDING';

  INSERT INTO `project.finance.STG_GL_TRANSACTIONS` (...)
  SELECT
    g.JNL_LINE_ID,
    le.ENTITY_SHORT_CODE,
    g.LEDGER_ID,
    period_name,
    EXTRACT(YEAR FROM period_date),
    EXTRACT(MONTH FROM period_date),
    g.ACCOUNT_SEGMENT,
    g.CC_SEGMENT,
    IF(g.DR_CR_FLAG = 'D', ABS(g.AMOUNT), 0),
    IF(g.DR_CR_FLAG = 'C', ABS(g.AMOUNT), 0),
    IF(g.DR_CR_FLAG = 'D', ABS(g.AMOUNT), -ABS(g.AMOUNT)),
    IFNULL(g.TXN_CURRENCY, 'GBP'),
    IFNULL(r.EXCHANGE_RATE, 1),
    IF(g.TXN_CURRENCY = 'GBP', ABS(g.AMOUNT), ABS(g.AMOUNT) * IFNULL(r.EXCHANGE_RATE, 1)),
    g.TXN_DATE,
    g.JOURNAL_CATEGORY,
    g.JOURNAL_SOURCE_NAME,
    SUBSTR(g.DESCRIPTION, 1, 240),
    g.CREATED_BY,
    g.CREATION_DATE,
    period_date,
    'PENDING'
  FROM `project.source_fin.GL_JNL_LINES` g
  JOIN `project.source_fin.GL_LEDGERS` l ON l.LEDGER_ID = g.LEDGER_ID
  JOIN `project.source_fin.LEGAL_ENTITIES` le ON le.LEGAL_ENTITY_ID = l.LEGAL_ENTITY_ID
  LEFT JOIN `project.finance.STG_PERIOD_RATES` r
    ON r.FROM_CURRENCY = IFNULL(g.TXN_CURRENCY, 'GBP')
   AND r.TO_CURRENCY = 'GBP'
   AND r.PERIOD_NAME = period_name
   AND r.RATE_TYPE = 'AVERAGE'
  WHERE g.PERIOD_NAME = period_name
    AND g.STATUS = 'POSTED'
    AND g.AMOUNT <> 0;
END;
```

5) Finance reconciliation

```sql
CREATE OR REPLACE PROCEDURE `project.finance.sp_close_period`(
  period_name STRING,
  entity_code STRING,
  force_close STRING
)
BEGIN
  -- build balances
  INSERT INTO `project.finance.FACT_GL_BALANCES` (...)
  SELECT ...
  FROM `project.finance.STG_GL_TRANSACTIONS` g
  JOIN `project.finance.DIM_ACCOUNT` da ...
  LEFT JOIN `project.finance.STG_PERIOD_RATES` r ...
  WHERE g.PERIOD_NAME = period_name
    AND g.ENTITY_CODE = entity_code
    AND g.ETL_STATUS = 'PENDING'
  GROUP BY ...;

  -- reconcile
  INSERT INTO `project.finance.FACT_PERIOD_RECONCILIATION` (...)
  SELECT
    ...
  FROM `project.finance.FACT_GL_BALANCES` gl
  LEFT JOIN `project.finance.SOURCE_FIN_AR_ACCOUNT_BALANCES` sl
    ON sl.ACCOUNT_CODE = gl.ACCOUNT_CODE
   AND sl.ENTITY_CODE = gl.ENTITY_CODE
   AND sl.PERIOD_NAME = period_name;

  UPDATE `project.finance.DIM_PERIOD`
  SET IS_CLOSED = 'Y',
      CLOSE_DATE = CURRENT_TIMESTAMP()
  WHERE PERIOD_NAME = period_name;
END;
```

6) Customer scoring and segmentation

```sql
CREATE OR REPLACE PROCEDURE `project.customer.sp_customer_scoring`(
  run_date DATE,
  segment STRING,
  region STRING,
  model_version STRING
)
BEGIN
  INSERT INTO `project.customer.FACT_CUSTOMER_SCORES` (...)
  SELECT
    GENERATE_UUID(),
    dc.customer_id,
    dc.dim_crm_customer_key,
    run_date,
    ROUND(LEAST(IFNULL(cs.lifetime_value, 0) / 1000.0, 10.0), 4),
    ROUND(CASE
      WHEN cs.last_purchase_date IS NULL THEN 0.95
      WHEN DATE_DIFF(CURRENT_DATE(), cs.last_purchase_date, DAY) > 180 THEN 0.80
      WHEN DATE_DIFF(CURRENT_DATE(), cs.last_purchase_date, DAY) > 90 THEN 0.50
      WHEN DATE_DIFF(CURRENT_DATE(), cs.last_purchase_date, DAY) > 30 THEN 0.20
      ELSE 0.05
    END, 4),
    ROUND(IFNULL(ev.conversion_rate, 0), 4),
    ROUND(CASE
      WHEN IFNULL(ix.interaction_count, 0) > 10 THEN 1.0
      WHEN IFNULL(ix.interaction_count, 0) > 5 THEN 0.75
      WHEN IFNULL(ix.interaction_count, 0) > 0 THEN 0.25
      ELSE 0.0
    END, 4),
    dc.customer_segment,
    CASE dc.customer_segment
      WHEN 'VIP' THEN 'High-Value Premium Customer'
      WHEN 'RETAIL' THEN 'Standard Retail Customer'
      WHEN 'WHOLESALE' THEN 'B2B Wholesale Customer'
      ELSE 'Unclassified'
    END,
    CURRENT_TIMESTAMP(),
    model_version,
    CAST(FORMAT_DATE('%Y%m%d', run_date) AS INT64)
  FROM `project.customer.DIM_CUSTOMER_CRM` dc
  LEFT JOIN `project.sales.STG_CUSTOMER_SALES` cs
    ON cs.customer_id = dc.customer_id
  LEFT JOIN (
    SELECT customer_id,
           COUNTIF(event_type = 'CONVERTED') / COUNT(*) AS conversion_rate
    FROM `project.customer.STG_CAMPAIGN_EVENTS`
    WHERE event_date >= DATE_SUB(run_date, INTERVAL 90 DAY)
    GROUP BY customer_id
  ) ev ON ev.customer_id = dc.customer_id
  LEFT JOIN (
    SELECT customer_id, COUNT(*) AS interaction_count
    FROM `project.customer.STG_CUSTOMER_INTERACTIONS`
    WHERE interaction_date >= DATE_SUB(run_date, INTERVAL 30 DAY)
    GROUP BY customer_id
  ) ix ON ix.customer_id = dc.customer_id
  WHERE dc.is_current = 'Y';
END;
```

7) Data quality checks

```sql
CREATE OR REPLACE PROCEDURE `project.sales.sp_retail_dq`(
  load_date DATE
)
BEGIN
  INSERT INTO `project.audit.DQ_RESULTS`
  SELECT
    CURRENT_TIMESTAMP(),
    load_date,
    'NULL_PRODUCT_KEY',
    COUNT(*),
    COUNTIF(dim_product_key IS NULL),
    SAFE_DIVIDE(COUNTIF(dim_product_key IS NULL), COUNT(*)) * 100,
    0,
    IF(COUNTIF(dim_product_key IS NULL) = 0, 'PASS', 'FAIL'),
    'CRITICAL'
  FROM `project.sales.FACT_DAILY_SALES`
  WHERE transaction_date = load_date;
END;
```

8) Orchestration wrapper for unsupported logic
- Keep in Python only:
  - event wait polling
  - retry/backoff
  - notification
  - file existence checks
- Python pseudocode:

```python
def wait_for_event(event_name, event_value):
    # poll Cloud Storage marker or Pub/Sub message
    pass

def retry_command(fn, max_retries=3):
    # exponential backoff wrapper
    pass

def log_job_audit(job_name, run_date, status, rows_processed):
    # insert into BigQuery audit table
    pass
```

Configuration files required for BigQuery execution

1. Environment/config files
- env_retail.properties
- Equivalent BigQuery config file, for example:
  - bq_env.properties or YAML/JSON config for dataset names, project IDs, region, service account, notification targets.

2. Workflow/orchestration config
- UC4 workflows are not used in BigQuery directly.
- Replace with:
  - Cloud Composer DAG config
  - or Cloud Workflows YAML
  - or Cloud Scheduler job definitions

3. Secret/config management
- Secret Manager entries for:
  - service account impersonation, if needed
  - external notification credentials, if any
- No plaintext DB passwords should remain.

4. SQL deployment files
- BigQuery SQL scripts for:
  - sales
  - finance
  - customer
- BigQuery stored procedure DDL files
- Optional UDF files for reusable scoring logic

5. Optional Python orchestration files
- If keeping unsupported logic in Python:
  - requirements.txt
  - main.py / runner.py
  - config.json or config.yaml
  - service account auth config via environment, not file-based credentials

Python Pseudocode (if applicable)

```python
def main():
    load_config()
    wait_for_upstream_events()
    run_bigquery_script("sales_extract")
    run_bigquery_procedure("sales_master_load")
    run_bigquery_script("finance_gl_extract")
    run_bigquery_procedure("finance_close_period")
    run_bigquery_procedure("customer_master_load")
    run_bigquery_script("customer_scoring")
    run_bigquery_dq()
    write_audit()
```

Summary of key logic and data flow

- Sales:
  - source transactions -> staging -> product/customer dimensions -> fact sales -> regional summary -> DQ.
- Finance:
  - source GL -> staging -> account/cost centre dimensions -> GL balances -> reconciliation -> close event.
- Customer:
  - staged profiles/events/interactions + retail spend + finance reconciliation -> scores -> segments -> summaries.
- Cross-domain:
  - sales and finance publish completion events consumed by customer.
  - customer reads shared sales and finance tables.

If you want, I can next produce a concrete BigQuery migration package structure:
- dataset-by-dataset table DDL
- stored procedure DDL
- scheduled script order
- and a single end-to-end BigQuery orchestration plan.