# Migration Design — CRM_ABINITIO_TRANSFORM

## 1. Purpose & Scope

The `CRM_ABINITIO_TRANSFORM` job is a critical ETL workflow responsible for processing and transforming customer and general ledger data. It consists of two primary Ab Initio graphs:

*   **`customer_transform.xfr`**: This graph enriches customer profiles by integrating sales and campaign interaction data. It computes various customer lifetime value (CLV) and churn risk scores, segments customers, and aggregates these metrics for reporting. The main business purpose is to provide insights into customer behavior and identify at-risk customers for targeted interventions.
*   **`gl_transform.xfr`**: This graph processes raw General Ledger (GL) transactions, joining them with account dimension data, normalizing fields, and rolling up balances to produce period-end financial summaries. Its business purpose is to support financial reporting and analysis.

The scope of this migration is to re-platform these `very_complex` Ab Initio ETL jobs, which are currently operating within a `redesign` migration bucket, to Google Cloud Platform (GCP). This will leverage BigQuery for data warehousing and PySpark (running on Dataproc Serverless) for the complex transformation logic. Legacy Oracle databases and local filesystem outputs will be replaced with BigQuery tables and Google Cloud Storage (GCS).

## 2. Source Inventory

The job comprises two primary Ab Initio graph files, both identified as `very_complex` and requiring `redesign` for migration:

| File Name                    | Technology  | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| :--------------------------- | :---------- | :-------------- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `customer/customer_transform.xfr` | Ab Initio   | very_complex    | redesign          | This Ab Initio graph transforms staged customer data by joining profiles (`STG_CUSTOMER_PROFILE`), retail spend (`DW_OWNER.STG_CUSTOMER_SALES`), and campaign events (`STG_CAMPAIGN_EVENTS`). It computes customer scores (CLV, churn risk, engagement, propensity, composite) and segment aggregates. Outputs to `FACT_CUSTOMER_SCORES`, `FACT_CUSTOMER_SEGMENT_SUMMARY` (Oracle tables), and `crm_at_risk_${RUN_DATE}.dat` (flat file). Uses dynamic variables like `${RUN_DATE}` and `${ORA_CONNECT_STRING}`. |
| `finance/gl_transform.xfr`   | Ab Initio   | very_complex    | redesign          | This Ab Initio graph transforms raw GL staging records (`STG_GL_TRANSACTIONS`) into period balance summaries. It involves joining GL transactions with account dimensions (`DIM_ACCOUNT`), normalizing financial fields, categorizing journal types, and rolling up aggregate balances. Outputs to `FACT_GL_BALANCES` (Oracle table) and `finance_unmatched_gl_${ENTITY_CODE}_${PERIOD_NAME}.dat` (flat file). Uses dynamic variables like `${PERIOD_NAME}`, `${ENTITY_CODE}`, and `${ORA_CONNECT_STRING}`. |

## 3. Target Architecture

The target architecture for `CRM_ABINITIO_TRANSFORM` on GCP will utilize a combination of managed services:

*   **Data Ingestion (from Oracle to BigQuery Staging):**
    *   **Cloud Data Fusion / Datastream**: For replicating Oracle source tables (`STG_CUSTOMER_PROFILE`, `DW_OWNER.STG_CUSTOMER_SALES`, `STG_CAMPAIGN_EVENTS`, `STG_GL_TRANSACTIONS`, `DIM_ACCOUNT`, `STG_PERIOD_RATES`) into BigQuery staging datasets. This will ensure that source data is available in BigQuery for transformations.
    *   **Google Cloud Storage (GCS)**: For intermediate files or for storing `write_file` outputs.

*   **Transformation Layer (Ab Initio -> PySpark):**
    *   **Dataproc Serverless**: PySpark jobs will be developed to replicate the transformation logic of the Ab Initio graphs. Dataproc Serverless is preferred for its fully managed, autoscaling capabilities, reducing operational overhead.
    *   **BigQuery**: Used as the primary data lakehouse for all staging, intermediate, and final transformed data.

*   **Orchestration:**
    *   **Cloud Composer (Apache Airflow)**: Airflow DAGs will orchestrate the end-to-end workflow, including triggering data ingestion jobs, submitting PySpark jobs to Dataproc Serverless, monitoring job status, handling dependencies, and managing parameters (`RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`).

*   **Data Warehousing (Final Output):**
    *   **BigQuery Datasets**: Dedicated BigQuery datasets (`crm_warehouse`, `finance_warehouse`) will store the final `FACT_CUSTOMER_SCORES`, `FACT_CUSTOMER_SEGMENT_SUMMARY`, and `FACT_GL_BALANCES` tables.
    *   **Google Cloud Storage (GCS)**: The output flat files (e.g., `crm_at_risk_*.dat`, `finance_unmatched_gl_*.dat`) will be written to GCS buckets.

## 4. Data Flow & Lineage

The overall data flow for the `CRM_ABINITIO_TRANSFORM` job in the target BigQuery/PySpark architecture will be:

**A. `customer_transform_pyspark.py` (Replacing `customer/customer_transform.xfr`)**

1.  **Inputs**:
    *   BigQuery `stg_crm.stg_customer_profile` (from Oracle `STG_CUSTOMER_PROFILE`)
    *   BigQuery `stg_crm.stg_customer_sales` (from Oracle `DW_OWNER.STG_CUSTOMER_SALES`)
    *   BigQuery `stg_crm.stg_campaign_events` (from Oracle `STG_CAMPAIGN_EVENTS`)
2.  **Transformations (PySpark on Dataproc Serverless)**:
    *   `read_stg_customers` -> PySpark DataFrame for customer profiles.
    *   `read_retail_spend` -> PySpark DataFrame for retail spend.
    *   `read_campaign_events` -> PySpark DataFrame for campaign events.
    *   `rollup_campaign_by_customer` -> Aggregates campaign events by customer.
    *   `join_retail_spend` -> Left joins customer profiles with retail spend.
    *   `join_campaign_data` -> Left joins enriched customer data with campaign aggregates.
    *   `reformat_score_fields` -> Computes CLV, churn, engagement, propensity, composite scores, and customer segments.
    *   `filter_churn_segments` -> Splits scored customers into "normal" and "at-risk" streams.
    *   `rollup_segment_summary` -> Aggregates scored customer data by segment and region.
3.  **Outputs**:
    *   BigQuery `crm_warehouse.fact_customer_scores` (from `reformat_score_fields`)
    *   GCS `gs://crm_alerts/crm_at_risk_${RUN_DATE}.csv` (from `filter_churn_segments` - at-risk stream)
    *   BigQuery `crm_warehouse.fact_customer_segment_summary` (from `rollup_segment_summary`)

**B. `gl_transform_pyspark.py` (Replacing `finance/gl_transform.xfr`)**

1.  **Inputs**:
    *   BigQuery `stg_finance.stg_gl_transactions` (from Oracle `STG_GL_TRANSACTIONS`)
    *   BigQuery `stg_finance.dim_account` (from Oracle `DIM_ACCOUNT`)
    *   BigQuery `stg_finance.stg_period_rates` (from Oracle `STG_PERIOD_RATES`)
2.  **Transformations (PySpark on Dataproc Serverless)**:
    *   `read_stg_gl` -> PySpark DataFrame for GL transactions.
    *   `read_dim_account` -> PySpark DataFrame for account dimension.
    *   `read_exchange_rates` -> PySpark DataFrame for exchange rates.
    *   `reformat_normalise` -> Normalizes GL transaction fields (e.g., amounts, journal types).
    *   `join_account_dim` -> Left joins normalized GL data with account dimension. Outputs `joined_gl` and `unmatched_gl`.
    *   `filter_by_journal_type` -> Splits `joined_gl` into "standard" and "adjusting" journals.
    *   `rollup_period_balances` -> Aggregates standard journals to account/period level.
3.  **Outputs**:
    *   BigQuery `finance_warehouse.fact_gl_balances` (from `rollup_period_balances`)
    *   GCS `gs://finance_errors/finance_unmatched_gl_${ENTITY_CODE}_${PERIOD_NAME}.csv` (from `join_account_dim` - unmatched stream)

**Overall Orchestration:**

*   **Cloud Composer DAG `crm_abinitio_transform_dag.py`**:
    *   Task Group 1: Data Ingestion from Oracle to BigQuery Staging (e.g., using `DatastreamOperator` or `DataFusionStartPipelineOperator`).
    *   Task Group 2: Run `customer_transform_pyspark.py` on Dataproc Serverless (using `DataprocServerlessBatchOperator`).
    *   Task Group 3: Run `gl_transform_pyspark.py` on Dataproc Serverless (using `DataprocServerlessBatchOperator`).
    *   Dependencies will ensure ingestion completes before transformations start. The customer and GL transformation tasks can run in parallel if no inter-dependencies exist.

## 5. Transformation Logic

The core of the migration involves translating Ab Initio graph components into PySpark DataFrame operations.

**A. `customer_transform_pyspark.py` (derived from `customer/customer_transform.xfr`)**

*   **Input Readers**: Will use `spark.read.format("bigquery").option("table", ...).load()` to read from BigQuery staging tables (`stg_crm.stg_customer_profile`, `stg_crm.stg_customer_sales`, `stg_crm.stg_campaign_events`). Filtering conditions (e.g., `LOAD_DATE`, `ETL_STATUS`) will be applied using PySpark's `filter` clause.
*   **`rollup_campaign_by_customer`**: Translated to PySpark `groupBy("customer_id").agg(...)`. Conditional aggregation (e.g., `sum(when(col("event_type") == "CONVERTED", 1).otherwise(0))`) will handle counting specific event types.
*   **`join_retail_spend` & `join_campaign_data`**: Translated to PySpark `join(other_df, on="customer_id", how="left_outer")`. `NVL` equivalents will be `F.nvl(col("column"), default_value)`.
*   **`reformat_score_fields`**: This complex component will use a series of PySpark `withColumn` transformations. `CASE` statements will be translated using `when().otherwise()`. Numeric calculations will use standard PySpark functions (e.g., `F.lit`, `F.greatest`).
*   **`filter_churn_segments`**: Translated to PySpark `filter(col("churn_risk_score") < 0.70)` to create the "normal customers" DataFrame, and `filter(col("churn_risk_score") >= 0.70)` for "at-risk customers".
*   **`rollup_segment_summary`**: Translated to PySpark `groupBy("segment_code", "region_code").agg(...)`. Average calculations will be performed after summing, dividing by counts, and handling division by zero using `F.greatest(col("customer_count"), F.lit(1))`.
*   **Output Writers**: `FACT_CUSTOMER_SCORES` and `FACT_CUSTOMER_SEGMENT_SUMMARY` will be written to BigQuery using `df.write.format("bigquery").option("table", ...).mode("append").save()`. The `crm_at_risk` file will be written to GCS using `df.write.format("csv").option("delimiter", "|").save(gcs_path)`.

**B. `gl_transform_pyspark.py` (derived from `finance/gl_transform.xfr`)**

*   **Input Readers**: Will read from BigQuery staging tables (`stg_finance.stg_gl_transactions`, `stg_finance.dim_account`, `stg_finance.stg_period_rates`). Filtering will be based on parameters like `PERIOD_NAME` and `ENTITY_CODE`.
*   **`reformat_normalise`**: Translated using `withColumn` for operations like `upper(trim())`, `abs()`, `NVL()`, and `when().otherwise()` for `journal_type` categorization.
*   **`join_account_dim`**: Translated using PySpark `join` with `how="left_outer"` on composite keys (`account_code`, `entity_code`). Unmatched records (e.g., where `account_name` is `UNMAPPED`) will be identified and written separately.
*   **`filter_by_journal_type`**: Translated using PySpark `filter` to create DataFrames for "standard" and "adjusting" journals based on `journal_type` values.
*   **`rollup_period_balances`**: Translated to PySpark `groupBy` and `agg` to calculate sums for debits, credits, net amounts, and transaction counts. The `opening_balance` will need to be explicitly managed, possibly loaded from a previous period's closing balance if historization is required, otherwise defaulted to 0 for initial period. `closing_balance` will be calculated based on `opening_balance`, `period_debits`, and `period_credits`.
*   **Output Writers**: `FACT_GL_BALANCES` will be written to BigQuery. The `finance_unmatched_gl` file will be written to GCS.

## 6. External Dependencies

All current external dependencies on Oracle databases and local filesystems will be migrated to GCP services.

| Original Dependency           | Type       | Purpose                                        | GCP Replacement                                | How it's replaced                                                                 |
| :---------------------------- | :--------- | :--------------------------------------------- | :--------------------------------------------- | :-------------------------------------------------------------------------------- |
| Oracle Database               | Database   | Source tables (`STG_CUSTOMER_PROFILE`, `DW_OWNER.STG_CUSTOMER_SALES`, `STG_CAMPAIGN_EVENTS`, `STG_GL_TRANSACTIONS`, `DIM_ACCOUNT`, `STG_PERIOD_RATES`) and Target tables (`FACT_CUSTOMER_SCORES`, `FACT_CUSTOMER_SEGMENT_SUMMARY`, `FACT_GL_BALANCES`) | BigQuery                                       | Data ingested to BigQuery staging datasets, and transformed data written to BigQuery warehouse datasets. |
| Local Filesystem `/opt/etl/alerts/` | File Store | Output for `crm_at_risk_*.dat`               | GCS `gs://crm_alerts/`                         | PySpark job writes directly to a GCS bucket.                                      |
| Local Filesystem `/opt/etl/errors/` | File Store | Output for `finance_unmatched_gl_*.dat`      | GCS `gs://finance_errors/`                     | PySpark job writes directly to a GCS bucket.                                      |
| Ab Initio Environment         | Runtime    | Execution platform for `.xfr` graphs           | Dataproc Serverless (PySpark runtime)          | PySpark scripts execute complex transformations on a fully managed Spark environment. |
| `${ORA_CONNECT_STRING}`       | Parameter  | Oracle DB connection string                    | BigQuery connection details (implicitly via PySpark BigQuery connector) | Authenticated via GCP service accounts.                                           |
| `${RUN_DATE}`                 | Parameter  | Runtime date for filtering/naming              | Airflow DAG parameter (`ds` or custom)         | Passed as a command-line argument to PySpark jobs.                                |
| `${PERIOD_NAME}`, `${ENTITY_CODE}` | Parameters | GL-specific runtime parameters                 | Airflow DAG parameters                         | Passed as command-line arguments to PySpark jobs.                                 |

## 7. Unresolved / Risks

*   **Ab Initio DML Complexity**: The `reformat` components in Ab Initio can contain highly customized DML (Data Manipulation Language) with specific functions not directly available in PySpark. Each such transformation will require careful manual translation and validation to ensure functional equivalence.
*   **Performance Tuning**: While PySpark is powerful, the `very_complex` nature of the jobs implies that direct translation might not yield optimal performance in BigQuery or Dataproc Serverless. Significant performance tuning (e.g., partitioning, clustering, caching, join strategies) will be required during testing.
*   **Error Handling and Rejection Logic**: Ab Initio graphs often have sophisticated error ports and rejection handling. This needs to be explicitly designed and implemented in PySpark, potentially leveraging Cloud Logging for error capture and GCS for storing rejected records.
*   **Data Type Mismatches**: Potential data type discrepancies between Oracle, Ab Initio's DML, and BigQuery schemas will require careful mapping and casting in PySpark to prevent data integrity issues.
*   **Parameterization Management**: Robust management of run-time parameters (`RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`) from Airflow to PySpark scripts needs to be thoroughly tested, including default values and validation.
*   **Cross-System Orchestration**: The interaction between data ingestion (Cloud Data Fusion/Datastream) and transformation (PySpark on Dataproc Serverless) needs seamless orchestration within Cloud Composer, particularly for ensuring data freshness and consistency.
*   **Cost Optimization**: Dataproc Serverless charges based on usage. Careful resource allocation and job optimization will be necessary to control costs, especially for `very_complex` and potentially long-running jobs.

## 8. Build Plan

1.  **Define BigQuery Schemas (DDL)**
    *   Create `stg_crm`, `stg_finance`, `crm_warehouse`, `finance_warehouse` BigQuery datasets.
    *   Create BigQuery table DDLs for all source staging tables (`stg_customer_profile`, `stg_customer_sales`, `stg_campaign_events`, `stg_gl_transactions`, `dim_account`, `stg_period_rates`) and target fact/summary tables (`fact_customer_scores`, `fact_customer_segment_summary`, `fact_gl_balances`).
    *   *Language: BigQuery DDL*

2.  **Implement Data Ingestion from Oracle to BigQuery**
    *   Configure and deploy Cloud Data Fusion pipelines or Datastream configurations for real-time or batch replication of the required Oracle source tables into their corresponding BigQuery staging tables.
    *   *Language: Cloud Data Fusion / Datastream Configuration*

3.  **Develop PySpark Transformation Scripts**
    *   **`customer_transform_pyspark.py`**:
        *   Initialize Spark session and read parameters (`run_date`).
        *   Read data from BigQuery `stg_crm` tables.
        *   Implement PySpark DataFrame transformations matching `customer/customer_transform.xfr` (rollup, joins, reformat, filter).
        *   Write `fact_customer_scores` and `fact_customer_segment_summary` to BigQuery `crm_warehouse`.
        *   Write `crm_at_risk_*.csv` to GCS `gs://crm_alerts`.
    *   **`gl_transform_pyspark.py`**:
        *   Initialize Spark session and read parameters (`period_name`, `entity_code`).
        *   Read data from BigQuery `stg_finance` tables.
        *   Implement PySpark DataFrame transformations matching `finance/gl_transform.xfr` (reformat, join, filter, rollup).
        *   Write `fact_gl_balances` to BigQuery `finance_warehouse`.
        *   Write `finance_unmatched_gl_*.csv` to GCS `gs://finance_errors`.
    *   *Language: Python with PySpark (using `pyspark.sql.functions` for transformations)*

4.  **Create GCS Buckets**
    *   Create `gs://crm_alerts` and `gs://finance_errors` buckets in GCS.
    *   *Language: GCP CLI / Console*

5.  **Develop Cloud Composer (Airflow) DAG**
    *   Create `crm_abinitio_transform_dag.py`.
    *   Define tasks to trigger the data ingestion (if not always-on).
    *   Define `DataprocServerlessBatchOperator` tasks for `customer_transform_pyspark.py` and `gl_transform_pyspark.py`, passing dynamic parameters.
    *   Establish task dependencies and scheduling.
    *   Include robust error handling, retries, and logging to Cloud Logging.
    *   *Language: Python for Airflow DAGs*

6.  **Testing and Validation**
    *   **Unit Tests**: For individual PySpark transformation functions.
    *   **Integration Tests**: Run PySpark jobs with sample data in BigQuery staging.
    *   **End-to-End Tests**: Execute the full Airflow DAG, comparing BigQuery and GCS outputs with existing Ab Initio job outputs for functional equivalence and data accuracy.
    *   **Performance Tests**: Profile and optimize PySpark job execution on Dataproc Serverless for efficiency and cost.

7.  **Deployment**
    *   Upload PySpark scripts to a GCS bucket designated for application code.
    *   Deploy `crm_abinitio_transform_dag.py` to the Cloud Composer environment.
    *   Configure necessary IAM roles and permissions for Dataproc Serverless, BigQuery, GCS, and Cloud Composer service accounts.
    *   *Language: GCP CLI / Console*