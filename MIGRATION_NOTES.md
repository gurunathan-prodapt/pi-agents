# MIGRATION_NOTES.md

## 1. Summary

The `CRM_ABINITIO_TRANSFORM` job, a critical ETL workflow comprising two `very_complex` Ab Initio graphs (`customer_transform.xfr` and `gl_transform.xfr`), has been migrated from a legacy Ab Initio environment to Google Cloud Platform (GCP). This migration falls under the `redesign` bucket due to the complexity of the original graphs.

The target platform leverages:
*   **BigQuery** for data warehousing (staging, intermediate, and final fact tables).
*   **PySpark** (running on **Dataproc Serverless**) for executing the complex transformation logic.
*   **Cloud Composer** (Apache Airflow) for end-to-end workflow orchestration.
*   **Google Cloud Storage (GCS)** for storing output files and PySpark application code.

Legacy Oracle databases and local filesystem outputs have been replaced with BigQuery tables and GCS buckets, respectively.

## 2. Generated artifacts

The following files were generated or are part of the migration solution:

*   **`pyspark/gl_transform_pyspark.py`**
    *   **Role**: This PySpark script replicates the transformation logic of the original `finance/gl_transform.xfr` Ab Initio graph. It reads GL transactions and account dimensions from BigQuery staging tables, normalizes data, performs joins, filters, and aggregates to produce period-end financial summaries. It writes the final `FACT_GL_BALANCES` to BigQuery and any unmatched GL records to a GCS error bucket.
*   **`pyspark/customer_transform_pyspark.py`** (Conceptual, based on design document)
    *   **Role**: This PySpark script (to be developed) will replicate the transformation logic of the original `customer/customer_transform.xfr` Ab Initio graph. It will enrich customer profiles by integrating sales and campaign interaction data, compute CLV and churn risk scores, segment customers, and aggregate metrics. It will write `FACT_CUSTOMER_SCORES` and `FACT_CUSTOMER_SEGMENT_SUMMARY` to BigQuery and at-risk customer data to a GCS alerts bucket.
*   **`airflow/dags/crm_abinitio_transform_dag.py`**
    *   **Role**: This Apache Airflow DAG orchestrates the entire `CRM_ABINITIO_TRANSFORM` workflow on GCP. It defines the sequence of tasks, including placeholder tasks for data ingestion from Oracle to BigQuery staging, and triggers the PySpark transformation jobs (`customer_transform_pyspark.py` and `gl_transform_pyspark.py`) on Dataproc Serverless. It manages dependencies and passes runtime parameters.

## 3. Key design decisions

*   **GCP as Target Platform**: Chosen for its fully managed services (BigQuery, Dataproc Serverless, Cloud Composer), offering scalability, high availability, reduced operational overhead, and seamless integration within the Google Cloud ecosystem.
*   **PySpark on Dataproc Serverless for Transformations**:
    *   **Why**: The original Ab Initio graphs were `very_complex` and required a `redesign`. PySpark provides a powerful, distributed processing framework capable of handling complex ETL logic and large datasets. Dataproc Serverless offers a fully managed, autoscaling Spark environment, eliminating the need for cluster management and optimizing resource utilization.
    *   **Trade-offs**: Requires a complete rewrite of Ab Initio DML logic into PySpark DataFrame operations, which can be time-consuming and requires deep understanding of both Ab Initio and PySpark paradigms. Performance tuning is crucial.
*   **BigQuery for Data Storage**:
    *   **Why**: Serves as the central data lakehouse for all data stages (staging, intermediate, and final fact tables). BigQuery's columnar storage, analytical capabilities, and serverless architecture are ideal for large-scale data warehousing and analytics.
    *   **Trade-offs**: Requires careful schema design and understanding of BigQuery's cost model.
*   **GCS for File Outputs**:
    *   **Why**: Provides highly durable, scalable, and cost-effective object storage for output files (e.g., `crm_at_risk_*.csv`, `finance_unmatched_gl_*.csv`) that were previously written to local filesystems. Seamlessly integrates with PySpark for direct write operations.
*   **Cloud Composer (Airflow) for Orchestration**:
    *   **Why**: Provides a robust, industry-standard platform for defining, scheduling, and monitoring complex data workflows. Its Python-based DAGs allow for flexible parameterization, dependency management, and integration with various GCP services.
    *   **Trade-offs**: Requires familiarity with Airflow concepts and Python for DAG development.
*   **Redesign Approach**: Given the `very_complex` nature of the Ab Initio graphs, a direct "lift and shift" or automated conversion was deemed impractical. A redesign and rewrite in PySpark allowed for leveraging cloud-native capabilities and optimizing the solution for GCP.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup**:
    *   Ensure a GCP project is active and billing is enabled.
    *   Replace `your-gcp-project-id` and `your-dataproc-code-bucket` placeholders in `airflow/dags/crm_abinitio_transform_dag.py` with actual values.

2.  **BigQuery Dataset Creation**:
    *   Create the following BigQuery datasets:
        *   `stg_crm`
        *   `stg_finance`
        *   `crm_warehouse`
        *   `finance_warehouse`
    *   **DDL Application**: Apply the necessary BigQuery DDLs for all source staging tables (`stg_customer_profile`, `stg_customer_sales`, `stg_campaign_events`, `stg_gl_transactions`, `dim_account`, `stg_period_rates`) and target fact/summary tables (`fact_customer_scores`, `fact_customer_segment_summary`, `fact_gl_balances`) within their respective datasets.

3.  **GCS Bucket Creation**:
    *   Create the following GCS buckets:
        *   `gs://crm_alerts` (for `crm_at_risk` outputs)
        *   `gs://finance_errors` (for `finance_unmatched_gl` outputs)
        *   `gs://your-dataproc-code-bucket` (or similar, for storing PySpark scripts and dependencies).

4.  **IAM & Permissions Configuration**:
    *   **Service Accounts**: Create or identify dedicated GCP service accounts for:
        *   **Cloud Composer**: Needs permissions to trigger Dataproc Serverless jobs, read/write to GCS, and interact with BigQuery.
        *   **Dataproc Serverless**: Needs permissions to read/write to BigQuery, read/write to GCS (including the code bucket and output buckets).
    *   **Roles**: Assign appropriate roles (e.g., `BigQuery Data Editor`, `Storage Object Admin`, `Dataproc Editor`, `Service Account User`) to these service accounts.

5.  **Data Ingestion Setup (Oracle to BigQuery Staging)**:
    *   **Cloud Data Fusion / Datastream Configuration**: Implement and configure the actual data ingestion pipelines using Cloud Data Fusion or Datastream to replicate the Oracle source tables (`STG_CUSTOMER_PROFILE`, `DW_OWNER.STG_CUSTOMER_SALES`, `STG_CAMPAIGN_EVENTS`, `STG_GL_TRANSACTIONS`, `DIM_ACCOUNT`, `STG_PERIOD_RATES`) into their corresponding BigQuery staging tables. The placeholder tasks in the Airflow DAG (`ingest_crm_staging_data`, `ingest_finance_staging_data`) will need to be replaced with actual operators or monitored externally.

6.  **PySpark Code Deployment**:
    *   Upload `pyspark/gl_transform_pyspark.py` (and `pyspark/customer_transform_pyspark.py` once developed) to the designated GCS bucket (e.g., `gs://your-dataproc-code-bucket/pyspark/`).
    *   Ensure the `spark-bigquery-with-dependencies` JAR is accessible to Dataproc Serverless (e.g., `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar` as referenced in the DAG).

7.  **Cloud Composer Environment Setup**:
    *   Deploy the `airflow/dags/crm_abinitio_transform_dag.py` to your Cloud Composer environment's DAGs folder.
    *   Configure any necessary Airflow Variables if parameters like `DEFAULT_GL_ENTITY_CODE` are to be managed dynamically.

8.  **Spark History Server (Optional)**:
    *   If a Spark History Server is desired for Dataproc Serverless job monitoring, ensure it's set up and the `dataprocCluster` path in the DAG is correctly configured.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up, including potential redesign (B4) items:

*   **`customer_transform_pyspark.py` Development**: The PySpark script for the customer transformation (`customer_transform_pyspark.py`) has been designed but not yet generated/implemented. This is a critical development task.
*   **Oracle Data Ingestion Implementation**: The Airflow DAG currently uses `DummyOperator` tasks for Oracle to BigQuery staging ingestion. The actual implementation using Cloud Data Fusion, Datastream, or another ingestion tool needs to be completed and integrated into the DAG.
*   **Ab Initio DML Translation Complexity**: The `very_complex` nature of the original Ab Initio DML (especially in `reformat` components) means that the PySpark translation required significant manual effort. Thorough validation is needed to ensure functional equivalence, and any remaining untranslated or simplified logic needs to be identified and addressed.
*   **Performance Tuning**: While PySpark on Dataproc Serverless offers scalability, the `very_complex` nature of the jobs implies that initial direct translations might not be optimally performant. Significant performance tuning (e.g., partitioning, clustering, caching, join strategies, resource allocation) will be required during testing and post-go-live.
*   **Error Handling and Rejection Logic**: Ab Initio graphs often have sophisticated error ports and rejection handling. While basic unmatched record handling is implemented for GL, a comprehensive error handling strategy for all transformation steps, including logging, alerting, and storing rejected records, needs to be explicitly designed and implemented in PySpark.
*   **Data Type Mismatches**: Potential data type discrepancies between Oracle, Ab Initio's DML, and BigQuery schemas were handled during development, but continued vigilance and careful mapping/casting in PySpark are necessary to prevent data integrity issues.
*   **GL `opening_balance` Historization**: The `gl_transform_pyspark.py` currently initializes `opening_balance` to `0.0` as per the observed Ab Initio XFR behavior. If the business requires `opening_balance` to be derived from the previous period's `closing_balance` for historical continuity, this logic needs to be added.
*   **GL Exchange Rate Application**: The original `gl_transform.xfr` did not explicitly apply exchange rates for currency conversion, and thus the PySpark script also omits this. If actual currency conversion is a business requirement, this is a functional gap that needs to be addressed.
*   **Parameterization Management**: Robust management of run-time parameters (`RUN_DATE`, `PERIOD_NAME`, `ENTITY_CODE`) from Airflow to PySpark scripts needs to be thoroughly tested, including default values, validation, and potential use of Airflow Variables for dynamic configuration.
*   **Cost Optimization**: Dataproc Serverless charges based on usage. Continuous monitoring and optimization of PySpark job configurations (e.g., driver/executor memory, cores, number of executors) will be necessary to control costs, especially for `very_complex` and potentially long-running jobs.

## 6. Validation

Validation of the migrated `CRM_ABINITIO_TRANSFORM` job involves several stages:

1.  **Unit Testing (PySpark)**:
    *   **How to run**: Individual PySpark transformation functions (e.g., `rollup_campaign_by_customer`, `reformat_normalise`) should have dedicated unit tests using `pyspark.sql.testing` or similar frameworks.
    *   **Passing means**: All unit tests pass, ensuring that individual transformation logic components produce the expected output for given input data.

2.  **Integration Testing (PySpark on Dataproc Serverless)**:
    *   **How to run**: Execute `gl_transform_pyspark.py` (and `customer_transform_pyspark.py` once developed) manually or via a dedicated Airflow DAG in a non-production GCP environment. Use representative sample data loaded into BigQuery staging tables.
    *   **Passing means**: PySpark jobs complete successfully without errors, and the output BigQuery tables (`fact_gl_balances`, `fact_customer_scores`, `fact_customer_segment_summary`) and GCS files (`finance_unmatched_gl_*.csv`, `crm_at_risk_*.csv`) contain data in the expected format and structure.

3.  **End-to-End Testing (Cloud Composer DAG)**:
    *   **How to run**: Trigger the `crm_abinitio_transform_dag.py` in a dedicated staging or UAT Cloud Composer environment. Ensure the data ingestion tasks (even if placeholders) are simulated or completed, and the PySpark jobs are triggered.
    *   **Passing means**:
        *   The Airflow DAG completes successfully without any failed tasks.
        *   The final output in BigQuery (`crm_warehouse.fact_customer_scores`, `crm_warehouse.fact_customer_segment_summary`, `finance_warehouse.fact_gl_balances`) and GCS (`gs://crm_alerts/crm_at_risk_*.csv`, `gs://finance_errors/finance_unmatched_gl_*.csv`) is functionally equivalent and data-accurate compared to the output of the legacy Ab Initio jobs for the same input data. This typically involves row counts, checksums, and detailed data comparisons.
        *   Job execution times are within acceptable Service Level Agreements (SLAs).

4.  **Performance Testing**:
    *   **How to run**: Execute the end-to-end DAG with production-scale data volumes in a performance testing environment. Monitor Dataproc Serverless resource utilization, BigQuery query costs, and overall job duration.
    *   **Passing means**: The job completes within defined performance SLAs and within acceptable cost thresholds.

## 7. Rollback procedure

In the event of critical failures or unacceptable performance post-go-live, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Pause/Disable New Workflow**: Immediately pause or disable the `crm_abinitio_transform_dag` in Cloud Composer to prevent further execution of the migrated jobs.
    *   **Notify Stakeholders**: Inform relevant business and technical teams about the rollback.

2.  **Revert to Legacy System**:
    *   **Re-enable Ab Initio Jobs**: Re-enable the original `customer_transform.xfr` and `gl_transform.xfr` Ab Initio jobs and their associated scheduling/orchestration in the legacy environment.
    *   **Verify Legacy Run**: Confirm that the legacy Ab Initio jobs execute successfully and produce expected outputs.

3.  **Data Consistency (if new data was written)**:
    *   **BigQuery Target Tables**: If the migrated jobs wrote data to `crm_warehouse.fact_customer_scores`, `crm_warehouse.fact_customer_segment_summary`, or `finance_warehouse.fact_gl_balances` before the rollback, a decision must be made:
        *   **Truncate/Delete**: If the data is deemed corrupted or incomplete, truncate/delete the affected partitions/tables in BigQuery.
        *   **Delta Load**: If the legacy system can handle incremental loads, ensure the next legacy run picks up from the correct point or re-processes the affected period.
    *   **GCS Output Files**: If new files were written to `gs://crm_alerts` or `gs://finance_errors`, these files should be reviewed and potentially archived or deleted to avoid confusion.

4.  **Analysis and Remediation**:
    *   **Root Cause Analysis**: Thoroughly investigate the cause of the failure or performance degradation in the migrated GCP environment.
    *   **Remediation Plan**: Develop a plan to address the identified issues, which may involve PySpark code fixes, BigQuery schema adjustments, IAM changes, or Dataproc Serverless configuration tuning.
    *   **Re-test**: Once remediation is complete, conduct comprehensive re-testing in a non-production environment before attempting another go-live.