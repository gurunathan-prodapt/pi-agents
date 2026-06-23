# Migration Design — BERT_V_TA_DISC_ZUSGF

## 1. Purpose & Scope
The purpose of this job, identified as `BERT_V_TA_DISC_ZUSGF`, is to process and concatenate discount descriptions related to contracts. Originating from an Oracle environment, the job uses a PL/SQL script to aggregate discount information, which involves custom object types and a pipelined table function, and then populates a target table (`sof$ta_disc_zusgf`). The execution is orchestrated by a UC4 job definition that invokes KornShell scripts for environment setup, parameter handling, and ultimately, the execution of the Oracle SQL transformation. The migration aims to re-implement this functionality on the Google Cloud Platform, utilizing BigQuery for data storage and transformation, and Airflow for workflow orchestration.

## 2. Source Inventory
This job comprises four primary components, each playing a distinct role in the overall workflow:

*   **vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml**
    *   **Technology:** UC4/Automic (XML Job Definition)
    *   **Summary:** Orchestrates the execution of a KornShell script for concatenating discount descriptions.
    *   **Migration Bucket:** semi_auto (Design for Airflow DAG generated)
*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh**
    *   **Technology:** KornShell (Shell Script)
    *   **Summary:** Wrapper script responsible for orchestrating the data reconciliation process, including parameter parsing, environment setup, logging, and error handling, before invoking the core control script.
    *   **Migration Bucket:** retire (Logic to be absorbed by Airflow DAG)
*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh**
    *   **Technology:** KornShell (Shell Script)
    *   **Summary:** Control script that manages job execution, handles parameters, and orchestrates the execution of the SQL script (`d_ausd_v_ta_disc_zusgf.sql`).
    *   **Migration Bucket:** semi_auto (Logic to be absorbed by Airflow DAG tasks)
*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql**
    *   **Technology:** Oracle PL/SQL (SQL Script)
    *   **Summary:** Defines custom object types, a PL/SQL package with a pipelined table function to concatenate discount information, and populates the `SOF$TA_DISC_ZUSGF` table.
    *   **Migration Bucket:** manual (Requires manual conversion effort for PL/SQL specific features)

## 3. Target Architecture
The migrated job will leverage the following Google Cloud Platform services:
*   **Google Cloud Storage (GCS):** To store the BigQuery SQL transformation scripts and potentially intermediate data.
*   **BigQuery:** As the primary data warehouse for storing source tables (`dwtk_meldungen`, `sof$ta_discount`) and the target table (`sof$ta_disc_zusgf`). BigQuery SQL will be used for all data transformation logic.
*   **Cloud Composer (Apache Airflow):** To orchestrate the end-to-end workflow, replacing the existing UC4 scheduler and KornShell scripts. This will involve:
    *   An Airflow DAG representing the job schedule and task dependencies.
    *   Python Operators for environment setup, parameter passing, and custom logic that cannot be directly translated to BigQuery SQL (e.g., dynamic date derivation, error handling).
    *   BigQuery Operators to execute the transformed BigQuery SQL statements.

## 4. Data Flow & Lineage
The original data flow is sequential:
1.  **UC4 Job (`DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`):** Triggers the wrapper KornShell script.
2.  **Wrapper KornShell (`r_ausd_v_ta_disc_zusgf.ksh`):** Sets up the environment, handles logging, parses parameters, and invokes the control KornShell script.
3.  **Control KornShell (`k_ausd_v_ta_disc_zusgf.ksh`):** Further manages execution, passes parameters, and executes the Oracle PL/SQL script.
4.  **Oracle PL/SQL (`d_ausd_v_ta_disc_zusgf.sql`):** Reads data from `isbert_schema.dwtk_meldungen` (to determine a processing date) and `sof$ta_discount` (discount data from Carmen DB via DB link). It defines and uses custom PL/SQL types and a pipelined table function to concatenate discount descriptions. Finally, it truncates and inserts the processed data into `sof$ta_disc_zusgf`.

In the target BigQuery/Airflow architecture, this flow will be transformed into an Airflow DAG:

*   **`dw_bert_ausd_v_ta_disc_zusgf` DAG:**
    *   **`start` task:** Initiates the DAG run.
    *   **`determine_processing_date` task (PythonOperator):** Replaces the logic in `d_ausd_v_ta_disc_zusgf.sql` that queries `dwtk_meldungen` for `timecreated`. This task will retrieve the `s_datum` (processing date) from the BigQuery equivalent of `dwtk_meldungen` and make it available as an XCom for subsequent tasks.
    *   **`execute_bq_transformation` task (BigQueryOperator):** Executes the converted BigQuery SQL for `d_ausd_v_ta_disc_zusgf.sql`. This task will consume the `s_datum` from the previous task if necessary. It will read from the BigQuery equivalent of `sof$ta_discount` and write to `sof$ta_disc_zusgf`.
    *   **`end` task:** Marks the successful completion of the DAG.

The shell script logic for environment setup, parameter parsing, and error handling (`r_ausd_v_ta_disc_zusgf.ksh` and `k_ausd_v_ta_disc_zusgf.ksh`) will be integrated directly into the Airflow DAG definition and PythonOperators, ensuring robust orchestration and observability.

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_v_ta_disc_zusgf.sql`, which concatenates discount descriptions. The original Oracle PL/SQL uses custom `OBJECT` and `TABLE OF OBJECT` types, along with a pipelined table function `concat_discounts` within a package `sof$sp_discount_functions`. This function groups and concatenates discount texts based on `cntrct_id` and `cntrct_obj_version`.

**Original Oracle PL/SQL (conceptual snippet):**
```sql
CREATE OR REPLACE PACKAGE sof$sp_discount_functions IS
  FUNCTION concat_discounts(...) RETURN sof$ty_t_discount PIPELINED PARALLEL_ENABLE(...);
END;
/
INSERT INTO sof$ta_disc_zusgf (...)
SELECT ...
  FROM (...) dzg
  JOIN TABLE(sof$sp_discount_functions.concat_discounts(CURSOR(...))) con
    ON dzg.cntrct_id = con.cntrct_id(+) ...;
```

**Target BigQuery SQL:**
The custom Oracle types and pipelined function will be replaced by standard BigQuery SQL features, primarily `STRING_AGG` within a `GROUP BY` clause. The `TRUNCATE TABLE` and `INSERT` will be replaced by `CREATE OR REPLACE TABLE AS SELECT`.

```sql
CREATE OR REPLACE TABLE `sof$ta_disc_zusgf` AS
WITH v_datum AS (
  -- Derives processing date from DWTK_MELDUNGEN (example, actual table name may vary)
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
  FROM `isbert_schema.dwtk_meldungen` m -- Assuming this table is migrated to BQ
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
),
discount_base AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
    disc_vector_ty
  FROM `sof$ta_discount` -- Assuming this table is migrated to BQ
),
discount_concat_source AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    CAST(cntrct_obj_version AS INT64) AS cntrct_obj_version,
    CONCAT(CAST(rabatt AS STRING), ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_text
  FROM `sof$ta_discount`
),
discount_agg AS (
  SELECT
    cntrct_id,
    cntrct_obj_version,
    STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
  FROM discount_concat_source
  GROUP BY cntrct_id, cntrct_obj_version
)
SELECT
  d.cntrct_id,
  d.cntrct_obj_version,
  d.disc_vector_ty,
  a.rabatt_alle
FROM discount_base d
LEFT JOIN discount_agg a
  ON d.cntrct_id = a.cntrct_id
 AND d.cntrct_obj_version = a.cntrct_obj_version;
```
**Data Type Mapping:**
*   `NUMBER(10)` will map to `INT64`.
*   `VARCHAR2(500)`/`VARCHAR2(4000)` will map to `STRING`.
*   `TO_CHAR(MAX(m.timecreated), 'YYYYMMDD')` will map to `FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated))`.
*   `NVL` will map to `COALESCE`.
*   The `(+)` outer join syntax will be replaced by `LEFT JOIN`.
*   The explicit `LENGTH(...) <= 500` constraint for `rabatt_alle` concatenation in Oracle will need to be re-evaluated for BigQuery. If a maximum length is critical, it can be enforced using `SUBSTRING` or `LEFT` functions.

## 6. External Dependencies
The original job has the following external dependencies:
*   **Oracle Database:** Source of data for `sof$ta_discount` and `dwtk_meldungen`, potentially accessed via a DB link (`@pcrs1`). This is a critical dependency that needs to be replaced by BigQuery tables.
*   **Host `DWHDWH1P` and Login `DW.UNIX.ISBERT`:** These are execution environment details from the UC4 job. In BigQuery/Airflow, this will translate to the Cloud Composer environment and BigQuery project/dataset where the job runs.
*   **Local file system (for shell scripts and logging):** The shell scripts source various utility scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and generate temporary files/logs. This will be replaced by:
    *   Airflow's native logging and XComs for inter-task communication.
    *   Python functions within Airflow Operators to handle similar environment setup and parameter management.
    *   Cloud Logging for job execution logs.

## 7. Unresolved / Risks
*   **Oracle PL/SQL Pipelined Function Complexity:** The `d_ausd_v_ta_disc_zusgf.sql` script uses a pipelined table function which is a complex Oracle-specific feature. While a BigQuery SQL equivalent using `STRING_AGG` has been designed, thorough testing is required to ensure functional parity, especially concerning edge cases in concatenation logic and the `ORDER BY` clause within `STRING_AGG`.
*   **Shell Script Logic Absorption (Parameter Handling & Error Logic):** The KornShell scripts (`r_ausd_v_ta_disc_zusgf.ksh`, `k_ausd_v_ta_disc_zusgf.ksh`) contain logic for parameter parsing (`getopts`), environment sourcing (`. $HOME/.dw_init`), error handling (`f_alis_msgerr.ksh`), and job control. This logic needs to be meticulously re-implemented using Python within the Airflow DAG, potentially using `PythonOperator` tasks or by integrating the logic directly into the BigQueryOperator's context. The `retire` migration bucket for `r_ausd_v_ta_disc_zusgf.ksh` suggests a full re-engineering of its role.
*   **`dwtk_meldungen` and `sof$ta_discount` Migration:** The successful migration of this job depends on the prior migration and availability of the `isbert_schema.dwtk_meldungen` and `sof$ta_discount` tables in BigQuery. The mechanism for populating these BigQuery tables needs to be confirmed.
*   **Dynamic `v_datum` Handling:** The `v_datum` variable, derived from `dwtk_meldungen`, is used for date-based filtering or partitioning. This dynamic date derivation needs to be correctly implemented in the Airflow DAG, likely as a Python task that passes the date via XComs to the BigQuery SQL.
*   **Performance Tuning:** The original script used `PARALLEL` hints. The BigQuery SQL should be reviewed for optimal performance, potentially using clustering, partitioning, and efficient join strategies.

## 8. Build Plan
1.  **Migrate Source Tables to BigQuery:** Ensure `isbert_schema.dwtk_meldungen` and `sof$ta_discount` are available in BigQuery with appropriate schema and data.
2.  **Develop BigQuery SQL Transformation:**
    *   Convert `d_ausd_v_ta_disc_zusgf.sql` into BigQuery SQL, implementing the `STRING_AGG` logic for discount concatenation.
    *   Test the BigQuery SQL transformation thoroughly with representative data.
3.  **Develop Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf.py`):**
    *   Define the DAG properties (schedule, start\_date, owner, etc.).
    *   Create a `PythonOperator` task (`determine_processing_date`) to fetch the processing date (`v_datum`) from the BigQuery `dwtk_meldungen` equivalent and pass it as an XCom.
    *   Create a `BigQueryOperator` task (`execute_bq_transformation`) to execute the BigQuery SQL transformation script. This task will utilize the `v_datum` XCom.
    *   Implement error handling, logging, and retry mechanisms within the DAG, replacing the KornShell script logic.
    *   Deploy the DAG to Cloud Composer.
4.  **Parameter Management:** Define how parameters (e.g., job IDs, entry numbers) will be passed into the Airflow DAG run, potentially using Airflow configurations or context variables.
5.  **Integration Testing:** Perform end-to-end testing of the Airflow DAG to verify data accuracy, performance, and operational stability in the BigQuery environment.