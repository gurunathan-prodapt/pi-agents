# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh

## 1. Purpose & Scope
This migration targets an existing KornShell (KSH) script (`k_ausd_v_ta_barrier_zusgf.ksh`) and its invoked Oracle SQL script (`d_ausd_v_ta_barrier_zusgf.sql`). The KSH script acts as a control script, handling environment setup, parameter parsing, error management, and the execution of the SQL script. The primary business purpose of the SQL script is to process barrier-related data from the `sof$ta_barrier` table, aggregate and concatenate specific attributes based on `cntrct_id`, and then insert the summarized results into the `sof$ta_barrier_zusgf` table. The job also involves reading a timestamp from `isbert_schema.dwtk_meldungen` and truncating the target table using a utility script. The overall goal is to migrate this data processing pipeline to Google Cloud Platform, specifically utilizing BigQuery for data storage and transformation.

## 2. Source Inventory
The job is composed of two primary files:

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh**
    *   **Technology**: KornShell (ksh)
    *   **Category**: shell
    *   **Tool**: KornShell
    *   **Summary**: A control script responsible for managing job execution, calling an SQL script for data processing, and handling job entries.
    *   **Purpose**: ETL Orchestration, Data Processing Trigger
    *   **Complexity Tier**: (Not available)
    *   **Migration Bucket**: (Not available)

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_barrier_zusgf.sql**
    *   **Technology**: Oracle SQL (with PL/SQL package)
    *   **Purpose**: Data Transformation, Aggregation, Table Load
    *   **Summary**: This SQL script defines a pipelined table function to aggregate barrier data and then truncates and repopulates `sof$ta_barrier_zusgf` from `sof$ta_barrier`.
    *   **Complexity Tier**: (Not available)
    *   **Migration Bucket**: (Not available)

## 3. Target Architecture
The migrated solution will primarily reside in BigQuery.

*   **BigQuery Stored Procedure**: A BigQuery stored procedure will encapsulate the entire logic of the KSH and SQL scripts. This procedure will handle parameter validation, data aggregation, and loading into the target table.
*   **BigQuery Tables**:
    *   `project.dataset.sof_ta_barrier`: This table will be the BigQuery equivalent of the source Oracle `sof$ta_barrier` table.
    *   `project.dataset.sof_ta_barrier_zusgf`: This will be the BigQuery equivalent of the target Oracle `sof$ta_barrier_zusgf` table. It will be the output table of the stored procedure.
    *   `project.dataset.dwtk_meldungen`: BigQuery equivalent of `isbert_schema.dwtk_meldungen`.
    *   `project.dataset.execution_log` (Optional): A new logging table to capture execution metadata (e.g., record counts, job identifiers), replacing the KSH script's temporary file output and job entry management.
*   **Orchestration**: The BigQuery stored procedure will be invoked by a scheduling mechanism such as Cloud Composer (Airflow DAG), Cloud Workflows, or a Scheduled Query in BigQuery, replacing the existing KSH script's role as a job controller.

## 4. Data Flow & Lineage
The original job involves the following data flow:

1.  The KSH script `k_ausd_v_ta_barrier_zusgf.ksh` is executed, receiving `Jobkennung` and `EintragsNr` parameters.
2.  The KSH script reads the maximum `timecreated` from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'` to determine a `v_datum`.
3.  The KSH script invokes the Oracle SQL script `d_ausd_v_ta_barrier_zusgf.sql` via a SQL*Plus wrapper (likely `h_alis_sqlplus.ksh`).
4.  The Oracle SQL script defines a PL/SQL package `sof$sp_table_functions` containing `concat_barriers` pipelined table function.
5.  The Oracle SQL script then truncates the `sof$ta_barrier_zusgf` table using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
6.  It populates `sof$ta_barrier_zusgf` by reading `DISTINCT` records from `sof$ta_barrier`.
7.  The `concat_barriers` function aggregates `sperrart`, `sperrgrund`, and `stilllegungszeitraum_alle` by `cntrct_id`, concatenating values into single strings, and derives `sperrgrund_zusgf`.
8.  After the SQL execution, the KSH script reads the record count from a temporary file.

In BigQuery, the data flow will be:

1.  A BigQuery Stored Procedure, named `project.dataset.k_ausd_v_ta_barrier_zusgf`, is invoked with `p_JobKennung` and `p_EintragsNr` as parameters.
2.  The procedure validates the input parameters.
3.  It retrieves the `v_datum` value by querying the BigQuery table `project.dataset.dwtk_meldungen`.
4.  The procedure truncates the target BigQuery table `project.dataset.sof_ta_barrier_zusgf`.
5.  It performs the data transformation and aggregation by selecting from `project.dataset.sof_ta_barrier` using SQL `WITH` clauses and BigQuery functions like `STRING_AGG`, `ARRAY_AGG`, `CASE`, and `FORMAT_DATE`.
6.  The transformed data is inserted into `project.dataset.sof_ta_barrier_zusgf`.
7.  The count of inserted records is determined and optionally logged into `project.dataset.execution_log`.

## 5. Transformation Logic
The core transformation logic resides in the Oracle SQL script's `concat_barriers` pipelined table function. This will be re-implemented in BigQuery SQL within the stored procedure.

**Original Logic (Oracle SQL)**:
The `concat_barriers` function processes a `CURSOR` of distinct records from `sof$ta_barrier`. For each `cntrct_id`, it iterates through related records to:
*   Concatenate `sperrart` values (after removing 'Rufnummern' and spaces) into `sperrart_alle`, separated by commas, up to 500 characters.
*   Concatenate `sperrgrund` values into `sperrgrund_alle`, separated by commas, up to 500 characters.
*   Concatenate `stilllegungszeitraum_alle` (derived from `ist_stillegung`, `sperr_beginn`, `sperr_ende`) into `stilllegungszeitraum_alle`, separated by ', ', up to 100 characters.
*   Derive `sperrgrund_zusgf`: If `barrier_reason_cv` is 2, it's 2; otherwise, it's 3. Within the aggregation, if any record for a `cntrct_id` has `sperrgrund_zusgf != 2`, the final aggregated `sperrgrund_zusgf` becomes 3.

**Target Logic (BigQuery SQL)**:
The transformation will use `ARRAY_AGG` to group values for concatenation and `STRING_AGG` for the actual string concatenation.

```sql
  -- ... (parameter validation and v_datum retrieval)

  TRUNCATE TABLE `project.dataset.sof_ta_barrier_zusgf`;

  INSERT INTO `project.dataset.sof_ta_barrier_zusgf`
  (
    cntrct_id,
    sperrart_alle,
    sperrgrund_alle,
    stilllegungszeitraum_alle,
    sperrgrund_zusgf
  )
  WITH src AS (
    SELECT DISTINCT
      cntrct_id,
      REPLACE(REPLACE(sperrart, 'Rufnummern', ''), ' ', '') AS sperrart,
      sperrgrund,
      CASE
        WHEN ist_stillegung = 1 THEN
          CASE
            WHEN sperr_ende IS NULL THEN CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)))
            ELSE CONCAT(
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)),
              ' - ',
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_ende))
            )
          END
        ELSE NULL
      END AS stilllegungszeitraum_alle,
      CASE
        WHEN barrier_reason_cv = 2 THEN 2
        ELSE 3
      END AS sperrgrund_zusgf
    FROM `project.dataset.sof_ta_barrier`
  ),
  grp AS (
    SELECT
      cntrct_id,
      ARRAY_AGG(sperrart ORDER BY sperrart) AS arr_sperrart, -- Maintain order for consistent STRING_AGG
      ARRAY_AGG(sperrgrund ORDER BY sperrart) AS arr_sperrgrund,
      ARRAY_AGG(stilllegungszeitraum_alle ORDER BY sperrart) AS arr_stilllegung,
      ARRAY_AGG(sperrgrund_zusgf ORDER BY sperrart) AS arr_zusgf
    FROM src
    GROUP BY cntrct_id
  )
  SELECT
    cntrct_id,
    (
      SELECT STRING_AGG(x, ',' ORDER BY off)\n      FROM UNNEST(arr_sperrart) AS x WITH OFFSET off
      -- Apply substring to ensure max length, if necessary (BigQuery string_agg has no length limit)
    ) AS sperrart_alle,
    (
      SELECT STRING_AGG(x, ',' ORDER BY off)\n      FROM UNNEST(arr_sperrgrund) AS x WITH OFFSET off
      WHERE x IS NOT NULL
      -- Apply substring to ensure max length, if necessary
    ) AS sperrgrund_alle,
    (
      SELECT STRING_AGG(x, ', ' ORDER BY off)\n      FROM UNNEST(arr_stilllegung) AS x WITH OFFSET off
      WHERE x IS NOT NULL
      -- Apply substring to ensure max length, if necessary
    ) AS stilllegungszeitraum_alle,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM UNNEST(arr_zusgf) AS x
        WHERE x != 2
      ) THEN 3
      ELSE 2
    END AS sperrgrund_zusgf
  FROM grp;
```

## 6. External Dependencies
*   **Oracle `isbert_schema.dwtk_meldungen`**: This table is read to determine a date parameter. In BigQuery, this will directly query the migrated `project.dataset.dwtk_meldungen` table.
*   **Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: This is used to truncate the target table. In BigQuery, this will be replaced by a direct `TRUNCATE TABLE project.dataset.sof_ta_barrier_zusgf;` statement within the stored procedure.
*   **DB-Link (`@pcrs1`)**: The Oracle SQL uses `DEFINE v_carmen = "@pcrs1"`, indicating a potential database link to a Carmen DB. If `sof$ta_barrier` (and `isbert_schema.dwtk_meldungen`) are sourced from this remote Carmen DB via the link, this dependency needs to be addressed.
    *   **Replacement**:
        *   **Option 1 (Preferred)**: Ingest data from the Carmen DB into BigQuery (`sof_ta_barrier`, `dwtk_meldungen`) as part of a broader data ingestion strategy (e.g., using federated queries from Cloud SQL if Carmen is Oracle, or setting up a batch/streaming ingestion pipeline). The BigQuery stored procedure will then query these local BigQuery tables.
        *   **Option 2 (Less Preferred for production)**: If real-time access is critical and data volume is small, consider BigQuery federated queries to a Cloud SQL instance that has a connection to the Carmen DB.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_v_ta_barrier_zusgf_$$.tmp`)**: Used to store and pass the record count from the SQL script back to the KSH script. This will be replaced by:
    *   A BigQuery stored procedure `OUT` parameter.
    *   An `INSERT` statement into an `execution_log` table in BigQuery.
*   **Sourced Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: The functionalities of these scripts (error handling, date utilities, parameter parsing, SQL execution wrapper) will be absorbed and re-implemented using BigQuery scripting features (`DECLARE`, `SET`, `IF`, `RAISE`), standard BigQuery SQL functions, and the overall orchestration logic (e.g., Cloud Composer for error alerting).

## 7. Unresolved / Risks
*   **Exact Oracle `PIPELINED` Function Behavior**: While `STRING_AGG` and `ARRAY_AGG` provide similar functionality, the exact row-by-row state machine and ordering within the Oracle pipelined function might have subtle differences if not precisely replicated. Careful testing is required. The BigQuery solution uses `ORDER BY` within `ARRAY_AGG` and `STRING_AGG` to maintain consistent ordering.
*   **String Length Limits**: The original Oracle SQL logic has explicit `LENGTH` checks for concatenated strings (e.g., `<= 500`). While BigQuery `STRING_AGG` does not have an inherent length limit, the target schema for `sperrart_alle`, `sperrgrund_alle`, and `stilllegungszeitraum_alle` columns in BigQuery should be defined appropriately (e.g., `STRING` type) and potentially truncated if a specific length constraint is a functional requirement.
*   **Error Handling Granularity**: The KSH script's `WHENEVER SQLERROR` and custom error messaging need to be mapped to BigQuery's error handling within stored procedures (`RAISE`) and the orchestration layer's logging and alerting.
*   **`v_carmen` DB-Link Details**: The nature of the Carmen DB and the tables accessed through the DB link (`sof$ta_barrier`, `isbert_schema.dwtk_meldungen`) needs to be fully understood to determine the best ingestion or federated query strategy.

## 8. Build Plan
1.  **Migrate Source Data**:
    *   Migrate `sof$ta_barrier` from Oracle to `project.dataset.sof_ta_barrier` in BigQuery.
    *   Migrate `isbert_schema.dwtk_meldungen` from Oracle to `project.dataset.dwtk_meldungen` in BigQuery.
    *   Establish a data ingestion pipeline (e.g., batch ETL, CDC) for these source tables if they are continuously updated in Oracle.
2.  **Create Target Tables**:
    *   Create the target table `project.dataset.sof_ta_barrier_zusgf` in BigQuery, defining appropriate schema for the aggregated columns (e.g., `STRING` for concatenated fields, `INT64` for `cntrct_id` and `sperrgrund_zusgf`).
    *   (Optional) Create `project.dataset.execution_log` table for logging.
3.  **Develop BigQuery Stored Procedure**:
    *   Implement the BigQuery SQL stored procedure `project.dataset.k_ausd_v_ta_barrier_zusgf` as per the pseudocode provided in the MCP output, incorporating parameter validation, date derivation, table truncation, and the core aggregation logic.
    *   The `DEFINE v_carmen` is not needed.
    *   The PL/SQL package `sof$sp_table_functions` logic will be translated into BigQuery SQL using `WITH` clauses and aggregation functions.
4.  **Testing**:
    *   **Unit Tests**: Test the BigQuery stored procedure with various input parameters and data scenarios to ensure correct data transformation and aggregation logic, matching the legacy system's output.
    *   **Integration Tests**: Verify the end-to-end process, including data ingestion into BigQuery and the execution of the stored procedure.
    *   **Performance Tests**: Benchmark the BigQuery solution against the legacy system for performance and cost efficiency.
5.  **Orchestration**:
    *   Create an Airflow DAG (using Cloud Composer) or BigQuery Scheduled Query to trigger the BigQuery stored procedure.
    *   Configure parameters (`p_JobKennung`, `p_EintragsNr`) to be passed to the stored procedure.
    *   Implement error handling and alerting mechanisms within the orchestration layer.
6.  **Deployment**:
    *   Deploy the BigQuery tables and stored procedure to the target BigQuery environment.
    *   Deploy the orchestration mechanism.
7.  **Decommissioning (Post-migration)**: Retire the original KSH script and Oracle SQL components once the BigQuery solution is validated and stable.