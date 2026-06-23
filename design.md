# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope

This migration design document details the conversion of an existing batch job, originating from a KornShell (KSH) wrapper script `r_ausd_v_ta_c_bfc.ksh`, to operate within the Google Cloud Platform, leveraging BigQuery for data processing. The primary purpose of this job is to calculate, refresh, and persist binding-period values for contracts, maintaining a "binding period cache" in the `ta_c_bfc` table.

The current system relies on a multi-step process: an initiating KSH wrapper, a controlling KSH script, and a core Oracle SQL script. The migration aims to translate this functionality into BigQuery SQL for data transformations and use GCP-native orchestration (e.g., Cloud Composer/Airflow) for workflow management, thereby eliminating dependencies on KornShell, Oracle SQL*Plus, and proprietary Oracle features like DB Links and PL/SQL packages.

## 2. Source Inventory

The job `6d73ee79` comprises three main components: two KornShell scripts and one Oracle SQL script. All files are categorized for "semi_auto" migration, indicating that some manual intervention or custom development will be required beyond automated conversion.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** (Not provided in metadata)
    *   **Automation Bucket:** semi_auto
    *   **Role:** This is the top-level wrapper script. It initializes the environment, manages logging and error handling through sourced utility scripts, defines basic job metadata (`ProgName`, `ProgVersion`), sets up `trap` handlers, and invokes the control script `k_ausd_v_ta_c_bfc.ksh` with specific job identifiers.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** (Not provided in metadata)
    *   **Automation Bucket:** semi_auto
    *   **Role:** This acts as the control script, invoked by `r_ausd_v_ta_c_bfc.ksh`. It further initializes the environment by sourcing utility scripts (including one for SQL*Plus routines), parses command-line parameters (`JobKennung`, `EintragsNr`), defines the target table name (`ta_c_bfc`), constructs the path to the Oracle SQL script (`d_ausd_v_ta_c_bfc.sql`), and executes it using a `starteSQLSkript` function. It also manages temporary files for record counts.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql`**
    *   **Technology:** Oracle SQL
    *   **Tier:** (Not provided in metadata)
    *   **Automation Bucket:** semi_auto
    *   **Role:** This is the core data processing script. It defines synonyms for remote Oracle objects via a DB Link, creates a PL/SQL function (`bfc_get_bindefrist`) for business logic calculation, and performs a multi-step data transformation process. This includes building a staging table (`sof$ta_c_bfc_akt`), conditionally inserting initial data into the main cache table (`sof$ta_c_bfc`), merging changes from the staging table, and updating stale entries based on procedure versioning. It uses Oracle-specific syntax and features like `ROWNUM`, `SPOOL`, and `WHENEVER SQLERROR`.

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform services to achieve scalability, managed infrastructure, and cost efficiency.

*   **Data Processing:** BigQuery will replace Oracle SQL for all data transformation logic.
*   **Orchestration:** Cloud Composer (managed Apache Airflow) is recommended for orchestrating the multi-step workflow. Alternatively, for simpler linear workflows, BigQuery Scheduled Queries or Cloud Workflows + Cloud Scheduler could be considered.
*   **Data Ingestion:** Oracle source data will be replicated to BigQuery using Datastream for Change Data Capture (CDC) or batch ingestion pipelines for periodic loads, depending on latency requirements.
*   **Logging & Monitoring:** Cloud Logging for centralized log collection and Cloud Monitoring for alerts and performance tracking.
*   **Secrets Management:** Sensitive information (e.g., database credentials) will be stored securely in Secret Manager.

**Target Table Structures:**

#### A. Target cache table: `ta_c_bfc`
```sql
CREATE TABLE IF NOT EXISTS `project.dataset.ta_c_bfc` (
  cntrct_id STRING NOT NULL,
  bindefrist DATE,
  bfc_age INT64,
  bfc_count INT64,
  bfc_procedure DATE,
  commitment_reference_date DATE,
  cntrct_validity_id STRING,
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

#### B. Staging table: `ta_c_bfc_akt`
```sql
CREATE TABLE IF NOT EXISTS `project.dataset.ta_c_bfc_akt` (\n  cntrct_id STRING NOT NULL,\n  bindefrist DATE,\n  bfc_age INT64,\n  bfc_count INT64,\n  bfc_procedure DATE,\n  commitment_reference_date DATE,\n  cntrct_validity_id STRING,\n  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()\n);
```
**Keys and Constraints (BigQuery considerations):**
*   `cntrct_id` will serve as the logical business key for cache rows.
*   BigQuery does not enforce primary/foreign keys. Data integrity will be maintained through the transformation logic.
*   Consider partitioning by `bfc_procedure` or `load_ts` and clustering by `cntrct_id` for query optimization.

## 4. Data Flow & Lineage

The job execution and data flow proceed sequentially:

1.  **`r_ausd_v_ta_c_bfc.ksh` (Wrapper):**
    *   Reads configuration from `$HOME/.dw_init`.
    *   Loads utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    *   Orchestrates the call to `k_ausd_v_ta_c_bfc.ksh`.
    *   Generates log output.

2.  **`k_ausd_v_ta_c_bfc.ksh` (Control Script):**
    *   Reads configuration from `$HOME/.dw_init`.
    *   Loads utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Passes `JobKennung` and `EintragsNr` to the SQL execution.
    *   Orchestrates the execution of `d_ausd_v_ta_c_bfc.sql`.
    *   Writes temporary record count data to `$DW_DIR_UTL/bert_k_ausd_v_ta_c_bfc_$$.tmp`.

3.  **`d_ausd_v_ta_c_bfc.sql` (Core SQL Processing):**
    *   **Inputs:**
        *   `isbert_schema.dwtk_meldungen` (Oracle table)
        *   `all_objects@pcrs1` (Oracle metadata table via DB Link)
        *   `spr_schema.cds$vr_Bindefrist@PCRS1` (Remote Oracle package via DB Link)
        *   `spr_schema.spr$pa_types@PCRS1` (Remote Oracle package via DB Link)
        *   `spr_schema.cds$ta_cntrct@PCRS1` (Remote Oracle table via DB Link)
        *   `sof$ta_cntrct_crs` (Oracle table)
        *   `sof$ta_barrier` (Oracle table)
        *   `sof$ta_cntrct_valid` (Oracle table)
        *   `sof$ta_period` (Oracle table)
        *   `sof$ta_c_bfc` (Oracle table - read for updates/merges)
        *   `sof$ta_c_bfc_akt` (Oracle table - read for merges)
    *   **Processes:**
        *   Defines Oracle synonyms and a PL/SQL function `bfc_get_bindefrist`.
        *   **Step 1:** Truncates `sof$ta_c_bfc_akt` and inserts aggregated data from source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`).
        *   **Step 2:** Performs an initial population of `sof$ta_c_bfc` from `sof$ta_c_bfc_akt` if `sof$ta_c_bfc` is empty.
        *   **Step 3:** Merges data from `sof$ta_c_bfc_akt` into `sof$ta_c_bfc`, updating existing rows based on changes in `bfc_age` or `bfc_count`, and inserting new contract IDs.
        *   **Step 4:** Updates `sof$ta_c_bfc` to recalculate binding periods for rows processed with older procedure versions, with a limit (`v_max_update`).
    *   **Outputs:**
        *   Updated `sof$ta_c_bfc` (Oracle table).
        *   Truncated `sof$ta_c_bfc_akt` (Oracle table).
        *   SQL*Plus spool output.

**Simplified Abstract Syntax Tree:**

```text
JobMigrationDocument
├── WrapperScript: r_ausd_v_ta_c_bfc.ksh
│   ├── SourceEnvironment
│   │   └── $HOME/.dw_init
│   ├── LoadUtilities
│   │   ├── f_alis_msgerr.ksh
│   │   ├── h_alis_parameter.ksh
│   │   └── h_alis_date.ksh
│   ├── DefineMetadata
│   │   ├── ProgName = "Bindefristcache"
│   │   └── ProgVersion = "V1.0.0"
│   ├── SetupTraps
│   ├── ParseParameters
│   ├── LogStart
│   └── InvokeControlScript
│       └── k_ausd_v_ta_c_bfc.ksh
│           ├── SourceEnvironment
│           │   └── $HOME/.dw_init
│           ├── LoadUtilities
│           │   ├── f_alis_msgerr.ksh
│           │   ├── h_alis_date.ksh
│           │   ├── h_alis_parameter.ksh
│           │   └── h_alis_sqlplus.ksh
│           ├── ParseParameters
│           │   ├── j = JobKennung
│           │   └── f = EintragsNr
│           ├── DefineTargetTable
│           │   └── v_TabName = "ta_c_bfc"
│           ├── DefineSQLScriptPath
│           │   └── d_ausd_v_ta_c_bfc.sql
│           ├── ExecuteSQLScript
│           └── ManageTempFile
│               └── bert_k_ausd_v_ta_c_bfc_$$.tmp
└── SQLScript: d_ausd_v_ta_c_bfc.sql
    ├── MetadataQueries
    │   ├── isbert_schema.dwtk_meldungen
    │   └── all_objects@pcrs1
    ├── CreateSynonyms
    │   ├── cds$vr_Bindefrist
    │   ├── spr$pa_types
    │   └── cds$ta_cntrct
    ├── CreateFunction
    │   └── bfc_get_bindefrist
    ├── Step1_BuildStaging
    │   └── sof$ta_c_bfc_akt
    ├── Step2_InitialLoad
    │   └── sof$ta_c_bfc
    ├── Step3_MergeChangedRows
    │   └── sof$ta_c_bfc
    ├── Step4_RecalculateStaleRows
    │   └── sof$ta_c_bfc
    ├── Cleanup
    │   └── Truncate sof$ta_c_bfc_akt
    └── CommitSequence
```

## 5. Transformation Logic

The core transformation logic resides within `d_ausd_v_ta_c_bfc.sql`, focusing on maintaining the `sof$ta_c_bfc` cache table. This logic will be translated to BigQuery SQL.

*   **Snapshot Building (Step 1):** The script aggregates contract-related data from `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, and multiple instances of `sof$ta_period` into a staging table `sof$ta_c_bfc_akt`. This aggregation involves `GROUP BY c.cntrct_id` and calculates `bfc_age` (maximum of several date fields) and `bfc_count`.
    *   **BigQuery Equivalent:** A BigQuery `CREATE TABLE AS SELECT` or a CTE (Common Table Expression) will generate the staging data.

*   **Initial Population (Step 2):** If `sof$ta_c_bfc` is empty, all records from `sof$ta_c_bfc_akt` are inserted.
    *   **BigQuery Equivalent:** An `INSERT INTO` statement conditional on checking the row count of the target table.

*   **Merge Logic (Step 3):** This is a crucial step that updates existing `sof$ta_c_bfc` records where the `bfc_age` has increased or `bfc_count` has changed in `sof$ta_c_bfc_akt`. New `cntrct_id`s found in `sof$ta_c_bfc_akt` but not in `sof$ta_c_bfc` are inserted. The `bindefrist` is recalculated using the `bfc_get_bindefrist` function.
    *   **BigQuery Equivalent:** BigQuery's `MERGE` statement is suitable here. The `bfc_get_bindefrist` function will need to be re-implemented as a BigQuery SQL UDF or potentially a BigQuery Stored Procedure, depending on its complexity and external dependencies.

*   **Stale Procedure Recalculation (Step 4):** Records in `sof$ta_c_bfc` where `bfc_procedure` is older than the current `v_bfc_procedure` are updated, also recalculating `bindefrist` using `bfc_get_bindefrist`. This step is throttled by `ROWNUM <= &v_max_update`.
    *   **BigQuery Equivalent:** An `UPDATE` statement with a `WHERE` clause for the date condition. The `ROWNUM` equivalent in BigQuery can be achieved using `QUALIFY ROW_NUMBER() OVER (...) <= v_max_update` within a subquery, or by breaking down the updates if `v_max_update` is a performance optimization rather than a strict business rule.

## 6. External Dependencies

The following external dependencies need to be addressed during migration:

*   **Oracle DB Link `@pcrs1`:** This link provides access to remote objects like `all_objects`, `spr_schema.cds$vr_Bindefrist`, `spr_schema.spr$pa_types`, and `spr_schema.cds$ta_cntrct`.
    *   **Replacement:** The data from the remote Oracle source that these objects represent must be replicated into BigQuery. Datastream can provide CDC for transactional tables, while batch exports and loads can handle slower-changing data or metadata. Once in BigQuery, these become standard BigQuery tables/views in a dedicated dataset.
*   **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** These are used for path resolution for sourced scripts and temporary file locations.
    *   **Replacement:** In Cloud Composer/Airflow, these will be replaced by Airflow Variables, XComs, or environment variables configured in the DAG execution environment. Cloud Storage buckets will replace local filesystem paths for temporary files.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `../trace.sql.cfg`):** These KSH scripts provide common functions for error handling, parameter parsing, date utilities, and SQL*Plus execution.
    *   **Replacement:** Error handling and logging will be replaced by Cloud Logging and Airflow's native error handling mechanisms. Parameter parsing will be handled by Airflow DAG parameters. SQL execution will be direct BigQuery SQL statements orchestrated by Airflow.
*   **Oracle Packages/Functions (`Cds$vr_Bindefrist.GetBindeFrist`, `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`):** These are proprietary Oracle functions/packages.
    *   **Replacement:** `Cds$vr_Bindefrist.GetBindeFrist` logic must be re-implemented in BigQuery SQL as a UDF or Stored Procedure. `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` (used for `TRUNCATE TABLE`) will be replaced by direct BigQuery DDL/DML statements.

## 7. Unresolved / Risks

*   **File Complexity (Unresolved):** `file_complexity` data was not available for any of the component files, making precise complexity assessment difficult.
*   **Oracle-Specific Syntax & Semantics:** Direct translation of Oracle PL/SQL, `ROWNUM`, `WHENEVER SQLERROR`, and date/null handling might introduce subtle behavioral differences in BigQuery.
*   **`bfc_get_bindefrist` Re-implementation:** The business logic encapsulated in the `Cds$vr_Bindefrist.GetBindeFrist` package (which `bfc_get_bindefrist` wraps) needs careful analysis and re-implementation in BigQuery. This is a critical risk and might require significant effort.
*   **DB Link Replacement:** Ensuring complete and accurate data replication from the `PCRS1` Oracle instance to BigQuery is crucial. Any missing data or latency issues could impact the cache's accuracy.
*   **Throttled Updates (`ROWNUM <= v_max_update`):** The exact reason for this throttling (e.g., resource management, avoiding long transactions) needs to be understood to implement an equivalent or improved strategy in BigQuery.
*   **Utility Script Logic:** The full extent of the logic within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` needs to be analyzed. While basic functions are evident, any complex error handling, logging, or job control logic would need to be re-implemented using GCP-native services.

## 8. Build Plan

The migration will follow a structured approach:

1.  **Data Ingestion Phase:**
    *   Establish Datastream connections or batch export/import pipelines to bring all Oracle source tables (e.g., `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `isbert_schema.dwtk_meldungen`, and remote tables accessed via `@pcrs1`) into BigQuery. Target language: Google Cloud Dataflow/Datastream configurations and BigQuery.

2.  **PL/SQL Function Re-implementation:**
    *   Analyze the Oracle `Cds$vr_Bindefrist.GetBindeFrist` package and `bfc_get_bindefrist` function.
    *   Re-implement the logic as a BigQuery SQL UDF or Stored Procedure. Target language: BigQuery SQL.

3.  **SQL Transformation Conversion:**
    *   Translate `d_ausd_v_ta_c_bfc.sql` into modular BigQuery SQL scripts.
    *   Each step (staging, initial load, merge, stale update, cleanup) should be a distinct BigQuery SQL script or a Dataform model.
    *   Target language: BigQuery SQL.

4.  **Orchestration Development:**
    *   Design and develop an Airflow DAG in Cloud Composer to orchestrate the BigQuery SQL steps.
    *   Replace shell-based environment initialization, parameter passing, logging, and error handling with Airflow features.
    *   Target language: Python (for Airflow DAG).

5.  **Monitoring & Alerting Setup:**
    *   Configure Cloud Monitoring alerts for BigQuery job failures and Airflow DAG failures.
    *   Ensure structured logging is enabled and ingested into Cloud Logging.
    *   Target language: YAML/JSON (for Cloud Monitoring/Logging configurations).

6.  **Testing & Validation:**
    *   Perform comprehensive unit, integration, and user acceptance testing to ensure data parity and functional equivalence with the legacy system.
    *   Target: BigQuery SQL, Python.