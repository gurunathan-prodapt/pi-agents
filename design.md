# Migration Design — ausd_bp_ta_ibcp_ccid

## 1. Purpose & Scope
The job `ausd_bp_ta_ibcp_ccid` is a financial data processing component. Based on the naming conventions, its business purpose is defined as follows:
*   **ausd**: Australian Dollar / Australia Subsidiary or Sales Domain.
*   **bp**: Business Partner or Business Process.
*   **ta**: Transaction Accounting or Trial Balance.
*   **ibcp**: Intercompany Business Cost Platform / Intercompany Billing Control Pool.
*   **ccid**: Cost Center Identifier / Code Combination Identifier (a standard database key in financial systems like Oracle General Ledger used to uniquely identify account combinations).

### Scope
This design document covers the migration of the legacy batch job `ausd_bp_ta_ibcp_ccid` into a modern, native BigQuery ELT pipeline. The job is responsible for aggregating, cleansing, and loading Cost Center and GL Code Combination IDs (CCIDs) related to Intercompany Business Billing and Transactions for Australian operations.

---

## 2. Source Inventory
No physical legacy source code files were found in the pre-collected context directory. Based on the job's context and metadata, the estimated source environment is structured as follows:

| Legacy Object Name | Technology / Language | Complexity Tier | Inferred Purpose |
| :--- | :--- | :--- | :--- |
| `JOB:AUSD_BP_TA_IBCP_CCID` | Unix Shell (KSH) / PL-SQL | Tier 2 (Medium) | Ingests GL combinations, maps intercompany accounts, and updates regional CCID dimension. |
| `GL_CODE_COMBINATIONS` | Oracle Database Table | - | Source ERP table containing account segment definitions. |
| `IBCP_STAGE_TXN` | Oracle Database Table | - | Source stage table for Intercompany Business billing transactions. |

---

## 3. Target Architecture
The legacy pipeline will be replaced by a BigQuery-native architecture scheduled via **Google Cloud Composer (Apache Airflow)**.

```
┌─────────────────────────────────┐      ┌───────────────────────────────┐      ┌─────────────────────────────┐
│  Source ERP / Oracle Stage      │ ───> │ BigQuery Staging (Raw)        │ ───> │ BigQuery Core (Integration) │
│  (GL_CODE_COMBINATIONS, etc.)   │      │ bq_stage_ta.stg_oracle_ccid   │      │ bq_core_ta.dim_ccid_ibcp    │
└─────────────────────────────────┘      └───────────────────────────────┘      └─────────────────────────────┘
```

### BigQuery Target Datasets & Tables
1.  **Staging Dataset (`bq_stage_ta`)**:
    *   `stg_oracle_ccid`: Raw copy of the CCID configuration segments.
    *   `stg_ibcp_txns`: Raw copy of regional intercompany transaction billing data.
2.  **Core Dataset (`bq_core_ta`)**:
    *   `dim_ccid_ibcp`: Conformed Dimension table containing segmented accounting lines with business partner and cost-pool classifications.
    *   `fact_ibcp_ledger`: Reconciled intercompany transaction ledger.

---

## 4. Data Flow & Lineage
The step-by-step execution flow for the migrated job is as follows:

1.  **Extract & Ingest**:
    *   An Airflow DAG triggers an export from the source financial ledger.
    *   Data is loaded into Google Cloud Storage (GCS) as Parquet files.
    *   Parquet files are loaded into BigQuery staging tables (`bq_stage_ta.stg_oracle_ccid` and `bq_stage_ta.stg_ibcp_txns`).
2.  **Transform & Validate**:
    *   Validate that code combinations are structurally correct (e.g., segment counts match the Australian Chart of Accounts).
    *   Parse segments into meaningful dimensions: Entity, Cost Center, Account, Product, Intercompany (IC) Partner.
3.  **Load & Merge**:
    *   Perform a `MERGE` operation to update `bq_core_ta.dim_ccid_ibcp` with new code combinations and deactivate retired CCIDs.

---

## 5. Transformation Logic
Because the source files were unavailable, the following industry-standard logic for Oracle GL/CCID mapping to BigQuery SQL is established.

### Segment Concatenation and Mapping
The CCID is mapped by concatenating individual segment columns into a fully qualified ledger key, while filtering specifically for Australian Entities and Intercompany Account segments.

```sql
-- Target BigQuery SQL Transformation Logic
MERGE INTO `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp` T
USING (
  SELECT 
    ccid AS code_combination_id,
    segment1 AS entity_code,
    segment2 AS cost_center_code,
    segment3 AS account_code,
    segment4 AS sub_account_code,
    segment5 AS intercompany_partner_code,
    -- Concatenated GL Account string
    CONCAT(segment1, '-', segment2, '-', segment3, '-', COALESCE(segment4, '0000'), '-', COALESCE(segment5, '000')) AS full_gl_account_string,
    summary_flag,
    enabled_flag,
    CAST(start_date_active AS DATE) AS start_date,
    CAST(end_date_active AS DATE) AS end_date,
    CURRENT_TIMESTAMP() AS dw_last_update_ts
  FROM 
    `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid`
  WHERE 
    -- Filtering for Australian subsidiary code (e.g., 'AU' or '080' depending on COA structure)
    segment1 IN ('AU', '080')
) S
ON T.code_combination_id = S.code_combination_id
WHEN MATCHED THEN
  UPDATE SET 
    T.entity_code = S.entity_code,
    T.cost_center_code = S.cost_center_code,
    T.account_code = S.account_code,
    T.sub_account_code = S.sub_account_code,
    T.intercompany_partner_code = S.intercompany_partner_code,
    T.full_gl_account_string = S.full_gl_account_string,
    T.summary_flag = S.summary_flag,
    T.enabled_flag = S.enabled_flag,
    T.start_date = S.start_date,
    T.end_date = S.end_date,
    T.dw_last_update_ts = S.dw_last_update_ts
WHEN NOT MATCHED THEN
  INSERT (
    code_combination_id, 
    entity_code, 
    cost_center_code, 
    account_code, 
    sub_account_code, 
    intercompany_partner_code, 
    full_gl_account_string, 
    summary_flag, 
    enabled_flag, 
    start_date, 
    end_date, 
    dw_last_update_ts
  )
  VALUES (
    S.code_combination_id, 
    S.entity_code, 
    S.cost_center_code, 
    S.account_code, 
    S.sub_account_code, 
    S.intercompany_partner_code, 
    S.full_gl_account_string, 
    S.summary_flag, 
    S.enabled_flag, 
    S.start_date, 
    S.end_date, 
    S.dw_last_update_ts
  );
```

---

## 6. External Dependencies
*   **Upstream ERP Database**: Source transactional data originates from an on-premise ERP or Oracle Cloud Financials database.
    *   *Replacement*: An Cloud Data Fusion instance or self-hosted Airflow JDBC worker will run the daily batch extract to Cloud Storage.
*   **Control Scheduler**: Legacy scheduling (e.g., Control-M or UC4).
    *   *Replacement*: Google Cloud Composer (Airflow), running on a daily schedule aligned with upstream ledger closes.

---

## 7. Unresolved / Risks
1.  **Missing Source Verification (High Risk)**: Since the physical code was missing from the initial scan, the exact source column names (e.g., `segment1`, `segment2`) and the specific company codes mapping to Australian `ausd` operations must be validated against the active ERP system.
2.  **Chart of Accounts (COA) Alignment**: The number of segments (e.g., 5-segment vs. 10-segment COA) must be parameterized in the transformation pipeline to accommodate any future business reorganizations.
3.  **Data Reconciliation**: Because this involves financial reporting columns, exact validation scripts must be run during User Acceptance Testing (UAT) to ensure that the target BigQuery tables match legacy ledger balances exactly.

---

## 8. Build Plan
The following components must be built in sequential order:

1.  **DDL Deployment**:
    *   Deploy target schema definitions for staging table `stg_oracle_ccid` and conformed core table `dim_ccid_ibcp` in BigQuery.
2.  **Extract Pipeline**:
    *   Create a Cloud Composer (Airflow) operator task utilizing a JDBC connection to execute the export of active CCIDs into GCS.
3.  **SQL Transformation Script**:
    *   Deploy the BigQuery SQL MERGE script (modeled in Section 5) to a transformation folder in Cloud Composer.
4.  **Reconciliation Task**:
    *   Deploy an automated validation step comparing row counts and checksum hashes on key transaction values between the Oracle DB source and BigQuery target.