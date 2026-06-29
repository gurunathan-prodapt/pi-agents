# Migration Validation Test Suite: `ausd_bp_ta_bpr_basis`

This document defines the migration-validation test suite for the BigQuery/Airflow job `ausd_bp_ta_bpr_basis`. These tests are designed to prove behavioral equivalence between the legacy Oracle implementation and the migrated Google Cloud Platform (GCP) implementation.

---

## Test Case 1: Parameter Resolution & Temporal Boundary Filtering
### Purpose
Verify that the dynamic parameter `v_datum` is correctly resolved from `core_bert.dwtk_meldungen` and that the temporal filters (`insert_at`, `modified_at`, `valid_from`, `valid_to`) correctly include or exclude records at the exact boundary conditions.

### Setup
1. Clear the target and staging tables.
2. Insert a control execution date into `core_bert.dwtk_meldungen`:
   ```sql
   INSERT INTO `core_bert.dwtk_meldungen` (job_kennung, timecreated) 
   VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-20 14:30:00 UTC'));
   ```
   *This sets `v_datum_date` to `2026-04-20`.*
3. Insert mock records into `src_carmen.cds$ta_cntrct` to test temporal boundaries:
   * **Record A (Valid Active):** `insert_at = '2026-04-19'`, `modified_at = NULL`, `valid_from = '2026-04-19'`, `valid_to = '2026-04-21'` (Should be INCLUDED)
   * **Record B (Future Insert):** `insert_at = '2026-04-21'`, `modified_at = NULL`, `valid_from = '2026-04-19'`, `valid_to = NULL` (Should be EXCLUDED)
   * **Record C (Past Modified):** `insert_at = '2026-04-19'`, `modified_at = '2026-04-19'`, `valid_from = '2026-04-19'`, `valid_to = NULL` (Should be EXCLUDED)
   * **Record D (Future Valid From):** `insert_at = '2026-04-19'`, `modified_at = NULL`, `valid_from = '2026-04-21'`, `valid_to = NULL` (Should be EXCLUDED)
   * **Record E (Past Valid To):** `insert_at = '2026-04-19'`, `modified_at = NULL`, `valid_from = '2026-04-19'`, `valid_to = '2026-04-19'` (Should be EXCLUDED)
   * **Record F (Exact Boundary Active):** `insert_at = '2026-04-20'`, `modified_at = '2026-04-21'`, `valid_from = '2026-04-20'`, `valid_to = '2026-04-21'` (Should be INCLUDED)

4. Insert corresponding matching records into `src_carmen.pds$ta_bpri_com` with `bpr_id = 31` (tnv) and matching temporal parameters.

### Action
Execute the historical extract SQL script `d_ausd_bp_ta_bpr_basis_his.sql`.

### Pass/Fail Criterion
Query the target table `core_bert.sof$ta_bpr_basis_his` and assert that only **Record A** and **Record F** are present.

```sql
-- Assertion Query
WITH expected_results AS (
  SELECT 'Record A' AS label, 101 AS cntrct_id UNION ALL
  SELECT 'Record F' AS label, 106 AS cntrct_id
)
SELECT 
  e.label,
  e.cntrct_id,
  CASE WHEN t.cntrct_id IS NOT NULL THEN 'PASS' ELSE 'FAIL' END AS status
FROM expected_results e
LEFT JOIN `core_bert.sof$ta_bpr_basis_his` t ON e.cntrct_id = t.cntrct_id;
```
*All rows must return `PASS`. Total row count in `core_bert.sof$ta_bpr_basis_his` must be exactly 2.*

---

## Test Case 2: ICCID Concatenation & NULL Handling
### Purpose
In BigQuery, `CONCAT()` returns `NULL` if any of its arguments are `NULL`. In Oracle, the concatenation operator `||` treats `NULL` as an empty string. This test ensures that the ICCID concatenation logic does not produce unexpected `NULL` values if optional components of the ICCID are missing, or verifies that the behavior matches the legacy system's handling of partial ICCIDs.

### Setup
1. Insert a valid parameter record in `core_bert.dwtk_meldungen` (`v_datum_date = '2026-04-20'`).
2. Insert a contract record in `src_carmen.cds$ta_cntrct` that passes all filters.
3. Insert two records into `src_carmen.pds$ta_bpri_com`:
   * **Record 1 (Fully Populated ICCID):** `iccid_mi='89'`, `iccid_ii='49'`, `iccid_iai='01'`, `iccid_nr='123456789'`, `iccid_cd='0'`
   * **Record 2 (Partially Null ICCID):** `iccid_mi='89'`, `iccid_ii='49'`, `iccid_iai=NULL`, `iccid_nr='123456789'`, `iccid_cd=NULL`

### Action
Execute `d_ausd_bp_ta_bpr_basis_his.sql`.

### Pass/Fail Criterion
Verify how the migrated code handles the partial NULL. If the legacy system expected a concatenated string with empty segments (e.g., `89-49--123456789-`), the BigQuery code using standard `CONCAT` will output `NULL`. 

Run the following assertion to detect silent NULL generation:
```sql
SELECT 
  bpri_com_id,
  iccid,
  CASE 
    WHEN bpri_com_id = 2 AND iccid IS NULL THEN 'FAIL: BigQuery CONCAT returned NULL due to missing components'
    WHEN bpri_com_id = 2 AND iccid = '89-49--123456789-' THEN 'PASS: Handled empty strings correctly'
    ELSE 'PASS'
  END AS validation_status
FROM `core_bert.sof$ta_bpr_basis_his`;
```
*If any row returns `FAIL`, the migration team must refactor the `CONCAT` statement to use `CONCAT(COALESCE(mi, ''), '-', COALESCE(ii, ''), ...)` to preserve Oracle behavioral equivalence.*

---

## Test Case 3: Contract Type and Parent Filter Logic
### Purpose
Verify the complex conditional logic: `(c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)`.

### Setup
1. Set `v_datum_date = '2026-04-20'`.
2. Insert test records into `src_carmen.cds$ta_cntrct` with varying `cntrct_ty` and `cntrct_parent` values:
   * **Record 1:** `cntrct_ty = 3`, `cntrct_parent = NULL` (Should be INCLUDED)
   * **Record 2:** `cntrct_ty = 1`, `cntrct_parent = 9999` (Should be INCLUDED)
   * **Record 3:** `cntrct_ty = 1`, `cntrct_parent = NULL` (Should be EXCLUDED)
   * **Record 4:** `cntrct_ty = 5`, `cntrct_parent = NULL` (Should be EXCLUDED)
   * **Record 5:** `cntrct_ty = 5`, `cntrct_parent = 8888` (Should be INCLUDED)

3. Ensure all other filters (status, production, dates) are met for these records, and matching records exist in `pds$ta_bpri_com`.

### Action
Execute `d_ausd_bp_ta_bpr_basis_his.sql`.

### Pass/Fail Criterion
Query the target table to ensure only the correct contract IDs are present.

```sql
SELECT 
  cntrct_id,
  CASE 
    WHEN cntrct_id IN (1, 2, 5) THEN 'PASS'
    ELSE 'FAIL: Unexpected contract ID found'
  END AS status
FROM `core_bert.sof$ta_bpr_basis_his`;
```
*The query must return exactly 3 rows, all with status `PASS`.*

---

## Test Case 4: Analytical Window Function & Deduplication
### Purpose
Verify that the consolidation step correctly identifies the latest active contract instance using the analytical window function `MAX(COALESCE(bp1.valid_to, DATE '4712-12-31')) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id)` and filters out older historical versions.

### Setup
1. Populate `core_bert.sof$ta_bpr_basis_his` with multiple historical versions of the same contract and base product:
   * **Contract 1001, Product 2759 (Twin Card):**
     * Version A: `bpri_com_id = 101`, `valid_to = '2025-12-31'`
     * Version B: `bpri_com_id = 102`, `valid_to = '2026-04-19'`
     * Version C: `bpri_com_id = 103`, `valid_to = NULL` (Active, defaults to `4712-12-31`)
   * **Contract 1002, Product 3848 (MultiSIM):**
     * Version A: `bpri_com_id = 201`, `valid_to = '2026-01-01'`
     * Version B: `bpri_com_id = 202`, `valid_to = '2026-03-01'` (This is the max valid_to for this closed contract)

### Action
Execute `d_ausd_bp_ta_bpr_basis.sql`.

### Pass/Fail Criterion
Verify that only the records with the maximum `valid_to` date per `(cntrct_id, bpr_id)` are written to `core_bert.sof$ta_bpr_basis`.

```sql
-- Assertion Query
WITH expected AS (
  SELECT 1001 AS cntrct_id, 103 AS bpr_instance_id, DATE '4712-12-31' AS valid_to UNION ALL
  SELECT 1002 AS cntrct_id, 202 AS bpr_instance_id, DATE '2026-03-01' AS valid_to
)
SELECT 
  e.cntrct_id,
  CASE 
    WHEN t.bpr_instance_id = e.bpr_instance_id AND t.valid_to = e.valid_to THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM expected e
LEFT JOIN `core_bert.sof$ta_bpr_basis` t ON e.cntrct_id = t.cntrct_id;
```
*All rows must return `PASS`. Total row count in `core_bert.sof$ta_bpr_basis` must be exactly 2.*

---

## Test Case 5: Left Join with SIM Card Details
### Purpose
Verify that the left outer join to `sof$ta_sim` correctly enriches the base product records with `card_type_name` when a matching ICCID exists, and leaves it `NULL` (without dropping the record) when no matching ICCID is found.

### Setup
1. Populate `core_bert.sof$ta_bpr_basis_his` with two records:
   * **Record 1:** `cntrct_id = 5001`, `iccid = '89-49-01-000000001-0'`
   * **Record 2:** `cntrct_id = 5002`, `iccid = '89-49-01-000000002-0'`
2. Populate `src_carmen.rma$ta_sim` and `src_carmen.rma$ta_sim_card_type` such that only the ICCID for **Record 1** exists and resolves to `card_type_name = 'Nano SIM'`. No SIM record exists for **Record 2**.

### Action
Execute `d_ausd_bp_ta_bpr_basis.sql`.

### Pass/Fail Criterion
Verify the output in `core_bert.sof$ta_bpr_basis`.

```sql
SELECT 
  cntrct_id,
  iccid,
  card_type_name,
  CASE 
    WHEN cntrct_id = 5001 AND card_type_name = 'Nano SIM' THEN 'PASS'
    WHEN cntrct_id = 5002 AND card_type_name IS NULL THEN 'PASS'
    ELSE 'FAIL'
  END AS join_validation
FROM `core_bert.sof$ta_bpr_basis`
WHERE cntrct_id IN (5001, 5002);
```
*Both rows must return `PASS`.*

---

## Test Case 6: End-to-End Automated Parity Test (PyTest)
### Purpose
An automated Python test script using `pytest` to orchestrate the execution of the BigQuery scripts against a test dataset, comparing the output against a pre-calculated expected golden dataset (representing legacy Oracle output).

### Code Implementation (`test_migration_parity.py`)

```python
import os
import pytest
from google.cloud import bigquery

# Initialize BigQuery Client
# Assumes GOOGLE_APPLICATION_CREDENTIALS is set in the environment
client = bigquery.Client()

TEST_PROJECT = os.getenv("GCP_PROJECT", "test-gcp-project")
DATASET_CORE = f"{TEST_PROJECT}.core_bert"
DATASET_SRC = f"{TEST_PROJECT}.src_carmen"

@pytest.fixture(scope="module", autouse=True)
def setup_test_data():
    """Prepares the database state before running tests."""
    # 1. Clean up target tables
    client.query(f"TRUNCATE TABLE `{DATASET_CORE}.dwtk_meldungen`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_CORE}.sof$ta_bpr_basis_his`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_CORE}.sof$ta_sim`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_CORE}.sof$ta_bpr_basis`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_SRC}.cds$ta_cntrct`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_SRC}.pds$ta_bpri_com`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_SRC}.rma$ta_sim`").result()
    client.query(f"TRUNCATE TABLE `{DATASET_SRC}.rma$ta_sim_card_type`").result()

    # 2. Insert Control Date
    client.query(f"""
        INSERT INTO `{DATASET_CORE}.dwtk_meldungen` (job_kennung, timecreated) 
        VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-20 00:00:00 UTC'))
    """).result()

    # 3. Insert Golden Source Data (Contracts & Base Products)
    client.query(f"""
        INSERT INTO `{DATASET_SRC}.cds$ta_cntrct` 
        (cntrct_id, cntrct_st, redundant_owner_id, insert_at, modified_at, valid_from, valid_to, is_production, cntrct_ty, cntrct_parent)
        VALUES 
        (10001, 5, 1, '2026-04-15', NULL, '2026-04-15', NULL, 1, 3, NULL), -- Valid
        (10002, 6, 1, '2026-04-15', NULL, '2026-04-15', NULL, 1, 1, 999); -- Valid (Parent not null)
    """).result()

    client.query(f"""
        INSERT INTO `{DATASET_SRC}.pds$ta_bpri_com` 
        (cntrct_id, bpr_id, bpri_com_id, iccid_mi, iccid_ii, iccid_iai, iccid_nr, iccid_cd, imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, cntrct_id_ref, valid_from, valid_to, modified_at, insert_at, slave_number, eid, is_production)
        VALUES 
        (10001, 2759, 9001, '89', '49', '01', '111111111', '1', '262', '07', '12', '34', NULL, '2026-04-15', NULL, NULL, '2026-04-15', 1, 'E1', 1),
        (10002, 3848, 9002, '89', '49', '01', '222222222', '2', '262', '07', '12', '56', NULL, '2026-04-15', NULL, NULL, '2026-04-15', 2, 'E2', 1);
    """).result()

    # 4. Insert Golden Source Data (SIM Cards)
    client.query(f"""
        INSERT INTO `{DATASET_SRC}.rma$ta_sim` (iccid_mi, iccid_ii, iccid_iai, iccid_nr, iccid_cd, sim_card_type_id, insert_at, modified_at, valid_from, valid_to)
        VALUES ('89', '49', '01', '111111111', '1', 55, '2026-04-15', NULL, '2026-04-15', NULL);
    """).result()

    client.query(f"""
        INSERT INTO `{DATASET_SRC}.rma$ta_sim_card_type` (sim_card_type_id, card_type_name, insert_at, modified_at)
        VALUES (55, 'eSIM', '2026-04-15', NULL);
    """).result()

    yield

def read_sql_file(file_path):
    with open(file_path, "r") as f:
        return f.read()

def test_historical_extract_execution():
    """Executes Task 1 and asserts intermediate table state."""
    sql = read_sql_file("src/sql/d_ausd_bp_ta_bpr_basis_his.sql")
    # Replace hardcoded dataset names with environment-specific ones if necessary
    query_job = client.query(sql)
    query_job.result()  # Wait for execution

    # Assert row count in intermediate table
    query = f"SELECT COUNT(*) as cnt FROM `{DATASET_CORE}.sof$ta_bpr_basis_his`"
    result = list(client.query(query).result())[0]
    assert result.cnt == 2, f"Expected 2 historical records, got {result.cnt}"

def test_consolidation_execution():
    """Executes Task 2 & 3 and asserts final parity."""
    sql = read_sql_file("src/sql/d_ausd_bp_ta_bpr_basis.sql")
    query_job = client.query(sql)
    query_job.result()

    # Assert final table contents
    query = f"""
        SELECT cntrct_id, bpr_id, iccid, card_type_name 
        FROM `{DATASET_CORE}.sof$ta_bpr_basis` 
        ORDER BY cntrct_id
    """
    rows = list(client.query(query).result())
    
    assert len(rows) == 2
    
    # Record 1: Should have resolved eSIM card type
    assert rows[0].cntrct_id == 10001
    assert rows[0].bpr_id == 2759
    assert rows[0].iccid == "89-49-01-111111111-1"
    assert rows[0].card_type_name == "eSIM"

    # Record 2: Should have NULL card type (no matching SIM record)
    assert rows[1].cntrct_id == 10002
    assert rows[1].bpr_id == 3848
    assert rows[1].iccid == "89-49-01-222222222-2"
    assert rows[1].card_type_name is None
```

### Action
Run the test suite using pytest:
```bash
pytest test_migration_parity.py -v
```

### Pass/Fail Criterion
All assertions in the pytest suite must pass. Any failure in row count, string concatenation format, or outer join resolution will fail the test run.