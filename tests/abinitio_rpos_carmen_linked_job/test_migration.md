# Migration Validation Test Suite: DW.RPOS_CARM_IMPORT

This document defines the migration-validation test suite for the migrated job `DW.RPOS_CARM_IMPORT`. These tests are designed to prove behavioral equivalence between the legacy Ab Initio/KornShell/UC4 implementation and the migrated Apache Airflow/PySpark/BigQuery implementation.

---

## Test Case 1: End-to-End Orchestration & Execution Parity

### Purpose
Verify that the Airflow DAG `dw_rpos_carm_import` successfully orchestrates the entire pipeline, triggering the Dataproc Serverless PySpark job with correct parameters, and that the execution completes without errors.

### Setup
1. Ensure Airflow variables (`GCP_PROJECT`, `GCP_REGION`, `DATAPROC_CLUSTER`, `GCS_BUCKET`) are populated in the test environment.
2. Upload a sample input file `CARMEN_B_TEST_pos.fix` to `gs://{GCS_BUCKET}/crs/work/`.
3. Populate the BigQuery source table `dwh_ta_c_vertrag` with basic test contract data.
4. Clear target tables in BigQuery: `dwh_ta_f_rpos_carm`, `dwh_ta_t_rpos_carm`, `dwh_ta_f_gpos_fact_carm`, `dwh_ta_f_rpos_fact_carm`, `dwh_ta_f_rpos_reselling_carm`.

### Action
Trigger the Airflow DAG `dw_rpos_carm_import` manually or via the Airflow CLI:
```bash
airflow dags trigger -c '{"BHB_Dateiname": "CARMEN_B_TEST_pos.fix"}' dw_rpos_carm_import
```

### Pass/Fail Criterion
* **Pass**: The DAG run state is `success`. The task `map_rpos_carmen_import` completes with exit code `0`. Dataproc batch logs show successful execution of `map_rpos_carmen_import_pyspark.py`.
* **Fail**: Any task in the DAG fails, or the Dataproc job fails with a execution error.

---

## Test Case 2: Ingestion, Splitting, and Data Cleansing

### Purpose
Verify that the PySpark job correctly splits raw input files into active data payloads (`Nutzdaten` starting with `P`) and metadata trailer records (starting with `X`), and normalizes European comma decimal separators to periods.

### Setup
1. Create a test file `CARMEN_B_CLEANSE_pos.fix` with the following content:
   ```csv
   H;HeaderInfo;20260421
   P;202604;DEB01;REC01;20260421;10001;20001;LEIST01;120,50;101,26;19,24;N;30001;40001;PROV01;1;1;F;V01;K5
   X;TEST_REMARK_ABSGRP_12345;20260421;1;P_RECORD_CONTENT;20260421;999
   ```
2. Upload this file to `gs://{GCS_BUCKET}/crs/work/`.

### Action
Run a PySpark test script to validate the parsing and cleansing step:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test_cleansing").getOrCreate()

def test_ingestion_and_cleansing(spark):
    gcs_bucket = os.environ.get("GCS_BUCKET")
    input_path = f"gs://{gcs_bucket}/crs/work/CARMEN_B_CLEANSE_pos.fix"
    
    rdd = spark.sparkContext.textFile(input_path)
    
    # Isolate Nutzdaten (P)
    position_rdd = rdd.filter(lambda line: line.startswith("P"))
    assert position_rdd.count() == 1
    
    # Parse line
    parts = position_rdd.first().split(";")
    
    # Normalize decimals (replace ',' with '.')
    brutto = parts[8].replace(",", ".")
    netto = parts[9].replace(",", ".")
    mwst = parts[10].replace(",", ".")
    
    assert brutto == "120.50"
    assert netto == "101.26"
    assert mwst == "19.24"
```

### Pass/Fail Criterion
* **Pass**: The payload record is successfully isolated, and decimal values are correctly normalized to standard period-based floats.
* **Fail**: The payload record is missed, or decimal values retain commas, causing downstream casting failures.

---

## Test Case 3: Strict Data Validation & Error Handling

### Purpose
Verify that the PySpark job raises the exact legacy German error messages when encountering invalid formats or nulls in mandatory fields, matching the legacy Ab Initio `force_error()` behavior.

### Setup
1. Create three invalid test files in GCS:
   * `CARMEN_B_ERR_MONAT_pos.fix`: Contains an invalid `monats_id` (e.g., `2026AB`).
   * `CARMEN_B_ERR_DATE_pos.fix`: Contains an invalid `rechnung_datum` (e.g., `2026-04-21`).
   * `CARMEN_B_ERR_NUM_pos.fix`: Contains a non-numeric `vertrags_id` (e.g., `NOT_A_NUMBER`).

### Action
Execute the PySpark validation function against these invalid records using `pytest`:

```python
import pytest
from abinitio_rpos_carmen_linked_job.TMD_processing.BHB.BD_PROC.run.map_rpos_carmen_import_pyspark import validate_reformat_for_db

def test_invalid_monats_id():
    bad_row = {
        "monats_id": "2026AB", "debitor_id": "DEB01", "rechnung_id": "REC01",
        "rechnung_datum": "20260421", "standardvertrags_id": "10001", "vertrags_id": "20001",
        "rech_leistung_id_carm": "LEIST01", "rechpos_brutto_eur": "100.00",
        "rechpos_netto_eur": "84.00", "rechpos_mwst_eur": "16.00"
    }
    with pytest.raises(ValueError) as excinfo:
        validate_reformat_for_db(bad_row)
    assert "Invalid data format in monats_id" in str(excinfo.value)

def test_invalid_rechnung_datum():
    bad_row = {
        "monats_id": "202604", "debitor_id": "DEB01", "rechnung_id": "REC01",
        "rechnung_datum": "2026-04-21", "standardvertrags_id": "10001", "vertrags_id": "20001",
        "rech_leistung_id_carm": "LEIST01", "rechpos_brutto_eur": "100.00",
        "rechpos_netto_eur": "84.00", "rechpos_mwst_eur": "16.00"
    }
    with pytest.raises(ValueError) as excinfo:
        validate_reformat_for_db(bad_row)
    assert "Invalid data format in rechnung_datum" in str(excinfo.value)

def test_invalid_vertrags_id():
    bad_row = {
        "monats_id": "202604", "debitor_id": "DEB01", "rechnung_id": "REC01",
        "rechnung_datum": "20260421", "standardvertrags_id": "10001", "vertrags_id": "NOT_A_NUMBER",
        "rech_leistung_id_carm": "LEIST01", "rechpos_brutto_eur": "100.00",
        "rechpos_netto_eur": "84.00", "rechpos_mwst_eur": "16.00"
    }
    with pytest.raises(ValueError) as excinfo:
        validate_reformat_for_db(bad_row)
    assert "Invalid data format in vertrags_id" in str(excinfo.value)
```

### Pass/Fail Criterion
* **Pass**: Each validation failure raises a `ValueError` containing the exact legacy German error string.
* **Fail**: The validation passes silently, or raises a generic/different error message.

---

## Test Case 4: Master Contract Join & Temporal Boundary Validation

### Purpose
Verify that the left join with `dwh_ta_c_vertrag` correctly maps contract attributes, and that the temporal boundary check ("Proof Join") nullifies contract attributes if the transaction month's last day falls outside the contract's validity range (`gueltig_von` to `gueltig_bis`).

### Setup
1. Populate `dwh_ta_c_vertrag` in BigQuery with two test contracts:
   * **Contract A**: `vertrag_id_carmen = 20001`, `gueltig_von = 2005-04-01`, `gueltig_bis = 2026-04-30` (Valid for April 2026).
   * **Contract B**: `vertrag_id_carmen = 20002`, `gueltig_von = 2005-04-01`, `gueltig_bis = 2026-03-31` (Expired before April 2026).
2. Create input transactions with `monats_id = 202604` (Month last day = `2026-04-30`) for both contracts.

### Action
Execute a BigQuery validation query to verify the join and temporal logic:

```sql
-- Assert Contract A (Valid) retains its mapped attributes
SELECT 
  vertrags_id,
  rahmenvertrag_id,
  dwh_vertrag_id
FROM `${GCP_PROJECT}.${BQ_DATASET}.vw_ranked`
WHERE vertrags_id = 20001;

-- Assert Contract B (Expired) has its mapped attributes nullified
SELECT 
  vertrags_id,
  rahmenvertrag_id,
  dwh_vertrag_id
FROM `${GCP_PROJECT}.${BQ_DATASET}.vw_ranked`
WHERE vertrags_id = 20002;
```

### Pass/Fail Criterion
* **Pass**: 
  * Contract A (`20001`) retains its `rahmenvertrag_id` and `dwh_vertrag_id`.
  * Contract B (`20002`) has its `rahmenvertrag_id` and `dwh_vertrag_id` set to `NULL`.
* **Fail**: Contract B retains its mapped attributes despite being expired, or Contract A's attributes are incorrectly nullified.

---

## Test Case 5: Priority Deduplication & Ranking

### Purpose
Verify that when multiple contract matches exist for a single `vertrags_id`, the pipeline correctly ranks them by `gueltig_von DESC, dwh_vertrag_id DESC` and selects only the top match (`rankindex = 1`).

### Setup
1. Populate `dwh_ta_c_vertrag` with duplicate records for `vertrag_id_carmen = 20003`:
   * **Record 1**: `dwh_vertrag_id = 99999901`, `gueltig_von = 2010-01-01`, `gueltig_bis = 2030-12-31`.
   * **Record 2**: `dwh_vertrag_id = 99999902`, `gueltig_von = 2020-01-01`, `gueltig_bis = 2030-12-31` (More recent `gueltig_von`).
2. Create an input transaction for `vertrags_id = 20003` with `monats_id = 202604`.

### Action
Execute the PySpark ranking logic and assert the selected contract:

```python
def test_priority_deduplication(spark):
    # Mock joined dataframe
    data = [
        ("202604", "20003", "99999901", "2010-01-01"),
        ("202604", "20003", "99999902", "2020-01-01")
    ]
    columns = ["monats_id", "vertrags_id", "dwh_vertrag_id", "gueltig_von"]
    df = spark.createDataFrame(data, columns)
    df.createOrReplaceTempView("vw_proofed")
    
    # Apply ranking
    df_ranked = spark.sql("""
        SELECT *,
            row_number() OVER (
                PARTITION BY vertrags_id, monats_id
                ORDER BY gueltig_von DESC, dwh_vertrag_id DESC
            ) AS rankindex
        FROM vw_proofed
    """)
    
    active_record = df_ranked.filter("rankindex = 1").collect()[0]
    
    # Assert that the record with the more recent gueltig_von is selected
    assert active_record["dwh_vertrag_id"] == "99999902"
```

### Pass/Fail Criterion
* **Pass**: The record with `dwh_vertrag_id = 99999902` is assigned `rankindex = 1` and selected.
* **Fail**: The older contract record is selected, or duplicate rows are propagated downstream.

---

## Test Case 6: Multi-Channel Routing & Target Distribution

### Purpose
Verify that processed records are correctly routed to their respective target tables based on the business rules for `typ` and `rpos_geschaeftsform_kenn`.

### Setup
1. Create an input file with 5 distinct records:
   * **Row 1**: `typ = 'T'` (Should route to `dwh_ta_t_rpos_carm`).
   * **Row 2**: `rpos_geschaeftsform_kenn = 'G'` (Should route to `dwh_ta_f_gpos_fact_carm`).
   * **Row 3**: `rpos_geschaeftsform_kenn = 'F'` (Should route to `dwh_ta_f_rpos_fact_carm`).
   * **Row 4**: `rpos_geschaeftsform_kenn = 'R'` (Should route to `dwh_ta_f_rpos_reselling_carm`).
   * **Row 5**: Standard record (Should route to `dwh_ta_f_rpos_carm`).

### Action
Run the PySpark pipeline and execute row-count assertions on BigQuery:

```sql
-- Assert routing counts
SELECT 'dwh_ta_t_rpos_carm' AS target, COUNT(*) AS cnt FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_t_rpos_carm`
UNION ALL
SELECT 'dwh_ta_f_gpos_fact_carm', COUNT(*) FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_f_gpos_fact_carm`
UNION ALL
SELECT 'dwh_ta_f_rpos_fact_carm', COUNT(*) FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_f_rpos_fact_carm`
UNION ALL
SELECT 'dwh_ta_f_rpos_reselling_carm', COUNT(*) FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_f_rpos_reselling_carm`
UNION ALL
SELECT 'dwh_ta_f_rpos_carm', COUNT(*) FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_f_rpos_carm`;
```

### Pass/Fail Criterion
* **Pass**: Each target table receives exactly 1 record matching the routing criteria.
* **Fail**: Records are misrouted, duplicated across tables, or lost.

---

## Test Case 7: Transactional Reload Idempotency (Delete-before-Insert)

### Purpose
Verify that the pipeline prevents duplicate appends by executing a clean delete of existing records matching the business keys before inserting new ones (idempotency).

### Setup
1. Populate `dwh_ta_f_rpos_carm` with a pre-existing record:
   * `rechnung_id = 'REC_ID_999'`, `rechnung_datum = '2026-04-21'`, `standardvertrags_id = 10001`, `vertrags_id = 20001`, `rech_leistung_id_carm = 'LEIST01'`, `rahmenvertrag = 'OLD_FRAMEWORK'`.
2. Create an input file containing a record with the exact same business keys but a different framework value: `rahmenvertrag = 'NEW_FRAMEWORK'`.

### Action
Run the Python wrapper script `map_rpos_carmen_import.py` which executes the pre-load deletes and submits the PySpark job. Then query the target table:

```sql
SELECT rahmenvertrag, COUNT(*) as cnt 
FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_f_rpos_carm`
WHERE rechnung_id = 'REC_ID_999'
GROUP BY rahmenvertrag;
```

### Pass/Fail Criterion
* **Pass**: The query returns exactly 1 row with `rahmenvertrag = 'NEW_FRAMEWORK'`. The old record was successfully deleted before the new one was inserted.
* **Fail**: The query returns 2 rows (duplicate append), or 0 rows (data loss).

---

## Test Case 8: Operational Logging & Metadata Auditing

### Purpose
Verify that the trailer record (`X`) correctly updates the operational logging tables `dwh_ta_k_meldungen` and `DWH_TA_K_RECH_ABSGRP` with accurate record counts, filenames, and timestamps.

### Setup
1. Insert a baseline run marker in `dwh_ta_k_meldungen` with `entrynr = 999`.
2. Provide an input file `CARMEN_B_AUDIT_pos.fix` containing the trailer record:
   ```csv
   X;CARMEN_B_AUDIT_pos.fix;20260421;150;TRAILER_TEXT;20260421;999
   ```

### Action
Run the pipeline and execute verification queries against the logging tables:

```sql
-- Assert dwh_ta_k_meldungen update
SELECT 
  anzahl_ds_eof,
  dateiname,
  enderecord_text,
  zusatzinfo
FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_k_meldungen`
WHERE entrynr = 999;

-- Assert DWH_TA_K_RECH_ABSGRP update/insert
SELECT 
  monats_id,
  abs_grp,
  dateiname,
  rechnungsteil
FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_k_rech_absgrp`
WHERE dateiname = 'CARMEN_B_AUDIT_pos.fix';
```

### Pass/Fail Criterion
* **Pass**: 
  * `dwh_ta_k_meldungen` is updated with `anzahl_ds_eof = 150`, `dateiname = 'CARMEN_B_AUDIT_pos.fix'`, and `enderecord_text = 'TRAILER_TEXT'`.
  * `dwh_ta_k_rech_absgrp` contains a record for `monats_id = '202603'` (derived from `stichtag` minus 1 month) and `rechnungsteil = 'P'`.
* **Fail**: Logging tables are not updated, or are updated with incorrect values.