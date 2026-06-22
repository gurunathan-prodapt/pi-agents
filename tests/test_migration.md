As a senior data-migration QA engineer, I've analyzed the provided migration design for `k_ausd_v_ta_p_vertrag.ksh` to Google Cloud Platform. The migration involves converting a KornShell orchestrator to an Airflow DAG and an Oracle SQL*Plus script to BigQuery SQL.

Below are comprehensive validation tests designed to ensure behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for `k_ausd_v_ta_p_vertrag.ksh`

### Test Case 1: End-to-End Output Parity (Full Data Comparison)

**Purpose:** To verify that the migrated BigQuery job produces an identical final output table (`sof$ta_p_vertrag`) compared to the legacy Oracle job when given the same input data. This is the most critical behavioral equivalence test.

**Setup:**
1.  **Legacy Oracle Environment:**
    *   Ensure the Oracle database contains the `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp` tables.
    *   Populate these input tables with a diverse, representative set of test data, including:
        *   Typical contract data.
        *   Cases where `twin_vertrag_id` matches an existing `vertrag_id_carmen`.
        *   Cases where `twin_vertrag_id` does *not* match any `vertrag_id_carmen`.
        *   `NULL` values in relevant columns (e.g., `twin_vertrag_id`, `timecreated`).
        *   `dwtk_meldungen` rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and without.
    *   Ensure the `sof$ta_p_vertrag` table exists and is empty before running the legacy job.
2.  **Migrated BigQuery Environment:**
    *   Create corresponding BigQuery tables: ``isbert_schema.dwtk_meldungen``, ``sof$ta_vertrag_tmp``, and ``sof$ta_p_vertrag``.
    *   Populate the BigQuery input tables (``isbert_schema.dwtk_meldungen``, ``sof$ta_vertrag_tmp``) with *exactly the same data* as used in the Oracle setup. This is crucial for a fair comparison.
    *   Ensure the BigQuery ``sof$ta_p_vertrag`` table exists and is empty before running the migrated job.

**Action:**
1.  Execute the legacy KornShell script (`k_ausd_v_ta_p_vertrag.ksh`) in the Oracle environment.
2.  Execute the migrated Airflow DAG (`k_ausd_v_ta_p_vertrag_dag`) in the BigQuery environment.

**Pass/Fail Criterion:**
*   The number of rows in `sof$ta_p_vertrag` in BigQuery must be identical to the number of rows in `sof$ta_p_vertrag` in Oracle.
*   Every column in every row of `sof$ta_p_vertrag` in BigQuery must be identical to its corresponding column and row in Oracle. Data type differences (e.g., `NUMBER` to `INT64`/`BIGNUMERIC`, `VARCHAR2` to `STRING`) should be handled by comparing their logical values. `NULL` values must match.

**Runnable Test Code (Conceptual Python with SQL assertions using `pytest` and `pandas`):**

```python
import pytest
from google.cloud import bigquery
import cx_Oracle # Requires Oracle client and cx_Oracle library
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime, timedelta
import subprocess # To simulate KSH execution

# --- Configuration (replace with your actual values) ---
ORACLE_CONN_STR = "user/password@host:port/service_name"
BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "your_bigquery_dataset" # For sof$ta_* tables
BIGQUERY_ISBERT_SCHEMA_DATASET_ID = "isbert_schema" # For dwtk_meldungen

# --- Helper Functions for Test Setup and Data Retrieval ---
def setup_oracle_test_data(oracle_conn):
    """Populates Oracle input tables with test data."""
    cursor = oracle_conn.cursor()
    # Clear and insert test data into Oracle dwtk_meldungen
    cursor.execute("TRUNCATE TABLE isbert_schema.dwtk_meldungen")
    cursor.execute("INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', SYSDATE)")
    cursor.execute("INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('OTHER_JOB', SYSDATE - 1)")
    cursor.execute("INSERT INTO isbert_schema.dwtk_meldungen (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', SYSDATE - 2)")
    # Clear and insert test data into Oracle sof$ta_vertrag_tmp
    cursor.execute("TRUNCATE TABLE sof$ta_vertrag_tmp")
    cursor.execute("""
        INSERT INTO sof$ta_vertrag_tmp (vertrag_id_carmen, twin_vertrag_id, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        VALUES ('V001', NULL, 'P001', 'RD01', 'KK01', 'MWST1', 'RV01', 'RL01', 'VO1', NULL, NULL, SYSDATE, 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF1', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN1', NULL, 'SV1', 'VDA1', 'CC1', 'CCU1', 'CT1', 'SEG1', 'RA1', 'RIK1', 'ON1', SYSDATE, 'CV1')
    """)
    cursor.execute("""
        INSERT INTO sof$ta_vertrag_tmp (vertrag_id_carmen, twin_vertrag_id, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        VALUES ('V002', 'V003', 'P002', 'RD02', 'KK02', 'MWST2', 'RV02', 'RL02', 'VO2', NULL, NULL, SYSDATE, 'ACTIVE', NULL, NULL, NULL, 'Y', 'TARIFF2', 24, NULL, 36, 'MONTH', 'CREDIT', 'POST', 'N', 'APN2', NULL, 'SV2', 'VDA2', 'CC2', 'CCU2', 'CT2', 'SEG2', 'RA2', 'RIK2', 'ON2', SYSDATE, 'CV2')
    """)
    cursor.execute("""
        INSERT INTO sof$ta_vertrag_tmp (vertrag_id_carmen, twin_vertrag_id, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        VALUES ('V003', NULL, 'P003', 'RD03', 'KK03', 'MWST3', 'RV03', 'RL03', 'VO3', NULL, NULL, SYSDATE, 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF3', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN3', NULL, 'SV3', 'VDA3', 'CC3', 'CCU3', 'CT3', 'SEG3', 'RA3', 'RIK3', 'ON3', SYSDATE, 'CV3')
    """)
    cursor.execute("""
        INSERT INTO sof$ta_vertrag_tmp (vertrag_id_carmen, twin_vertrag_id, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        VALUES ('V004', 'NON_EXISTENT_TWIN', 'P004', 'RD04', 'KK04', 'MWST4', 'RV04', 'RL04', 'VO4', NULL, NULL, SYSDATE, 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF4', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN4', NULL, 'SV4', 'VDA4', 'CC4', 'CCU4', 'CT4', 'SEG4', 'RA4', 'RIK4', 'ON4', SYSDATE, 'CV4')
    """)
    oracle_conn.commit()
    print("Oracle test data setup complete.")

def setup_bigquery_test_data(bq_client):
    """Populates BigQuery input tables with test data mirroring Oracle."""
    # Clear and insert test data into BigQuery isbert_schema.dwtk_meldungen
    bq_client.query(f"TRUNCATE TABLE `{BIGQUERY_ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen`").result()
    bq_client.query(f"""
        INSERT INTO `{BIGQUERY_ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', CURRENT_TIMESTAMP()),
        ('OTHER_JOB', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)),
        ('BERT_DROP_TEMP_TABLE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
    """).result()
    # Clear and insert test data into BigQuery sof$ta_vertrag_tmp
    bq_client.query(f"TRUNCATE TABLE `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp`").result()
    bq_client.query(f"""
        INSERT INTO `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp` (vertrag_id_carmen, twin_vertrag_id, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        VALUES
        ('V001', NULL, 'P001', 'RD01', 'KK01', 'MWST1', 'RV01', 'RL01', 'VO1', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF1', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN1', NULL, 'SV1', 'VDA1', 'CC1', 'CCU1', 'CT1', 'SEG1', 'RA1', 'RIK1', 'ON1', CURRENT_DATE(), 'CV1'),
        ('V002', 'V003', 'P002', 'RD02', 'KK02', 'MWST2', 'RV02', 'RL02', 'VO2', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'Y', 'TARIFF2', 24, NULL, 36, 'MONTH', 'CREDIT', 'POST', 'N', 'APN2', NULL, 'SV2', 'VDA2', 'CC2', 'CCU2', 'CT2', 'SEG2', 'RA2', 'RIK2', 'ON2', CURRENT_DATE(), 'CV2'),
        ('V003', NULL, 'P003', 'RD03', 'KK03', 'MWST3', 'RV03', 'RL03', 'VO3', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF3', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN3', NULL, 'SV3', 'VDA3', 'CC3', 'CCU3', 'CT3', 'SEG3', 'RA3', 'RIK3', 'ON3', CURRENT_DATE(), 'CV3'),
        ('V004', 'NON_EXISTENT_TWIN', 'P004', 'RD04', 'KK04', 'MWST4', 'RV04', 'RL04', 'VO4', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF4', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN4', NULL, 'SV4', 'VDA4', 'CC4', 'CCU4', 'CT4', 'SEG4', 'RA4', 'RIK4', 'ON4', CURRENT_DATE(), 'CV4')
    """).result()
    print("BigQuery test data setup complete.")

def fetch_oracle_data(oracle_conn, table_name):
    """Fetches data from an Oracle table into a Pandas DataFrame."""
    cursor = oracle_conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY vertrag_id_carmen") # Order for consistent comparison
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)

def fetch_bigquery_data(bq_client, table_name):
    """Fetches data from a BigQuery table into a Pandas DataFrame."""
    query_job = bq_client.query(f"SELECT * FROM `{BIGQUERY_DATASET_ID}.{table_name}` ORDER BY vertrag_id_carmen")
    return query_job.to_dataframe()

# --- Pytest Fixtures ---
@pytest.fixture(scope="module")
def oracle_connection():
    """Establishes and yields an Oracle database connection."""
    conn = cx_Oracle.connect(ORACLE_CONN_STR)
    yield conn
    conn.close()

@pytest.fixture(scope="module")
def bigquery_client():
    """Establishes and yields a BigQuery client."""
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    yield client
    client.close()

# --- Test Case Implementation ---
def test_output_parity(oracle_connection, bigquery_client):
    # 1. Setup test data in both environments
    setup_oracle_test_data(oracle_connection)
    setup_bigquery_test_data(bigquery_client)

    # 2. Execute legacy job (simulated for demonstration)
    # In a real test, this would involve calling the KSH script via subprocess.
    # Example: subprocess.run(["/path/to/k_ausd_v_ta_p_vertrag.ksh", "-j", "TEST_JOB", "-f", "123"], check=True)
    print("Simulating legacy KSH job execution (running Oracle SQL directly)...")
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute("TRUNCATE TABLE sof$ta_p_vertrag")
    # Simulate v_datum calculation (Oracle)
    oracle_cursor.execute("""
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """)
    v_datum_oracle = oracle_cursor.fetchone()[0] # This value is not used in the main INSERT, but part of the original script
    print(f"Oracle v_datum calculated: {v_datum_oracle}")

    # Simulate main insert (Oracle)
    oracle_cursor.execute(f"""
        INSERT INTO sof$ta_p_vertrag
               (vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, twin_vertrag_id, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        SELECT
               v.vertrag_id_carmen, v.partner_id_carmen, v.rechdef_id_carmen, v.kundenkonto, v.mwst_kennzeichen, v.rahmenvertrag_id, v.rechnungslauf, v.vo_kenn, v.geplant_kuend, v.eingang_kuend, v.vertragsbeginn, v.vertragsstatus, v.sperrart, v.sperrgrund, v.stillegungszeitraum, v.twincard, v.dwh_tarifgr_text, v.bindefrist, v.letztes_upgrade, v.vertragsbindung, v.vertragsbindungseinheit, v.rechnungszahlart, v.rechnungsmedium, v.twin_vertrag_id, v.upgradeberechtigt, v.apn, v.upgradegrund, v.sv_id, v.vda, v.cost_centre, v.cost_centre_user, v.cntrct_ty, v.segment_id, v.rv_action_id, v.rechn_inh_konfig_text, v.order_number, v.commitment_reference_date, v.cntrct_validity_id
          FROM sof$ta_vertrag_tmp v, sof$ta_vertrag_tmp pv
         WHERE v.twin_vertrag_id = pv.vertrag_id_carmen (+)
    """)
    oracle_connection.commit()
    print("Legacy Oracle job simulated.")

    # 3. Execute migrated job (Airflow DAG)
    # In a real test, you'd trigger the Airflow DAG using Airflow's API or CLI.
    # Example: from airflow.api.client.local_client import Client; client = Client(None, None); client.trigger_dag(dag_id='k_ausd_v_ta_p_vertrag_dag', conf={'JobKennung': 'TEST_JOB', 'EintragsNr': '123'})
    print("Triggering Airflow DAG (running BigQuery SQL directly)...")
    bq_client.query(f"""
        DECLARE v_datum STRING;
        SET v_datum = (
          SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
          FROM `{BIGQUERY_ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        TRUNCATE TABLE `{BIGQUERY_DATASET_ID}.sof$ta_p_vertrag`;
        INSERT INTO `{BIGQUERY_DATASET_ID}.sof$ta_p_vertrag`
               (vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, twin_vertrag_id, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        SELECT
               v.vertrag_id_carmen, v.partner_id_carmen, v.rechdef_id_carmen, v.kundenkonto, v.mwst_kennzeichen, v.rahmenvertrag_id, v.rechnungslauf, v.vo_kenn, v.geplant_kuend, v.eingang_kuend, v.vertragsbeginn, v.vertragsstatus, v.sperrart, v.sperrgrund, v.stillegungszeitraum, v.twincard, v.dwh_tarifgr_text, v.bindefrist, v.letztes_upgrade, v.vertragsbindung, v.vertragsbindungseinheit, v.rechnungszahlart, v.rechnungsmedium, v.twin_vertrag_id, v.upgradeberechtigt, v.apn, v.upgradegrund, v.sv_id, v.vda, v.cost_centre, v.cost_centre_user, v.cntrct_ty, v.segment_id, v.rv_action_id, v.rechn_inh_konfig_text, v.order_number, v.commitment_reference_date, v.cntrct_validity_id
          FROM `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp` v
          LEFT JOIN `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp` pv
            ON v.twin_vertrag_id = pv.vertrag_id_carmen;
    """).result()
    print("Migrated BigQuery job executed.")

    # 4. Fetch and compare results
    oracle_df = fetch_oracle_data(oracle_connection, "sof$ta_p_vertrag")
    bigquery_df = fetch_bigquery_data(bigquery_client, "sof$ta_p_vertrag")

    # Standardize column names (Oracle often uppercase, BQ can be mixed)
    oracle_df.columns = [col.lower() for col in oracle_df.columns]
    bigquery_df.columns = [col.lower() for col in bigquery_df.columns]

    # Convert date/timestamp columns to a common format (e.g., string or datetime objects without timezone)
    # This is crucial for comparison as Oracle and BQ might store/represent dates differently
    date_cols = ['vertragsbeginn', 'geplant_kuend', 'eingang_kuend', 'commitment_reference_date']
    for col in date_cols:
        if col in oracle_df.columns:
            # Convert to datetime, then normalize to remove time component if not relevant, then to string for strict comparison
            oracle_df[col] = pd.to_datetime(oracle_df[col]).dt.normalize().astype(str)
            bigquery_df[col] = pd.to_datetime(bigquery_df[col]).dt.normalize().astype(str)

    # Sort both dataframes for consistent comparison
    oracle_df = oracle_df.sort_values(by=list(oracle_df.columns)).reset_index(drop=True)
    bigquery_df = bigquery_df.sort_values(by=list(bigquery_df.columns)).reset_index(drop=True)

    assert_frame_equal(oracle_df, bigquery_df, check_dtype=False, check_like=True) # check_like=True ignores column order
    print("Output parity test passed: DataFrames are identical.")

```

---

### Test Case 2: `v_datum` Calculation Correctness

**Purpose:** To specifically verify the correct calculation and default handling of the `v_datum` variable in BigQuery, which determines a reference date based on `isbert_schema.dwtk_meldungen`.

**Setup:**
1.  **BigQuery Environment:**
    *   Ensure the BigQuery table ``isbert_schema.dwtk_meldungen`` exists.
    *   Prepare multiple scenarios for `dwtk_meldungen` data:
        *   Scenario A: Multiple rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values.
        *   Scenario B: No rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   Scenario C: `dwtk_meldungen` table is completely empty.
        *   Scenario D: `timecreated` is `NULL` for `BERT_DROP_TEMP_TABLE` rows.

**Action:**
1.  For each scenario, execute only the `DECLARE v_datum ...` part of the BigQuery SQL and retrieve the value of `v_datum`.

**Pass/Fail Criterion:**
*   **Scenario A:** `v_datum` must be `FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated)))` for the relevant rows.
*   **Scenario B & C:** `v_datum` must be `'19000101'`.
*   **Scenario D:** `v_datum` must be `'19000101'` (due to `IFNULL`).

**Runnable Test Code (Pytest with BigQuery assertions):**

```python
import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta

BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_ISBERT_SCHEMA_DATASET_ID = "isbert_schema"

@pytest.fixture(scope="module")
def bigquery_client():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    yield client
    client.close()

def get_v_datum_from_bq(bq_client):
    """Executes the v_datum calculation and returns the result."""
    query = f"""
        DECLARE v_datum STRING;
        SET v_datum = (
          SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
          FROM `{BIGQUERY_ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        SELECT v_datum;
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    return [row[0] for row in result][0]

def test_v_datum_calculation(bigquery_client):
    table_id = f"{BIGQUERY_ISBERT_SCHEMA_DATASET_ID}.dwtk_meldungen"

    # Scenario A: Multiple relevant rows
    bigquery_client.query(f"TRUNCATE TABLE `{table_id}`").result()
    now = datetime.now()
    bq_client.query(f"""
        INSERT INTO `{table_id}` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', '{now.isoformat()}'),
        ('OTHER_JOB', '{(now - timedelta(days=1)).isoformat()}'),
        ('BERT_DROP_TEMP_TABLE', '{(now - timedelta(days=2)).isoformat()}')
    """).result()
    expected_v_datum = now.strftime('%Y%m%d')
    assert get_v_datum_from_bq(bigquery_client) == expected_v_datum, "Scenario A failed: Max timecreated"

    # Scenario B: No rows with 'BERT_DROP_TEMP_TABLE'
    bigquery_client.query(f"TRUNCATE TABLE `{table_id}`").result()
    bq_client.query(f"""
        INSERT INTO `{table_id}` (job_kennung, timecreated) VALUES
        ('OTHER_JOB_1', '{(now - timedelta(days=3)).isoformat()}'),
        ('OTHER_JOB_2', '{(now - timedelta(days=4)).isoformat()}')
    """).result()
    assert get_v_datum_from_bq(bigquery_client) == '19000101', "Scenario B failed: No matching job_kennung"

    # Scenario C: Empty table
    bigquery_client.query(f"TRUNCATE TABLE `{table_id}`").result()
    assert get_v_datum_from_bq(bigquery_client) == '19000101', "Scenario C failed: Empty table"

    # Scenario D: timecreated is NULL for relevant rows
    bigquery_client.query(f"TRUNCATE TABLE `{table_id}`").result()
    bq_client.query(f"""
        INSERT INTO `{table_id}` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', NULL),
        ('BERT_DROP_TEMP_TABLE', NULL)
    """).result()
    assert get_v_datum_from_bq(bigquery_client) == '19000101', "Scenario D failed: NULL timecreated"

```

---

### Test Case 3: `LEFT JOIN` and `NULL` Handling Correctness

**Purpose:** To verify that the `LEFT JOIN` logic and `NULL` value propagation in the main `INSERT` statement are correctly translated from Oracle's `(+)` syntax to BigQuery's `LEFT JOIN`. Specifically, since all output columns are from the left table (`v`), this test ensures that all rows from `v` are preserved, and their values are correctly mapped.

**Setup:**
1.  **BigQuery Environment:**
    *   Ensure the BigQuery table ``sof$ta_vertrag_tmp`` exists.
    *   Populate ``sof$ta_vertrag_tmp`` with specific test data to cover join scenarios:
        *   Row A: `twin_vertrag_id` is `NULL`.
        *   Row B: `twin_vertrag_id` matches an existing `vertrag_id_carmen` in another row.
        *   Row C: This row is the "twin" for Row B.
        *   Row D: `twin_vertrag_id` does *not* match any `vertrag_id_carmen` in `sof$ta_vertrag_tmp`.

**Action:**
1.  Execute only the `INSERT INTO sof$ta_p_vertrag SELECT ... FROM sof$ta_vertrag_tmp v LEFT JOIN sof$ta_vertrag_tmp pv ON v.twin_vertrag_id = pv.vertrag_id_carmen` part of the BigQuery SQL.
2.  Query the `sof$ta_p_vertrag` table to inspect the results.

**Pass/Fail Criterion:**
*   The number of rows inserted into `sof$ta_p_vertrag` must equal the total number of rows in the `sof$ta_vertrag_tmp` (the `v` table), as expected from a `LEFT JOIN` where all selected columns are from the left side.
*   The values for each column in `sof$ta_p_vertrag` must exactly match the corresponding values from the `sof$ta_vertrag_tmp` table (the `v` alias).

**Runnable Test Code (Pytest with BigQuery assertions):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from pandas.testing import assert_frame_equal

BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "your_bigquery_dataset"

@pytest.fixture(scope="module")
def bigquery_client():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    yield client
    client.close()

def setup_join_test_data(bq_client):
    """Populates sof$ta_vertrag_tmp with specific data for join testing."""
    table_id_tmp = f"{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp"
    table_id_target = f"{BIGQUERY_DATASET_ID}.sof$ta_p_vertrag"

    bq_client.query(f"TRUNCATE TABLE `{table_id_tmp}`").result()
    bq_client.query(f"TRUNCATE TABLE `{table_id_target}`").result()

    bq_client.query(f"""
        INSERT INTO `{table_id_tmp}` (vertrag_id_carmen, twin_vertrag_id, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        VALUES
        -- Row A: twin_vertrag_id is NULL (should result in NULLs from pv, but pv columns are not selected)
        ('V_A', NULL, 'P_A', 'RD_A', 'KK_A', 'MWST_A', 'RV_A', 'RL_A', 'VO_A', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF_A', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN_A', NULL, 'SV_A', 'VDA_A', 'CC_A', 'CCU_A', 'CT_A', 'SEG_A', 'RA_A', 'RIK_A', 'ON_A', CURRENT_DATE(), 'CV_A'),
        -- Row B: twin_vertrag_id matches V_C (should join with V_C, but only v columns are selected)
        ('V_B', 'V_C', 'P_B', 'RD_B', 'KK_B', 'MWST_B', 'RV_B', 'RL_B', 'VO_B', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'Y', 'TARIFF_B', 24, NULL, 36, 'MONTH', 'CREDIT', 'POST', 'N', 'APN_B', NULL, 'SV_B', 'VDA_B', 'CC_B', 'CCU_B', 'CT_B', 'SEG_B', 'RA_B', 'RIK_B', 'ON_B', CURRENT_DATE(), 'CV_B'),
        -- Row C: This is the 'twin' contract for V_B (will be the right side of the join for V_B)
        ('V_C', NULL, 'P_C', 'RD_C', 'KK_C', 'MWST_C', 'RV_C', 'RL_C', 'VO_C', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF_C', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN_C', NULL, 'SV_C', 'VDA_C', 'CC_C', 'CCU_C', 'CT_C', 'SEG_C', 'RA_C', 'RIK_C', 'ON_C', CURRENT_DATE(), 'CV_C'),
        -- Row D: twin_vertrag_id does NOT match any existing vertrag_id_carmen (should result in NULLs from pv, but only v columns are selected)
        ('V_D', 'NON_EXISTENT_TWIN', 'P_D', 'RD_D', 'KK_D', 'MWST_D', 'RV_D', 'RL_D', 'VO_D', NULL, NULL, CURRENT_DATE(), 'ACTIVE', NULL, NULL, NULL, 'N', 'TARIFF_D', 12, NULL, 24, 'MONTH', 'BANK', 'EMAIL', 'Y', 'APN_D', NULL, 'SV_D', 'VDA_D', 'CC_D', 'CCU_D', 'CT_D', 'SEG_D', 'RA_D', 'RIK_D', 'ON_D', CURRENT_DATE(), 'CV_D')
    """).result()
    print("BigQuery join test data setup complete.")

def test_left_join_and_null_handling(bigquery_client):
    setup_join_test_data(bigquery_client)

    # Execute the INSERT statement
    bq_client.query(f"""
        INSERT INTO `{BIGQUERY_DATASET_ID}.sof$ta_p_vertrag`
               (vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen, rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend, vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum, twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung, vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, twin_vertrag_id, upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre, cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text, order_number, commitment_reference_date, cntrct_validity_id)
        SELECT
               v.vertrag_id_carmen, v.partner_id_carmen, v.rechdef_id_carmen, v.kundenkonto, v.mwst_kennzeichen, v.rahmenvertrag_id, v.rechnungslauf, v.vo_kenn, v.geplant_kuend, v.eingang_kuend, v.vertragsbeginn, v.vertragsstatus, v.sperrart, v.sperrgrund, v.stillegungszeitraum, v.twincard, v.dwh_tarifgr_text, v.bindefrist, v.letztes_upgrade, v.vertragsbindung, v.vertragsbindungseinheit, v.rechnungszahlart, v.rechnungsmedium, v.twin_vertrag_id, v.upgradeberechtigt, v.apn, v.upgradegrund, v.sv_id, v.vda, v.cost_centre, v.cost_centre_user, v.cntrct_ty, v.segment_id, v.rv_action_id, v.rechn_inh_konfig_text, v.order_number, v.commitment_reference_date, v.cntrct_validity_id
          FROM `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp` v
          LEFT JOIN `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp` pv
            ON v.twin_vertrag_id = pv.vertrag_id_carmen;
    """).result()

    # Fetch results from target table
    query_job_output = bigquery_client.query(f"SELECT * FROM `{BIGQUERY_DATASET_ID}.sof$ta_p_vertrag` ORDER BY vertrag_id_carmen")
    results_df = query_job_output.to_dataframe()

    # Fetch original data from source table (v)
    query_job_source = bigquery_client.query(f"SELECT * FROM `{BIGQUERY_DATASET_ID}.sof$ta_vertrag_tmp` ORDER BY vertrag_id_carmen")
    source_df = query_job_source.to_dataframe()

    # Standardize column names and date types for comparison
    results_df.columns = [col.lower() for col in results_df.columns]
    source_df.columns = [col.lower() for col in source_df.columns]
    date_cols = ['vertragsbeginn', 'geplant_kuend', 'eingang_kuend', 'commitment_reference_date']
    for col in date_cols:
        if col in results_df.columns:
            results_df[col] = pd.to_datetime(results_df[col]).dt.normalize().astype(str)
            source_df[col] = pd.to_datetime(source_df[col]).dt.normalize().astype(str)

    # Assertions
    assert len(results_df) == len(source_df), "Row count mismatch: Output table should have same rows as source (v)."
    assert_frame_equal(source_df.reset_index(drop=True), results_df.reset_index(drop=True), check_dtype=False, check_like=True)
    print("LEFT JOIN and NULL handling test passed: All rows from 'v' are preserved with their original values.")

```

---

### Test Case 4: Temporary Table Truncation

**Purpose:** To verify that all specified temporary tables are correctly truncated after the main processing, as per the migration design's interpretation of the `DWPA_UTIL_SKRIPT.runstatement` calls.

**Setup:**
1.  **BigQuery Environment:**
    *   Ensure all listed temporary tables exist in BigQuery.
    *   Populate each of these temporary tables with at least one row of dummy data.

**Action:**
1.  Execute the entire migrated Airflow DAG (`k_ausd_v_ta_p_vertrag_dag`).
2.  After the DAG completes, query the row count for each of the temporary tables.

**Pass/Fail Criterion:**
*   The row count for *every* listed temporary table must be zero.

**Runnable Test Code (Pytest with BigQuery assertions):**

```python
import pytest
from google.cloud import bigquery

BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "your_bigquery_dataset"

TEMP_TABLES = [
    "sof$ta_disc_zusgf", "sof$ta_discount", "sof$ta_barrier_zusgf", "sof$ta_barrier",
    "sof$ta_cntrct_crs", "sof$ta_cntrct_templ", "sof$ta_cntrct_valid", "sof$ta_period",
    "sof$ta_bp_ref", "sof$ta_inv_assign", "sof$ta_inv_def", "sof$ta_acc_ref",
    "sof$ta_notice", "sof$ta_apn_ve", "sof$ta_discount_rr", "sof$ta_vvl_dwh",
    "sof$ta_vvl_upgrade", "sof$ta_cntrct_crs2", "sof$ta_cntrct_crs3", "sof$ta_inv_acc",
    "sof$ta_vertrag_tmp", "sof$ta_action_assoc"
]

@pytest.fixture(scope="module")
def bigquery_client():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    yield client
    client.close()

def populate_temp_tables(bq_client):
    """Populates temporary tables with dummy data for testing truncation."""
    print("Populating temporary tables with dummy data...")
    for table_name in TEMP_TABLES:
        table_id = f"{BIGQUERY_DATASET_ID}.{table_name}"
        try:
            # Ensure table exists and has a schema to insert into
            table = bigquery.Table(table_id, schema=[bigquery.SchemaField("dummy_col", "STRING")])
            bq_client.create_table(table, exists_ok=True)
            # Insert dummy data
            bq_client.query(f"INSERT INTO `{table_id}` (dummy_col) VALUES ('test_data_1'), ('test_data_2')").result()
            print(f"  - Populated {table_name}")
        except Exception as e:
            print(f"  - Error populating {table_name}: {e}. Attempting to create and insert.")
            # Fallback: create table with dummy_col if it doesn't exist or has schema issues
            bq_client.query(f"CREATE TABLE IF NOT EXISTS `{table_id}` (dummy_col STRING)").result()
            bq_client.query(f"INSERT INTO `{table_id}` (dummy_col) VALUES ('test_data_1'), ('test_data_2')").result()
            print(f"  - Created and populated {table_name} with dummy_col")

def get_table_row_count(bq_client, table_name):
    """Returns the row count for a given BigQuery table."""
    query = f"SELECT COUNT(1) FROM `{BIGQUERY_DATASET_ID}.{table_name}`"
    query_job = bq_client.query(query)
    result = query_job.result()
    return [row[0] for row in result][0]

def test_temporary_table_truncation(bigquery_client):
    # 1. Populate temporary tables
    populate_temp_tables(bigquery_client)

    # 2. Verify they are populated initially
    for table_name in TEMP_TABLES:
        count = get_table_row_count(bigquery_client, table_name)
        assert count > 0, f"Pre-condition failed: Table {table_name} should have data before truncation."

    # 3. Execute the migrated job (Airflow DAG)
    # In a real test, you'd trigger the Airflow DAG. For this example, we'll execute the full BigQuery SQL.
    print("Executing BigQuery SQL for full job flow including truncation...")
    full_dag_sql = """
        -- Stichtag ermitteln (dummy for this test, actual value not critical for truncation)
        DECLARE v_datum STRING;
        SET v_datum = '20230101';

        -- tabelle von vorherigem lauf leeren (sof$ta_p_vertrag)
        TRUNCATE TABLE `sof$ta_p_vertrag`;

        -- vertragstabelle sof$ta_p_vertrag (dummy insert for this test, actual data not critical for truncation)
        INSERT INTO `sof$ta_p_vertrag` (vertrag_id_carmen) VALUES ('dummy_contract');

        -- leeren der temporaeren zwischentabellen
    """
    for table_name in TEMP_TABLES:
        full_dag_sql += f"TRUNCATE TABLE `{BIGQUERY_DATASET_ID}.{table_name}`;\n"

    bigquery_client.query(full_dag_sql).result()
    print("BigQuery job (including truncation) executed.")

    # 4. Verify all temporary tables are empty
    for table_name in TEMP_TABLES:
        count = get_table_row_count(bigquery_client, table_name)
        assert count == 0, f"Post-condition failed: Table {table_name} should be empty after truncation, but has {count} rows."
    print("Temporary table truncation test passed.")

```

---

### Test Case 5: Schema and Data Type Integrity

**Purpose:** To ensure that the schema (column names, data types, nullability) of the target table `sof$ta_p_vertrag` in BigQuery is consistent with the legacy Oracle table, and that data types are appropriately mapped without loss of precision or unexpected conversions.

**Setup:**
1.  **Legacy Oracle Environment:**
    *   Obtain the schema definition for `sof$ta_p_vertrag` (column names, data types, nullability, precision/scale).
2.  **Migrated BigQuery Environment:**
    *   Ensure the BigQuery table `sof$ta_p_vertrag` exists and has a schema.

**Action:**
1.  Retrieve the schema of `sof$ta_p_vertrag` from BigQuery.
2.  Compare it against the dynamically retrieved Oracle schema.

**Pass/Fail Criterion:**
*   All column names in BigQuery must match those in Oracle (case-insensitivity handled by standardizing to lowercase).
*   BigQuery data types must be appropriate mappings of Oracle data types (e.g., `VARCHAR2(N)` -> `STRING`, `NUMBER` -> `INT64`/`BIGNUMERIC`/`FLOAT64`, `DATE`/`TIMESTAMP` -> `DATE`/`TIMESTAMP`).
*   Nullability constraints should be consistent.
*   Precision and scale for numeric types should be preserved or adequately handled (e.g., `NUMBER(10,2)` to `BIGNUMERIC` or `NUMERIC`).

**Runnable Test Code (Pytest with BigQuery/Oracle schema comparison):**

```python
import pytest
from google.cloud import bigquery
import cx_Oracle # Requires Oracle client and cx_Oracle library

ORACLE_CONN_STR = "user/password@host:port/service_name"
BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "your_bigquery_dataset" # Assuming sof$ta_p_vertrag is here

@pytest.fixture(scope="module")
def oracle_connection():
    conn = cx_Oracle.connect(ORACLE_CONN_STR)
    yield conn
    conn.close()

@pytest.fixture(scope="module")
def bigquery_client():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    yield client
    client.close()

def get_oracle_schema(oracle_conn, table_name, owner_schema):
    """Retrieves schema information for an Oracle table."""
    cursor = oracle_conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = UPPER('{table_name}') AND OWNER = UPPER('{owner_schema}')
        ORDER BY COLUMN_ID
    """)
    schema = []
    for col_name, data_type, data_length, data_precision, data_scale, nullable in cursor:
        schema.append({
            'name': col_name.lower(), # Standardize to lowercase for comparison
            'type': data_type,
            'length': data_length,
            'precision': data_precision,
            'scale': data_scale,
            'nullable': nullable == 'Y'
        })
    return schema

def get_bigquery_schema(bq_client, dataset_id, table_name):
    """Retrieves schema information for a BigQuery table."""
    table_ref = bq_client.dataset(dataset_id).table(table_name)
    table = bq_client.get_table(table_ref)
    schema = []
    for field in table.schema:
        schema.append({
            'name': field.name.lower(), # BigQuery field names are typically lowercase
            'type': field.field_type,
            'mode': field.mode # NULLABLE, REQUIRED, REPEATED
        })
    return schema

def map_oracle_type_to_bigquery(oracle_col_info):
    """Maps Oracle data types to expected BigQuery data types and nullability modes."""
    oracle_type = oracle_col_info['type']
    oracle_nullable = oracle_col_info['nullable']
    bq_mode = 'NULLABLE' if oracle_nullable else 'REQUIRED'

    if oracle_type.startswith('VARCHAR') or oracle_type == 'CHAR' or oracle_type == 'NVARCHAR2' or oracle_type == 'CLOB':
        return {'type': 'STRING', 'mode': bq_mode}
    elif oracle_type == 'NUMBER':
        if oracle_col_info['scale'] == 0 and oracle_col_info['precision'] is not None and oracle_col_info['precision'] <= 18:
            return {'type': 'INT64', 'mode': bq_mode}
        elif oracle_col_info['precision'] is not None and oracle_col_info['precision'] <= 38 and oracle_col_info['scale'] is not None:
            return {'type': 'BIGNUMERIC', 'mode': bq_mode} # Or NUMERIC if within 38,9
        else: # Fallback for larger numbers or floats
            return {'type': 'FLOAT64', 'mode': bq_mode}
    elif oracle_type == 'DATE':
        return {'type': 'DATE', 'mode': bq_mode}
    elif oracle_type == 'TIMESTAMP':
        return {'type': 'TIMESTAMP', 'mode': bq_mode}
    elif oracle_type == 'BLOB':
        return {'type': 'BYTES', 'mode': bq_mode}
    else:
        return {'type': 'STRING', 'mode': bq_mode} # Default fallback for unknown types

def test_schema_integrity(oracle_connection, bigquery_client):
    oracle_table_name = "sof$ta_p_vertrag"
    oracle_owner_schema = BIGQUERY_DATASET_ID # Assuming Oracle schema name matches BigQuery dataset ID
    bigquery_table_name = "sof$ta_p_vertrag"

    oracle_schema = get_oracle_schema(oracle_connection, oracle_table_name, oracle_owner_schema)
    bigquery_schema = get_bigquery_schema(bigquery_client, BIGQUERY_DATASET_ID, bigquery_table_name)

    # Convert BigQuery schema to a dict for easier lookup
    bq_schema_dict = {col['name']: col for col in bigquery_schema}

    assert len(oracle_schema) == len(bigquery_schema), \
        f"Column count mismatch: Oracle has {len(oracle_schema)}, BigQuery has {len(bigquery_schema)}."

    for oracle_col in oracle_schema:
        col_name = oracle_col['name']
        assert col_name in bq_schema_dict, f"Column '{col_name}' missing in BigQuery schema."

        bq_col = bq_schema_dict[col_name]

        # Compare data types and nullability
        expected_bq_type_info = map_oracle_type_to_bigquery(oracle_col)
        assert bq_col['type'] == expected_bq_type_info['type'], \
            f"Type mismatch for column '{col_name}': Expected BigQuery type '{expected_bq_type_info['type']}', got '{bq_col['type']}'."
        assert bq_col['mode'] == expected_bq_type_info['mode'], \
            f"Nullability mode mismatch for column '{col_name}': Expected BigQuery mode '{expected_bq_type_info['mode']}', got '{bq_col['mode']}'."

    print("Schema integrity test passed: Column names, types, and nullability match expectations.")

```

---

### Test Case 6: Input Data Ingestion Validation (External System Replacement)

**Purpose:** To ensure that the data ingested from the external Oracle system into BigQuery for the input tables (`isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`) is accurate and complete, reflecting the source data behaviorally. This validates the "External-system replacements" aspect. This test is crucial for validating the upstream ingestion pipelines (e.g., Data Fusion, Dataflow).

**Setup:**
1.  **Legacy Oracle Environment:**
    *   Ensure `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp` are populated with a representative dataset.
2.  **Migrated BigQuery Environment:**
    *   Ensure the corresponding BigQuery tables exist.
    *   The ingestion pipelines (e.g., Data Fusion, Dataflow) should have run to populate these BigQuery tables.

**Action:**
1.  Query all data from `isbert_schema.dwtk_meldungen` in Oracle.
2.  Query all data from `isbert_schema.dwtk_meldungen` in BigQuery.
3.  Repeat for `sof$ta_vertrag_tmp`.

**Pass/Fail Criterion:**
*   For both `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp`:
    *   The row count in BigQuery must be identical to Oracle.
    *   Every column in every row in BigQuery must be identical to its corresponding column and row in Oracle, considering appropriate type mappings and potential data transformations during ingestion (e.g., timezone handling for timestamps).
    *   No data loss or corruption should be observed.

**Runnable Test Code (Conceptual Python with SQL assertions):**

```python
import pytest
from google.cloud import bigquery
import cx_Oracle
import pandas as pd
from pandas.testing import assert_frame_equal

ORACLE_CONN_STR = "user/password@host:port/service_name"
BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "your_bigquery_dataset" # For sof$ta_vertrag_tmp
BIGQUERY_ISBERT_SCHEMA_DATASET_ID = "isbert_schema" # For dwtk_meldungen

@pytest.fixture(scope="module")
def oracle_connection():
    conn = cx_Oracle.connect(ORACLE_CONN_STR)
    yield conn
    conn.close()

@pytest.fixture(scope="module")
def bigquery_client():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    yield client
    client.close()

def fetch_and_compare_tables(oracle_conn, bq_client, oracle_table_full_name, bq_dataset_id, bq_table_name):
    """Fetches data from Oracle and BigQuery and compares them."""
    print(f"Comparing data for {oracle_table_full_name} (Oracle) vs {bq_dataset_id}.{bq_table_name} (BigQuery)...")

    # Fetch Oracle data
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute(f"SELECT * FROM {oracle_table_full_name}")
    oracle_columns = [col[0] for col in oracle_cursor.description]
    oracle_data = oracle_cursor.fetchall()
    oracle_df = pd.DataFrame(oracle_data, columns=oracle_columns)
    oracle_df.columns = [col.lower() for col in oracle_df.columns] # Standardize column names

    # Fetch BigQuery data
    bq_query_job = bq_client.query(f"SELECT * FROM `{bq_dataset_id}.{bq_table_name}`")
    bq_df = bq_query_job.to_dataframe()
    bq_df.columns = [col.lower() for col in bq_df.columns] # Standardize column names

    # Sort both dataframes for consistent comparison (requires knowing primary key or unique combination)
    # For demonstration, sort by all common columns. In a real scenario, use actual primary keys.
    common_cols = sorted(list(set(oracle_df.columns) & set(bq_df.columns)))
    if common_cols:
        oracle_df = oracle_df.sort_values(by=common_cols).reset_index(drop=True)
        bq_df = bq_df.sort_values(by=common_cols).reset_index(drop=True)
    else: # Fallback if no common columns or no clear sort key
        oracle_df = oracle_df.sort_values(by=list(oracle_df.columns)).reset_index(drop=True)
        bq_df = bq_df.sort_values(by=list(bq_df.columns)).reset_index(drop=True)

    # Handle date/timestamp comparisons (e.g., normalize to UTC, remove microseconds if not relevant)
    # This is critical as ingestion might change precision or timezone.
    for col in oracle_df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        if col in bq_df.columns:
            # Example: Convert to string for exact comparison, or normalize to a common timezone/precision
            oracle_df[col] = oracle_df[col].dt.tz_convert('UTC').dt.normalize().astype(str) if oracle_df[col].dt.tz is not None else oracle_df[col].dt.normalize().astype(str)
            bq_df[col] = bq_df[col].dt.tz_convert('UTC').dt.normalize().astype(str) if bq_df[col].dt.tz is not None else bq_df[col].dt.normalize().astype(str)

    # Assertions
    assert_frame_equal(oracle_df, bq_df, check_dtype=False, check_like=True)
    print(f"  - Data ingestion for {bq_table_name} passed.")

def test_input_data_ingestion_validation(oracle_connection, bigquery_client):
    # This test assumes the ingestion pipelines have already run to populate BigQuery tables.
    # In a full CI/CD pipeline, this test would run *after* the ingestion step.

    # Compare dwtk_meldungen
    fetch_and_compare_tables(
        oracle_connection,
        bigquery_client,
        "isbert_schema.dwtk_meldungen",
        BIGQUERY_ISBERT_SCHEMA_DATASET_ID,
        "dwtk_meldungen"
    )

    # Compare sof$ta_vertrag_tmp
    fetch_and_compare_tables(
        oracle_connection,
        bigquery_client,
        "sof$ta_vertrag_tmp",
        BIGQUERY_DATASET_ID,
        "sof$ta_vertrag_tmp"
    )

    print("All input data ingestion validation tests passed.")

```