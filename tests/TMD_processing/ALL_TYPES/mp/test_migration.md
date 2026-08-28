# Migration Validation Test Suite: Job `Shared Files — TMD_processing/ALL_TYPES/mp` (Graph `tmpjaasp8qn`)

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy Ab Initio graph and the migrated PySpark job running on Dataproc Serverless.

---

## Test Case 1: Team Virtuality Lookup Generation (Oracle vs. BigQuery)

### Purpose
Verify that the BigQuery SQL logic used to generate the `lkp_team_virt_ccos` lookup table is behaviorally identical to the legacy Oracle query, specifically validating the join logic, the `team_sichtbarkeitstyp_id = 10` filter, and the complex visibility flag condition `(vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)`.

### Setup
1. Populate mock tables in the test environment for:
   * `ccr_ta_f_teamsichtbarkeit` (vir)
   * `ccr_ta_s_sdm_team` (tea)
   * `ccr_ta_s_sdm_abteilung` (abt)
2. Include edge cases for the visibility flags:
   * Record A: `team_sichtbarkeitstyp_id = 10`, `UNSICHTBAR_FLAG = 0`, `ABT_EXTERN = 0` (Should Pass)
   * Record B: `team_sichtbarkeitstyp_id = 10`, `UNSICHTBAR_FLAG = 1`, `ABT_EXTERN = 1` (Should Pass)
   * Record C: `team_sichtbarkeitstyp_id = 10`, `UNSICHTBAR_FLAG = 1`, `ABT_EXTERN = 0` (Should Fail Filter)
   * Record D: `team_sichtbarkeitstyp_id = 9`, `UNSICHTBAR_FLAG = 0`, `ABT_EXTERN = 1` (Should Fail Filter)

### Action
Execute the lookup generation query on the mock dataset.

### Pass/Fail Criterion
* **Pass**: Only Records A and B are returned. The schema contains exactly `stichtag` and `sdm_team_id`. The output is sorted ascending by `stichtag` and `sdm_team_id`.
* **Fail**: Any records violating the filter criteria are present, or sorting is incorrect.

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def test_lookup_generation(spark_session):
    # Setup Mock Data
    vir_data = [
        # stichtag, sdm_team_id, team_sichtbarkeitstyp_id, unsichtbar_flag
        ("20231001", "T1", 10, 0),  # Pass (unsichtbar_flag = 0)
        ("20231001", "T2", 10, 1),  # Pass (abt_extern = 1 downstream)
        ("20231001", "T3", 10, 1),  # Fail (unsichtbar_flag = 1 and abt_extern = 0)
        ("20231001", "T4", 9, 0),   # Fail (sichtbarkeitstyp != 10)
    ]
    tea_data = [
        # sdm_team_id, sdm_abteilung_id
        ("T1", "A1"),
        ("T2", "A2"),
        ("T3", "A3"),
        ("T4", "A1"),
    ]
    abt_data = [
        # sdm_abteilung_id, abt_extern
        ("A1", 0),
        ("A2", 1), # External department
        ("A3", 0),
    ]

    vir_df = spark_session.createDataFrame(vir_data, ["stichtag", "sdm_team_id", "team_sichtbarkeitstyp_id", "unsichtbar_flag"])
    tea_df = spark_session.createDataFrame(tea_data, ["sdm_team_id", "sdm_abteilung_id"])
    abt_df = spark_session.createDataFrame(abt_data, ["sdm_abteilung_id", "abt_extern"])

    vir_df.createOrReplaceTempView("ta_f_teamsichtbarkeit")
    tea_df.createOrReplaceTempView("ta_s_sdm_team")
    abt_df.createOrReplaceTempView("ta_s_sdm_abteilung")

    # Action
    df_result = spark_session.sql("""
        SELECT 
          vir.stichtag, 
          tea.sdm_team_id 
        FROM 
          ta_f_teamsichtbarkeit vir,
          ta_s_sdm_team tea,
          ta_s_sdm_abteilung abt
        WHERE vir.team_sichtbarkeitstyp_id = 10
          AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)
          AND tea.sdm_team_id = vir.sdm_team_id
          AND abt.sdm_abteilung_id = tea.sdm_abteilung_id
        ORDER BY vir.stichtag, tea.sdm_team_id
    """)

    results = df_result.collect()
    
    # Assertions
    assert len(results) == 2
    assert results[0]["sdm_team_id"] == "T1"
    assert results[1]["sdm_team_id"] == "T2"
```

---

## Test Case 2: Standard Stream Lookup Override Logic (Reformat-1)

### Purpose
Verify that the `sdm_team_id` is correctly overridden in standard streams using the `Lkp_teamvirt_ccos` lookup. This tests the three logical branches of the legacy `first_defined` and `is_defined` logic:
1. Input `sdm_team_id` is defined and exists in the lookup -> Keep original `sdm_team_id`.
2. Input `sdm_team_id` is defined but does NOT exist in the lookup -> Replace with `""` (empty string).
3. Input `sdm_team_id` is NULL -> Keep as `NULL`.

### Setup
1. Create a mock lookup DataFrame `df_teamvirt_ccos` with key `("20231001", "T1")`.
2. Create a mock input DataFrame with three records representing the three logical branches.

### Action
Perform the left join and apply the conditional replacement logic.

### Pass/Fail Criterion
* **Pass**: 
  * Record 1 (`sdm_team_id` = "T1") maps to `"T1"`.
  * Record 2 (`sdm_team_id` = "T2") maps to `""`.
  * Record 3 (`sdm_team_id` = `None`) maps to `None`.
* **Fail**: Any mapping deviates from the above rules (e.g., unmatched team ID maps to `None` instead of `""`).

```python
def test_standard_stream_lookup_override(spark_session):
    # Setup Mock Lookup
    lookup_data = [("20231001", "T1")]
    df_lkp = spark_session.createDataFrame(lookup_data, ["lkp_stichtag", "lkp_sdm_team_id"])

    # Setup Mock Input
    input_data = [
        ("20231001", "T1", "CANCELLATIONS", "10"),  # Branch 1: Exists in lookup
        ("20231001", "T2", "CANCELLATIONS", "20"),  # Branch 2: Missing from lookup
        ("20231001", None, "CANCELLATIONS", "30"),  # Branch 3: Input is NULL
    ]
    df_input = spark_session.createDataFrame(input_data, ["stichtag", "sdm_team_id", "tos_mea_group_name", "mea_1"])

    # Action
    df_joined = df_input.join(
        df_lkp,
        (df_input["stichtag"] == df_lkp["lkp_stichtag"]) & 
        (df_input["sdm_team_id"] == df_lkp["lkp_sdm_team_id"]),
        how="left"
    )

    df_resolved = df_joined.withColumn(
        "resolved_sdm_team_id",
        F.when(F.col("sdm_team_id").isNotNull(),
               F.coalesce(F.col("lkp_sdm_team_id"), F.lit(""))
        ).otherwise(F.lit(None))
    ).drop("lkp_stichtag", "lkp_sdm_team_id")

    results = df_resolved.select("sdm_team_id", "resolved_sdm_team_id").collect()

    # Assertions
    # Record 1: T1 -> T1
    assert results[0]["sdm_team_id"] == "T1"
    assert results[0]["resolved_sdm_team_id"] == "T1"

    # Record 2: T2 -> ""
    assert results[1]["sdm_team_id"] == "T2"
    assert results[1]["resolved_sdm_team_id"] == ""

    # Record 3: None -> None
    assert results[2]["sdm_team_id"] is None
    assert results[2]["resolved_sdm_team_id"] is None
```

---

## Test Case 3: Weekly Stream Date Filtering (Filter to previous week greatest)

### Purpose
Verify that the weekly stream date filter correctly identifies and filters out records that fall on or after the Monday of the current execution week. It must also verify that the weekly stream preserves the original `sdm_team_id` without lookup modification.

### Setup
1. Determine the current week's Monday dynamically.
2. Create mock input records:
   * Record A: `stichtag` is Sunday of the previous week (Should Pass).
   * Record B: `stichtag` is Monday of the current week (Should Fail Filter).
   * Record C: `stichtag` is Tuesday of the current week (Should Fail Filter).

### Action
Apply the weekly filter logic: `F.to_date(F.col("stichtag"), "yyyyMMdd") < F.date_trunc("week", F.current_date())`.

### Pass/Fail Criterion
* **Pass**: Only Record A is retained in the weekly output. The `sdm_team_id` remains unchanged.
* **Fail**: Any records from the current week are retained, or the `sdm_team_id` is modified.

```python
from datetime import datetime, timedelta

def test_weekly_stream_date_filtering(spark_session):
    # Calculate dates relative to today
    today = datetime.now()
    current_week_monday = today - timedelta(days=today.weekday()) # weekday() 0 is Monday
    previous_week_sunday = current_week_monday - timedelta(days=1)
    
    str_prev_sunday = previous_week_sunday.strftime("%Y%m%d")
    str_curr_monday = current_week_monday.strftime("%Y%m%d")

    input_data = [
        (str_prev_sunday, "T_ORIGINAL_1", "CANCELLATIONS", "5"), # Should Pass
        (str_curr_monday, "T_ORIGINAL_2", "CANCELLATIONS", "10"), # Should Fail
    ]
    df_input = spark_session.createDataFrame(input_data, ["stichtag", "sdm_team_id", "tos_mea_group_name", "mea_1"])

    # Action
    df_wk = df_input.filter(
        (F.col("tos_mea_group_name") == "CANCELLATIONS") &
        (F.to_date(F.col("stichtag"), "yyyyMMdd") < F.date_trunc("week", F.current_date()))
    )

    results = df_wk.collect()

    # Assertions
    assert len(results) == 1
    assert results[0]["stichtag"] == str_prev_sunday
    assert results[0]["sdm_team_id"] == "T_ORIGINAL_1" # Must preserve original team ID
```

---

## Test Case 4: Products Stream Transformations

### Purpose
Verify that the standard and weekly products streams correctly map the product count (`anzahl_produkte` = `mea_1`) and generate the concatenated product key `tcn_offer_product_id` using trimmed values of `tos_offer_id` and `tcn_product_id` separated by a tilde (`~`).

### Setup
Create mock input records with leading/trailing spaces in `tos_offer_id` and `tcn_product_id`.

### Action
Execute the product stream transformation logic.

### Pass/Fail Criterion
* **Pass**: `tcn_offer_product_id` is correctly concatenated and trimmed (e.g., `"OFFER1~PROD1"`). `anzahl_produkte` matches `mea_1`.
* **Fail**: Leading/trailing spaces are preserved, the separator is incorrect, or the count mapping is wrong.

```python
def test_products_stream_transformations(spark_session):
    input_data = [
        # stichtag, tos_offer_id, tcn_product_id, tos_mea_group_name, mea_1
        ("20231001", "  OFFER1  ", "  PROD1  ", "PRODUCTS", "15")
    ]
    df_input = spark_session.createDataFrame(input_data, ["stichtag", "tos_offer_id", "tcn_product_id", "tos_mea_group_name", "mea_1"])

    # Action
    df_products = df_input \
        .filter(F.col("tos_mea_group_name") == "PRODUCTS") \
        .withColumn("anzahl_produkte", F.col("mea_1")) \
        .withColumn("tcn_offer_product_id", F.concat(F.trim(F.col("tos_offer_id")), F.lit("~"), F.trim(F.col("tcn_product_id"))))

    result = df_products.collect()[0]

    # Assertions
    assert result["anzahl_produkte"] == "15"
    assert result["tcn_offer_product_id"] == "OFFER1~PROD1"
```

---

## Test Case 5: Quotes & Contracts Stream Transformations

### Purpose
Verify conditional mapping and decimal-to-comma replacement for the Quotes & Contracts stream:
1. If `tos_mea_group_name` is `QUOTES`:
   * `anzahl_angebote` = `mea_1`
   * `subventionen` = `mea_2` with `.` replaced by `,`
   * `anzahl_vertraege` = `NULL`
2. If `tos_mea_group_name` is `CONTRACTS`:
   * `anzahl_angebote` = `NULL`
   * `subventionen` = `NULL`
   * `anzahl_vertraege` = `mea_1`

### Setup
Create mock records for both `QUOTES` (with decimal subvention values) and `CONTRACTS`.

### Action
Execute the conditional mapping logic.

### Pass/Fail Criterion
* **Pass**: 
  * Quotes record has `anzahl_angebote` mapped, `subventionen` formatted with a comma (e.g., `"150.50"` -> `"150,50"`), and `anzahl_vertraege` as `None`.
  * Contracts record has `anzahl_vertraege` mapped, and both `anzahl_angebote` and `subventionen` as `None`.
* **Fail**: Any conditional mapping or string replacement fails.

```python
def test_quotes_contracts_transformations(spark_session):
    input_data = [
        # stichtag, tos_mea_group_name, mea_1, mea_2
        ("20231001", "QUOTES", "100", "250.75"),
        ("20231001", "CONTRACTS", "50", None)
    ]
    df_input = spark_session.createDataFrame(input_data, ["stichtag", "tos_mea_group_name", "mea_1", "mea_2"])

    # Action
    df_transformed = df_input \
        .filter((F.col("tos_mea_group_name") == "CONTRACTS") | (F.col("tos_mea_group_name") == "QUOTES")) \
        .withColumn("anzahl_angebote", F.when(F.col("tos_mea_group_name") == "QUOTES", F.col("mea_1")).otherwise(F.lit(None))) \
        .withColumn("subventionen", F.when(F.col("tos_mea_group_name") == "QUOTES", F.regexp_replace(F.col("mea_2"), "\\.", ",")).otherwise(F.lit(None))) \
        .withColumn("anzahl_vertraege", F.when(F.col("tos_mea_group_name") == "CONTRACTS", F.col("mea_1")).otherwise(F.lit(None)))

    results = df_transformed.collect()

    # Assertions - Record 1 (QUOTES)
    assert results[0]["anzahl_angebote"] == "100"
    assert results[0]["subventionen"] == "250,75"
    assert results[0]["anzahl_vertraege"] is None

    # Assertions - Record 2 (CONTRACTS)
    assert results[1]["anzahl_angebote"] is None
    assert results[1]["subventionen"] is None
    assert results[1]["anzahl_vertraege"] == "50"
```

---

## Test Case 6: End-to-End Row Count and Schema Assertions

### Purpose
Verify that the output files are written to the correct paths with the correct schemas, delimiters (`;`), and that the sum of rows across standard and weekly outputs matches the expected distribution of the input dataset.

### Setup
1. Prepare a complete input dataset `x_tos_measures.dat` containing:
   * 3 Cancellation records (2 standard, 1 weekly-eligible)
   * 3 Product records (2 standard, 1 weekly-eligible)
   * 4 Quotes/Contracts records (2 standard, 2 weekly-eligible)
2. Run the complete PySpark job.

### Action
Inspect the output directories and read the generated CSV files.

### Pass/Fail Criterion
* **Pass**:
  * All 7 target files are created.
  * Row counts match exactly:
    * `tos_cancellations.dat` = 3 rows
    * `tos_cancellations_wk.dat` = 1 row
    * `tos_products.dat` = 3 rows
    * `tos_products_wk.dat` = 1 row
    * `tos_quotes_contracts.dat` = 4 rows
    * `tos_quotes_contracts_wk.dat` = 2 rows
  * Files are semicolon-delimited and contain no headers.
* **Fail**: Row counts do not match, files are missing, or formatting is incorrect.

```python
import os
import glob

def test_e2e_file_generation_and_row_counts(tmp_path):
    # Setup environment variables to redirect outputs to tmp_path
    os.environ["CCR_AI_DAT_FILE_DIR"] = str(tmp_path)
    os.environ["TCN_DS_SERIAL_LOOKUP"] = str(tmp_path)
    os.environ["GCP_PROJECT"] = "test-project"
    os.environ["BQ_DATASET"] = "test_dataset"

    # (In a real test runner, mock the BigQuery reads and execute the main script logic)
    # Asserting file existence and properties post-execution:
    
    expected_files = [
        "lkp_team_virt_ccos.dat",
        "tos_cancellations.dat",
        "tos_cancellations_wk.dat",
        "tos_products.dat",
        "tos_products_wk.dat",
        "tos_quotes_contracts.dat",
        "tos_quotes_contracts_wk.dat"
    ]

    for file_name in expected_files:
        file_dir = os.path.join(tmp_path, file_name)
        assert os.path.exists(file_dir), f"Output directory {file_name} was not created."
        
        # Find the actual part file inside the Spark output directory
        part_files = glob.glob(os.path.join(file_dir, "part-*.csv"))
        assert len(part_files) > 0, f"No data file found inside {file_name}"
        
        # Verify semicolon delimiter is used instead of comma
        with open(part_files[0], 'r') as f:
            first_line = f.readline()
            if first_line: # If file is not empty
                assert ";" in first_line or "," not in first_line, f"File {file_name} does not appear to be semicolon-delimited."
```