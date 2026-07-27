# Reviewer Rejected — Human Review Required

**Job:** `DW.RPOS_CARM_IMPORT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output contains redundant and conflicting implementations of the ETL logic: the Ab Initio graph was correctly converted to a PySpark script, but the KSH wrapper was incorrectly converted to a standalone pandas script that duplicates the entire ETL process. The KSH wrapper should be retired as its orchestration is replaced by the Airflow DAG. Additionally, several literal error messages from the Ab Initio graph's Validate_Records transform were dropped in the PySpark script.

## Required Changes

1. Establish the PySpark script (`mp/map_rpos_carmen_import.py`) as the canonical implementation for the ETL logic.
2. Retire the KSH wrapper script entirely, as its orchestration is handled by the Airflow DAG and its ETL logic is redundant.
3. In the PySpark script, restore the dropped literal error messages from the Validate_Records transform.
## Per-File Review Results

- ✅ `abinitio_rpos_carmen_linked_job/DWH_BD_PROC_JOB/DW.RPOS_CARM_IMPORT.xml`
- ❌ `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/mp/map_rpos_carmen_import.mp`
  - 1. The canonical implementation for the ETL logic is this PySpark script.
2. Preserve the exact literal error messages from `Validate_Records-22.xfr` which were dropped. Add validation checks to raise these exact strings: "Invalid data format in monats_id", "Invalid data format in rechnung_datum", "Invalid data format in standardvertrags_id", "Invalid data format in vertrags_id", "Invalid data format in rechpos_brutto_eur", "Invalid data format in rechpos_netto_eur", and "Invalid data format in rechpos_mwst_eur".
- ❌ `abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.ksh`
  - 1. The canonical implementation for the ETL logic is the PySpark script (`mp/map_rpos_carmen_import.py`).
2. This KSH wrapper file should be fully retired and emptied (or omitted), as its orchestration logic is handled by the Airflow DAG and its ETL logic is redundant with the PySpark script. Do not re-implement the ETL logic in pandas here.