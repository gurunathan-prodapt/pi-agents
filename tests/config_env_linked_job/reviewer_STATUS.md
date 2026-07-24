# Reviewer Rejected — Human Review Required

**Job:** `DW.CFG_LOAD_PARAMS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains multiple concatenated drafts (three separate 'MIGRATION DESIGN DOCUMENT' headers and conflicting file plans), which caused the build to generate three redundant and conflicting Airflow DAG files. Additionally, the design and build violate the literal preservation rule by reworking 'Lade Parameter nach ${STG_TABLE} auf ${DB_HOST}/${DB_SID}' into 'Lade Parameter nach {dataset_id}.{table_id} ...' and completely dropping the error messages 'FEHLER: sqlldr beendet mit RC=${rc}' and 'FEHLER: d_param_load.sql beendet mit RC=${rc}'.

## Required Changes

['Consolidate the design document into a single, coherent plan with exactly one target Airflow DAG file.', "Ensure the Python script preserves the exact wording of 'Lade Parameter nach ${STG_TABLE} auf ${DB_HOST}/${DB_SID}' (using appropriate target variables for the placeholders).", "Ensure the Python script preserves the exact wording of the error messages 'FEHLER: sqlldr beendet mit RC=${rc}' and 'FEHLER: d_param_load.sql beendet mit RC=${rc}' (e.g., in exception blocks if the BigQuery load or Dataform invocation fails)."]