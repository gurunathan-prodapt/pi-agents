# Note on PySpark Configuration Files for DW.DWH_APT_EXPORT_MONATLICH_JP

## Purpose

This document addresses the "Configuration Files for PySpark Jobs" item from the Build Plan (Section 8) of the Migration Design Document for `DW.DWH_APT_EXPORT_MONATLICH_JP`. The original UC4 job referenced `.var` files (`h_exis_apt_nna_daten.var`, `h_exis_apt_nna_voice.var`) which are described as containing configuration and potentially embedded SQL.

## Approach for this Migration

Given that the specific content and format of `h_exis_apt_nna_daten.var` and `h_exis_apt_nna_voice.var` are unknown and require manual reverse-engineering (as noted in "Unresolved / Risks" Section 7 of the design document), the current PySpark applications (`nna_data_exporter.py` and `nna_voice_exporter.py`) have adopted the following strategy:

1.  **Embedded Configuration Placeholders:** Connection details (JDBC URL, user, password) and the core SQL queries are currently embedded as placeholder variables directly within the PySpark script files. These are clearly marked with comments indicating they need to be replaced with actual values and logic.

2.  **Argument-based Parameters:** Dynamic parameters like `monat_id` and `output_gcs_bucket` are passed to the PySpark applications via command-line arguments, which are handled by `argparse`. This allows for flexible configuration from the orchestrating Airflow DAG.

## Future Enhancements / Next Steps

Once the content of the original `.var` files is successfully reverse-engineered, the recommended approach for managing these configurations in the GCP environment is as follows:

*   **External Configuration Files:** For more complex configurations or sensitive data, consider externalizing these into dedicated configuration files (e.g., YAML, JSON, or `.env` files). These files can then be:
    *   Stored in a GCS bucket and read by the PySpark applications at runtime.
    *   Managed through Airflow variables or connections, if appropriate.
*   **Secret Management:** For sensitive credentials (like database passwords), utilize Google Cloud Secret Manager. The PySpark applications can then securely retrieve these secrets at runtime.
*   **Parameterized SQL:** If the `.var` files contain large or dynamic SQL queries, these can be stored as separate `.sql` files in a GCS bucket and loaded by the PySpark jobs, allowing for easier management and versioning of the SQL logic.

**Action Required:**
The embedded placeholder configurations within `nna_data_exporter.py` and `nna_voice_exporter.py` must be updated with the actual logic and values extracted from the legacy `h_exis_apt_nna_daten.var` and `h_exis_apt_nna_voice.var` files. This will be a manual step as part of the detailed design and implementation phase.