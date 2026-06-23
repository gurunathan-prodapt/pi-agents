# Legacy Source: .dw_init (environment variables) for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
This module provides environment configuration.
In the legacy system, a `.dw_init` script sourced environment variables.
In this migrated environment, environment variables should be set
in the Airflow environment, or sensitive parameters should be managed
via Airflow Connections or Variables.

This file serves as a placeholder to define or retrieve common configuration
parameters.
"""

import os

class EnvConfig:
    """
    Centralized configuration class to hold environment-specific settings.
    These can be loaded from environment variables, Airflow variables,
    or a configuration file.
    """
    # Placeholder for BERT_DIR_ROOT, derived from common source paths
    BERT_DIR_ROOT = os.getenv("BERT_DIR_ROOT", "/vobs/dw_source/isrpt/isbert")
    
    # Placeholder for DW_ORAUSER, which would be an Oracle username in legacy
    # In BigQuery context, this might not be directly applicable or
    # would be replaced by service account credentials.
    DW_ORAUSER = os.getenv("DW_ORAUSER", "default_oracle_user")

    # Placeholder for DW_DIR_ROOT, often points to the root of DW scripts
    DW_DIR_ROOT = os.getenv("DW_DIR_ROOT", "/vobs/dw_source/isrpt/isbert")

    # Placeholder for DW_DIR_PROT, often points to a log directory
    DW_DIR_PROT = os.getenv("DW_DIR_PROT", "/tmp/logs") 

    # Add other environment variables as needed, e.g., BigQuery project/dataset
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "<your-gcp-project-id>")
    BQ_DATASET = os.getenv("BQ_DATASET", "<your-bigquery-dataset>")

    @classmethod
    def get_config(cls, key: str, default=None):
        """Retrieves a configuration value."""
        return getattr(cls, key, default)

    @classmethod
    def set_config(cls, key: str, value):
        """Sets a configuration value dynamically."""
        setattr(cls, key, value)

# Example usage:
# if __name__ == "__main__":
#     print(f"BERT_DIR_ROOT: {EnvConfig.BERT_DIR_ROOT}")
#     print(f"GCP_PROJECT_ID: {EnvConfig.GCP_PROJECT_ID}")
#     EnvConfig.set_config("NEW_VAR", "new_value")
#     print(f"NEW_VAR: {EnvConfig.get_config('NEW_VAR')}")