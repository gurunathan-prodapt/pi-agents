"""
Migrated Utility from UC4 JOBI: DW.HOLE_PFAD_VTRG
Performs central path resolution for the Data Warehouse (DWH) environment.
Fetches paths from Airflow Variables (or GCP Secrets Manager) with fallback defaults.
"""

import os
import logging
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Global Constant Fallbacks via GCP environment variable policies
GCS_BUCKET = os.environ.get("GCS_BUCKET")


def resolve_dwh_paths(**context) -> dict:
    """
    Resolves target GCS path coordinates mimicking the legacy 'DW.VARIABLEN' container.
    Pushes resolved paths to XCom for downstream dynamic reference.
    
    Returns:
        dict: A dictionary containing the resolved paths.
    """
    try: 
        # Retrieve mapped variables from Airflow DB / Secrets Manager
        # Standardizing on GCP_PROJECT, GCS_BUCKET policies without placeholder literals
        default_bucket_path = f"gs://{GCS_BUCKET}" if GCS_BUCKET else "gs://dwh-bucket-default"
        
        dwh_home = Variable.get(
            "dw_variablen_dwh_home", 
            default_var=f"{default_bucket_path}/dwh_home"
        )
        home = Variable.get(
            "dw_variablen_home", 
            default_var=f"{default_bucket_path}/home"
        )
        pms_home = Variable.get(
            "dw_variablen_pms_home", 
            default_var=f"{default_bucket_path}/pms_home"
        )
        
        # Structure the payload
        resolved_paths = {
            "DWH_HOME": dwh_home,
            "HOME": home,
            "PMS_HOME": pms_home
        }
        
        # Safely push variables to XCom if context is provided
        if context and 'ti' in context:
            ti = context['ti']
            for key, val in resolved_paths.items():
                ti.xcom_push(key=key, value=val)
                
        logging.info(
            f"Paths successfully resolved: DWH_HOME={dwh_home}, "
            f"HOME={home}, PMS_HOME={pms_home}"
        )
        return resolved_paths

    except Exception as e:
        error_msg = f"Failed to resolve path variables: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)