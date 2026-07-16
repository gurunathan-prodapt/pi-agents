"""
gcp_dataproc_helpers.py
Reusable helper functions for Cloud Dataproc Job generation.
"""

from typing import List, Dict, Any

def build_pyspark_job_config(
    project_id: str,
    cluster_name: str,
    bucket_name: str,
    script_name: str,
    args: List[str]
) -> Dict[str, Any]:
    """
    Generates standard PySpark payload for DataprocSubmitJobOperator.
    
    :param project_id: GCP Project ID
    :param cluster_name: Cloud Dataproc Cluster Name
    :param bucket_name: Cloud Storage Bucket holding PySpark scripts
    :param script_name: Name of the .py script to run
    :param args: List of string parameters passed to the script
    :return: Formatted PySpark job configuration dictionary
    """
    return {
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket_name}/pyspark_scripts/{script_name}",
            "args": args
        }
    }