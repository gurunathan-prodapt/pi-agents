"""
DAG: dw_cfg_load_params
Source UC4 Object: DW.CFG_LOAD_PARAMS (JOBS_UNIX)
Schedule: None (Manual / Sensor-driven execution)

Description:
This DAG orchestrates the loading of Data Warehouse (DWH) parameter configurations 
into the staging environment, migrating the legacy UC4 UNIX Job "DW.CFG_LOAD_PARAMS".

In order to avoid structural contradictions and prevent orphaning the SQL component
"d_param_load.sql", this DAG implements a Unified Execution Strategy:
1. Runs a PySpark wrapper task on Dataproc (migrated from r_load_params.ksh) to parse and 
   stage configurations, injecting the required legacy 'DWH_JOB_KENNUNG' variable.
2. Runs Dataform operators to compile and execute the parameter-loading SQL model 
   (migrated from d_param_load.sql to Dataform sqlx).

Failure Handling:
If the Dataform pipeline fails, a custom failure callback captures the failure and logs 
the legacy-compatible error output in German character-for-character:
"FEHLER: d_param_load.sql beendet mit RC=1"
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# =============================================================================
# GCP Configuration Constants (GLOBAL Roles)
# Sourced dynamically via Airflow Variables.
# =============================================================================
GCP_PROJECT_ID = Variable.get(