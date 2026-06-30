"""
h_alis_sqlplus.py

Migration helper for the legacy KSH utility `h_alis_sqlplus.ksh`.

This module provides two cloud-native execution patterns for Google Cloud BigQuery:

Option A:
- Python orchestration helper for Airflow / Composer / custom runners.
- Validates inputs, checks script availability in GCS or local filesystem,
  logs execution details, and executes BigQuery SQL.

Option B:
- Registry-driven execution helper that can be used by orchestration code
  to resolve a logical script name to SQL text stored in BigQuery or GCS.

The code is modular and reusable so it can be adapted to:
- Cloud Composer / Apache Airflow DAG tasks
- Cloud Run jobs
- Custom Python batch runners
- Unit tests with mocked BigQuery / storage clients
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

try:
    from google.cloud import bigquery
    from google.cloud import storage
except Exception:  # pragma: no cover - allows unit testing without GCP libs
    bigquery = None
    storage = None


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Legacy-compatible return codes
ERR_MISSING_ARGUMENTS = 196
ERR_SCRIPT_NOT_FOUND = 201
ERR_EXECUTION_FAILED = 1
SUCCESS = 0


@dataclass(frozen=True)
class ExecutionContext:
    """
    Execution metadata comparable to the legacy environment variables.
    """
    module_name: str = "h_alis_sqlplus"
    module_version: str = "1.1.3"
    gcp_project_id: Optional[str] = "gcp-is-dw-dev"
    bq_utility_dataset: Optional[str] = "dw_utility_dev"
    bq_logging_dataset: Optional[str] = "dw_audit_dev"
    gcs_sql_bucket: Optional[str] = "gcp-is-dw-dev-sql-scripts"


@dataclass(frozen=True)
class ScriptReference:
    """
    Logical script reference used by the registry-driven approach.
    """
    script_name: str
    script_sql: Optional[str] = None
    is_active: bool = True


class ScriptExecutionError(Exception):
    """Base exception for script execution failures."""


class MissingArgumentsError(ScriptExecutionError):
    """Raised when required arguments are missing."""


class ScriptNotFoundError(ScriptExecutionError):
    """Raised when a script cannot be resolved."""


class ScriptRegistry:
    """
    Registry abstraction for resolving logical script names to SQL text.

    Supports:
    - BigQuery table-backed registry
    - In-memory registry for tests
    - Custom implementations via subclassing
    """

    def resolve(self, script_name: str) -> ScriptReference:
        raise NotImplementedError


class InMemoryScriptRegistry(ScriptRegistry):
    """
    Simple registry for tests or small deployments.
    """

    def __init__(self, scripts: Sequence[ScriptReference]):
        self._scripts = {s.script_name: s for s in scripts if s.is_active}

    def resolve(self, script_name: str) -> ScriptReference:
        ref = self._scripts.get(script_name)
        if not ref:
            raise ScriptNotFoundError(f"Script not found or inactive: {script_name}")
        return ref


class BigQueryTableScriptRegistry(ScriptRegistry):
    """
    BigQuery-backed registry.

    Expected schema:
      project.dataset.sql_script_registry(
        script_name STRING,
        script_sql STRING,
        is_active BOOL,
        last_modified TIMESTAMP
      )
    """

    def __init__(self, client: Any, table_fqn: str):
        self.client = client
        self.table_fqn = table_fqn

    def resolve(self, script_name: str) -> ScriptReference:
        query = f"""
            SELECT script_name, script_sql, is_active
            FROM `{self.table_fqn}`
            WHERE script_name = @script_name
              AND is_active = TRUE
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("script_name", "STRING", script_name)
            ]
        )
        rows = list(self.client.query(query, job_config=job_config).result())
        if not rows:
            raise ScriptNotFoundError(f"Script not found or inactive: {script_name}")

        row = rows[0]
        return ScriptReference(
            script_name=row["script_name"],
            script_sql=row["script_sql"],
            is_active=bool(row["is_active"]),
        )


def get_default_context() -> ExecutionContext:
    """
    Build execution context from environment variables with safe defaults.
    """
    return ExecutionContext(
        module_name=os.getenv("MODUL_NAME", "h_alis_sqlplus"),
        module_version=os.getenv("MODUL_VERSION", "1.1.3"),
        gcp_project_id=os.getenv("GCP_PROJECT_ID", "gcp-is-dw-dev"),
        bq_utility_dataset=os.getenv("BQ_UTILITY_DATASET", "dw_utility_dev"),
        bq_logging_dataset=os.getenv("BQ_LOGGING_DATASET", "dw_audit_dev"),
        gcs_sql_bucket=os.getenv("GCS_SQL_BUCKET", "gcp-is-dw-dev-sql-scripts"),
    )


def log_error(entry_no: Optional[int], code: int, message: str, context: ExecutionContext) -> None:
    """
    Replacement for DWMSG_MeldeFehler.

    Logs to standard output/logger and can be extended to execute BigQuery DML
    inserts against the table `{gcp_project_id}.{bq_logging_dataset}.sql_execution_audit`.
    """
    LOGGER.error(
        "Fehler %s | code=%s | module=%s | version=%s | message=%s",
        entry_no,
        code,
        context.module_name,
        context.module_version,
        message,
    )


def log_info(message: str, context: ExecutionContext) -> None:
    """
    Structured info logging.
    """
    LOGGER.info(
        "%s | module=%s | version=%s",
        message,
        context.module_name,
        context.module_version,
    )


def validate_required_arguments(entry_no: Optional[int], script_name: Optional[str]) -> None:
    """
    Validate required inputs.

    Mirrors legacy behavior:
    - missing entry number or script name => return code 196
    """
    if entry_no is None or not script_name:
        raise MissingArgumentsError("Missing required arguments: entry number or script name")


def normalize_script_name(script_name: str) -> str:
    """
    Normalize script identifiers for registry lookup or path handling.
    """
    return script_name.strip()


def is_local_file_readable(path: str) -> bool:
    """
    Local filesystem readability check.
    """
    p = Path(path)
    return p.is_file() and os.access(p, os.R_OK)


def is_gcs_object_readable(bucket_name: str, blob_name: str, client: Any) -> bool:
    """
    GCS object existence/readability check.
    """
    if storage is None:
        raise RuntimeError("google-cloud-storage is not available")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists(client)


def read_sql_from_local_file(path: str) -> str:
    """
    Read SQL text from a local file.
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def read_sql_from_gcs(bucket_name: str, blob_name: str, client: Any) -> str:
    """
    Read SQL text from GCS.
    """
    if storage is None:
        raise RuntimeError("google-cloud-storage is not available")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text(encoding="utf-8")


def substitute_positional_parameters(sql_text: str, params: Sequence[str]) -> str:
    """
    Safe positional placeholder substitution.

    Supports template replacement targets:
      ${1}, ${2}, ${3}, ...
    """
    result = sql_text
    for idx, value in enumerate(params, start=1):
        placeholder = "${" + str(idx) + "}"
        result = result.replace(placeholder, value)
    return result


def build_parameter_string(params: Sequence[str]) -> str:
    """
    Build a legacy-style parameter string for logging.
    """
    return " ".join(params) if params else ""


def execute_bigquery_sql(
    sql_text: str,
    client: Any,
    job_config: Optional[Any] = None,
) -> None:
    """
    Execute SQL in BigQuery.
    """
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is not available")
    client.query(sql_text, job_config=job_config).result()


def execute_script_from_registry(
    entry_no: Optional[int],
    script_name: str,
    params: Sequence[str],
    registry: ScriptRegistry,
    context: Optional[ExecutionContext] = None,
    bq_client: Optional[Any] = None,
) -> int:
    """
    Option B Execution Pattern:
    Resolve a logical script name from a database-backed registry and execute it.

    Returns legacy-compatible codes:
    - 196 missing arguments
    - 201 script not found
    - 0 success
    - 1 execution failure
    """
    context = context or get_default_context()

    try:
        validate_required_arguments(entry_no, script_name)
    except MissingArgumentsError:
        log_error(entry_no, ERR_MISSING_ARGUMENTS, "Missing required arguments", context)
        return ERR_MISSING_ARGUMENTS

    script_name = normalize_script_name(script_name)

    try:
        ref = registry.resolve(script_name)
    except ScriptNotFoundError:
        log_error(entry_no, ERR_SCRIPT_NOT_FOUND, script_name, context)
        return ERR_SCRIPT_NOT_FOUND

    if not ref.script_sql:
        log_error(entry_no, ERR_SCRIPT_NOT_FOUND, f"Empty SQL for script: {script_name}", context)
        return ERR_SCRIPT_NOT_FOUND

    log_info("Rufe SQL*PLUS auf mit folgenden Einstellungen", context)
    log_info(f"Sql*Plus-Skript : {script_name}", context)
    log_info(f"Skript-Parameter: {build_parameter_string(params)}", context)

    sql_text = substitute_positional_parameters(ref.script_sql, params)

    try:
        if bq_client is None:
            raise RuntimeError("BigQuery client is required for execution")
        execute_bigquery_sql(sql_text, bq_client)
        return SUCCESS
    except Exception as exc:
        log_error(entry_no, ERR_EXECUTION_FAILED, str(exc), context)
        return ERR_EXECUTION_FAILED


def execute_script_from_local_path(
    entry_no: Optional[int],
    script_path: str,
    params: Sequence[str],
    bq_client: Any,
    context: Optional[ExecutionContext] = None,
) -> int:
    """
    Option A Execution Pattern:
    Execute a standard SQL file loaded from the local filesystem.

    Useful for container deployment or local developer workstations.
    """
    context = context or get_default_context()

    try:
        validate_required_arguments(entry_no, script_path)
    except MissingArgumentsError:
        log_error(entry_no, ERR_MISSING_ARGUMENTS, "Missing required arguments", context)
        return ERR_MISSING_ARGUMENTS

    if not is_local_file_readable(script_path):
        log_error(entry_no, ERR_SCRIPT_NOT_FOUND, script_path, context)
        return ERR_SCRIPT_NOT_FOUND

    log_info("Rufe SQL*PLUS auf mit folgenden Einstellungen", context)
    log_info(f"Sql*Plus-Skript : {script_path}", context)
    log_info(f"Skript-Parameter: {build_parameter_string(params)}", context)

    try:
        sql_text = read_sql_from_local_file(script_path)
        sql_text = substitute_positional_parameters(sql_text, params)
        execute_bigquery_sql(sql_text, bq_client)
        return SUCCESS
    except Exception as exc:
        log_error(entry_no, ERR_EXECUTION_FAILED, str(exc), context)
        return ERR_EXECUTION_FAILED


def execute_script_from_gcs(
    entry_no: Optional[int],
    gcs_uri: str,
    params: Sequence[str],
    bq_client: Any,
    gcs_client: Any,
    context: Optional[ExecutionContext] = None,
) -> int:
    """
    Option A variant (GCS Based Execution):
    Download a SQL script from a GCS cloud storage bucket and execute it.

    gcs_uri format:
      gs://bucket/path/to/script.sql
    """
    context = context or get_default_context()

    try:
        validate_required_arguments(entry_no, gcs_uri)
    except MissingArgumentsError:
        log_error(entry_no, ERR_MISSING_ARGUMENTS, "Missing required arguments", context)
        return ERR_MISSING_ARGUMENTS

    match = re.match(r"^gs://([^/]+)/(.+)$", gcs_uri.strip())
    if not match:
        log_error(entry_no, ERR_SCRIPT_NOT_FOUND, f"Invalid GCS URI: {gcs_uri}", context)
        return ERR_SCRIPT_NOT_FOUND

    bucket_name, blob_name = match.group(1), match.group(2)

    if not is_gcs_object_readable(bucket_name, blob_name, gcs_client):
        log_error(entry_no, ERR_SCRIPT_NOT_FOUND, gcs_uri, context)
        return ERR_SCRIPT_NOT_FOUND

    log_info("Rufe SQL*PLUS auf mit folgenden Einstellungen", context)
    log_info(f"Sql*Plus-Skript : {gcs_uri}", context)
    log_info(f"Skript-Parameter: {build_parameter_string(params)}", context)

    try:
        sql_text = read_sql_from_gcs(bucket_name, blob_name, gcs_client)
        sql_text = substitute_positional_parameters(sql_text, params)
        execute_bigquery_sql(sql_text, bq_client)
        return SUCCESS
    except Exception as exc:
        log_error(entry_no, ERR_EXECUTION_FAILED, str(exc), context)
        return ERR_EXECUTION_FAILED


def build_registry_table_ddl(project_id: str, dataset_id: str) -> str:
    """
    Generate the BigQuery DDL for the script registry table.
    """
    return f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.sql_script_registry` (
  script_name STRING OPTIONS(description="The logical name of the script equivalent"),
  script_sql STRING OPTIONS(description="The complete BQSQL code or stored procedure invoke command"),
  is_active BOOL OPTIONS(description="Active execution flag"),
  last_modified TIMESTAMP
);
""".strip()


def build_stored_procedure_ddl(project_id: str, dataset_id: str) -> str:
    """
    Generate a BigQuery stored procedure equivalent for starteSQLSkript.
    """
    return f"""
CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.starteSQLSkript`(
  p_Eintragsnr INT64,
  p_Skript STRING,
  p_Parameter ARRAY<STRING>
)
BEGIN
  DECLARE errcode INT64 DEFAULT 0;
  DECLARE v_script_exists BOOL DEFAULT FALSE;
  DECLARE v_sql STRING DEFAULT '';
  DECLARE v_params STRING DEFAULT '';
  DECLARE v_msg STRING DEFAULT '';

  IF p_Eintragsnr IS NULL OR p_Skript IS NULL OR p_Skript = '' THEN
    SET errcode = 196;
    SELECT errcode AS return_code;
    RETURN;
  END IF;

  SET v_script_exists = EXISTS (
    SELECT 1
    FROM `{project_id}.{dataset_id}.sql_script_registry`
    WHERE script_name = p_Skript
      AND is_active = TRUE
  );

  IF NOT v_script_exists THEN
    SET errcode = 201;
    SELECT errcode AS return_code;
    RETURN;
  END IF;

  SET v_params = (
    SELECT STRING_AGG(param, ' ')
    FROM UNNEST(p_Parameter) AS param
  );

  SELECT 'Rufe SQL*PLUS auf mit folgenden Einstellungen' AS info_message;
  SELECT CONCAT('Sql*Plus-Skript : ', p_Skript) AS info_message;
  SELECT CONCAT('Skript-Parameter: ', IFNULL(v_params, '')) AS info_message;

  SET v_sql = (
    SELECT script_sql
    FROM `{project_id}.{dataset_id}.sql_script_registry`
    WHERE script_name = p_Skript
      AND is_active = TRUE
    LIMIT 1
  );

  BEGIN
    EXECUTE IMMEDIATE v_sql;
    SET errcode = 0;
  EXCEPTION WHEN ERROR THEN
    SET errcode = 1;
    SELECT CONCAT('Execution failed for script: ', p_Skript) AS error_message;
  END;

  SELECT errcode AS return_code;
END;
""".strip()


def main() -> int:
    """
    Standalone initialization.
    """
    context = get_default_context()
    LOGGER.info("Module %s version %s loaded successfully.", context.module_name, context.module_version)
    return SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())