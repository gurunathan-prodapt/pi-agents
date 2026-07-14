# Migration Notes: `h_alis_rest_metaauth` (Perl to Python)

## 1. Summary
The legacy Perl utility module `h_alis_rest_metaauth.pm` has been migrated to a modern, secure, and native Python 3.11 library (`h_alis_rest_metaauth.py`). 

* **Source Platform:** On-premises Data Warehouse (DW) environment running Perl scripts that executed shell-based `curl` subprocesses to query a legacy REST authentication service (`metaAuth`).
* **Target Platform:** Google Cloud Composer (Apache Airflow) / Google Cloud Run.
* **Migration Pattern:** **UC4_ONLY / Cloud Composer (High Confidence)**. The shell-dependent Perl module has been completely re-engineered into a thread-safe Python class (`MetaAuthClient`) that supports a fallback hierarchy across Google Cloud Secret Manager, Airflow Connections, and the legacy REST service.

---

## 2. Generated Artifacts
The following files have been generated to replace the legacy Perl module:

1. **`dags/utils/h_alis_rest_metaauth.py`**
   * *Role:* The core Python utility library. It replaces `h_alis_rest_metaauth.pm` and provides the `MetaAuthClient` class. It implements native gRPC calls to Google Cloud Secret Manager, queries the local Airflow metadata database, or falls back to secure HTTPS requests using the `requests` library.
2. **`dags/utils/test_h_alis_rest_metaauth.py`**
   * *Role:* A comprehensive suite of unit tests using Python's standard `unittest` framework and `unittest.mock`. It validates all resolution paths (Secret Manager, Airflow Connections, and Legacy REST) without requiring live external connections.

---

## 3. Key Design Decisions

* **Elimination of Shell Subprocesses:** The legacy Perl code relied on backticked shell commands (`qx/curl .../`). The Python implementation uses native, thread-safe connection pooling via `requests.Session()` and the official Google Cloud gRPC-based SDK client. This eliminates command injection risks and improves performance.
* **Fallback Authentication Chain (Rule 1):** To support hybrid migration phases, the client implements an automated fallback hierarchy (`auth_method="auto"`):
  1. **Google Cloud Secret Manager** (Primary cloud-native target)
  2. **Airflow Connection Backend** (Secondary cloud-native target)
  3. **Legacy On-Premises REST Endpoint** (Fallback for hybrid operations)
* **Aggressive Token Expiry Warning (Rule 2):** If a retrieved token has an `expires_in` value of 300 seconds (5 minutes) or less, the client logs an aggressive warning. This prevents long-running downstream BigQuery jobs from failing mid-execution due to expired credentials.
* **Environment Isolation (Rule 3):** The client automatically detects the active environment (`DEV`, `STAGE`, `PROD`) via the `ENVIRONMENT` environment variable and appends appropriate suffixes (e.g., `-stage` or `-prod`) to the requested secret ID to prevent cross-environment credential leaks.
* **Graceful Degradation of Imports:** External dependencies (`google.cloud.secretmanager` and `airflow`) are imported within `try-except` blocks. This allows the utility to be imported and run in lightweight, non-Airflow Python environments (such as local developer machines or Cloud Run microservices) without throwing immediate `ModuleNotFoundError` exceptions.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema & Dataset Creation
While this utility does not write to databases, it queries the Airflow metadata database when using the `airflow` resolution method. Ensure that any custom Airflow connections are registered in the Airflow metadata database.

### 4.2 IAM & Permissions
The Cloud Composer environment's worker Service Account must be granted the minimal IAM role required to access secrets:
* **Role:** `roles/secretmanager.secretAccessor`
* **Scope:** Grant this role strictly on the target secrets in Google Cloud Secret Manager rather than project-wide, adhering to the principle of least privilege.

### 4.3 Connection Strings & Secrets
If migrating credentials from the legacy `metaAuth` service to Google Cloud Secret Manager:
1. Create a secret in Secret Manager (e.g., `projects/[PROJECT_ID]/secrets/alis-api-key`).
2. Store the credential payload as a JSON string matching the expected format:
   ```json
   {
     "secret_value": "your-api-token-here",
     "credential_type": "bearer_token",
     "expires_in": 3600
   }
   ```

### 4.4 Environment Variables & Airflow Variables
Configure the following variables in your Cloud Composer environment:
* **Airflow Variables (Preferred):**
  * `GCP_PROJECT`: The target GCP Project ID where Secret Manager is hosted.
  * `RESTMETAAUTH_URI`: The base URL of the legacy REST service (e.g., `https://auth.alis.internal/v1`).
* **OS Environment Variables:**
  * `ENVIRONMENT`: Set to `DEV`, `STAGE`, or `PROD` to enable automatic environment-based secret suffixing.

### 4.5 Scheduling & Packaging
1. Copy `h_alis_rest_metaauth.py` into the `/dags/utils/` directory of your Cloud Composer environment's Google Cloud Storage (GCS) bucket.
2. Ensure downstream DAGs import the client using:
   ```python
   from utils.h_alis_rest_metaauth import MetaAuthClient
   ```

---

## 5. Known Gaps & Unresolved References

* **Downstream Pipeline Integration (`B4` Redesign Item):**
  * The downstream consumer pipeline **`DW.DWH_OAIS_EX_PPES_CUBES`** has not yet been migrated. 
  * *Action Required:* Once the downstream pipeline is migrated to Cloud Composer, its DAG tasks must be refactored to instantiate `MetaAuthClient` and replace any legacy shell-based credential retrieval steps.
* **Hybrid Network Routing:**
  * If the client is configured to use the legacy REST endpoint (`auth_method="rest"` or via fallback), Cloud Composer must have established network routes (via Cloud VPN or Interconnect) to access the on-premises `metaAuth` service.

---

## 6. Validation

### 6.1 Running Unit Tests
Run the provided unit tests using the following command in your terminal:
```bash
python -m unittest dags/utils/test_h_alis_rest_metaauth.py
```

### 6.2 What "Passing" Means
A successful validation run must output:
```text
...
----------------------------------------------------------------------
Ran 4 tests in 0.015s

OK
```
This confirms that:
1. Secret Manager JSON payloads are correctly parsed.
2. Airflow Connection parameters are successfully extracted.
3. Legacy REST HTTP requests are correctly dispatched and handled.
4. Network timeouts and connection failures raise the custom `MetaAuthException` as expected.

---

## 7. Rollback Procedure

If issues are encountered with the migrated Python utility in production:

1. **Fallback to Legacy REST:**
   If Secret Manager or Airflow Connection lookups fail, force the client to use the legacy on-premises REST service by passing `auth_method="rest"` during instantiation:
   ```python
   client = MetaAuthClient()
   credentials = client.get_credentials("secret-id", auth_method="rest")
   ```
2. **Revert DAG Imports:**
   If a complete rollback of the pipeline is required, revert the downstream DAG code to its previous state and redeploy the legacy Perl execution tasks within your hybrid orchestration environment.