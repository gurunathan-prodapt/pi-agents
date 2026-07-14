Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Python utility `h_alis_rest_metaauth.py` is behaviorally equivalent to the legacy Perl module `h_alis_rest_metaauth.pm`, while validating its new cloud-native capabilities.

---

# Migration Validation Test Suite: `h_alis_rest_metaauth`

## 1. Output Parity & Interface Equivalence

### Purpose
To prove that the migrated Python client produces the exact same output values as the legacy Perl module when given identical inputs under the legacy REST execution path.

### Setup
* **Legacy Environment:** A running instance of the legacy Perl environment with `h_alis_rest_metaauth.pm` loaded, pointing to a mock REST service.
* **Target Environment:** A Python 3.11+ environment with `h_alis_rest_metaauth.py` loaded, pointing to the same mock REST service.
* **Mock REST Service:** A mock HTTP server running on `http://localhost:8080/metaAuth/rest` that responds to:
  * **Perl GET request:** `GET /password/PROD_ENV/db_user` $\rightarrow$ returns raw string `P@ssword123!`
  * **Python POST request:** `POST /auth/token` with payload `{"resource": "db_user-prod"}` $\rightarrow$ returns JSON:
    ```json
    {
      "secret_value": "P@ssword123!",
      "credential_type": "bearer_token",
      "expires_in": 3600
    }
    ```

### Action
1. Execute the legacy Perl function:
   ```perl
   use h_alis_rest_metaauth;
   my $legacy_pass = restmetaauth_getpassword("PROD_ENV", "db_user");
   print "LECO_OUT:$legacy_pass\n";
   ```
2. Execute the migrated Python client with environment isolation active (`ENVIRONMENT=PROD`):
   ```python
   import os
   os.environ["ENVIRONMENT"] = "PROD"
   from h_alis_rest_metaauth import MetaAuthClient
   
   client = MetaAuthClient(rest_endpoint="http://localhost:8080/metaAuth/rest")
   result = client.get_credentials("db_user", auth_method="rest")
   print(f"PY_OUT:{result['secret_value']}")
   ```

### Pass/Fail Criterion
* **Pass:** The extracted string from `LECO_OUT` matches `PY_OUT` exactly (`P@ssword123!`), and the Python execution completes without throwing any exceptions.
* **Fail:** Any mismatch in the retrieved password string, or a failure to connect/parse the response.

---

## 2. Transformation Correctness & Business Rules

### Purpose
To validate the correct execution of the three core business rules:
1. **Rule 1:** Fallback Authentication Chain (`auto` mode hierarchy).
2. **Rule 2:** Token Expiry and Refresh Warning (aggressive warning threshold $\le 300$ seconds).
3. **Rule 3:** Environment Isolation (automatic suffixing based on environment variables).

### Setup
A Python test environment using `pytest` and `pytest-mock`.

### Action
Run the following automated test suite:

```python
import os
import logging
import pytest
from h_alis_rest_metaauth import MetaAuthClient, MetaAuthException

# --- Rule 3: Environment Isolation Tests ---

@pytest.mark.parametrize("env_val, input_id, expected_id", [
    ("PROD", "my-secret", "my-secret-prod"),
    ("PRODUCTION", "my-secret", "my-secret-prod"),
    ("STAGE", "my-secret", "my-secret-stage"),
    ("DEV", "my-secret", "my-secret"),
    ("PROD", "my-secret-prod", "my-secret-prod"), # Should not double-suffix
])
def test_rule_3_environment_isolation(env_val, input_id, expected_id, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", env_val)
    client = MetaAuthClient()
    assert client._apply_environment_isolation(input_id) == expected_id


# --- Rule 2: Token Expiry Warning Tests ---

def test_rule_2_token_expiry_warning(caplog):
    client = MetaAuthClient()
    
    # Case A: Token has 301 seconds remaining (No warning)
    with caplog.at_level(logging.WARNING):
        client._apply_token_expiration_policy({"expires_in": 301})
    assert "Aggressive refresh is recommended" not in caplog.text

    # Case B: Token has 300 seconds remaining (Warning triggered)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        client._apply_token_expiration_policy({"expires_in": 300})
    assert "Aggressive refresh is recommended" in caplog.text


# --- Rule 1: Fallback Authentication Chain Tests ---

def test_rule_1_fallback_chain_success_first_step(mocker):
    """Should return immediately if Secret Manager succeeds."""
    client = MetaAuthClient()
    
    mock_sm = mocker.patch.object(client, "_get_from_secret_manager", return_value={"status": "SUCCESS", "source": "google_secret_manager"})
    mock_af = mocker.patch.object(client, "_get_from_airflow_connection")
    mock_rest = mocker.patch.object(client, "_get_from_legacy_rest")

    res = client.get_credentials("test-secret", auth_method="auto")
    
    assert res["source"] == "google_secret_manager"
    mock_sm.assert_called_once()
    mock_af.assert_not_called()
    mock_rest.assert_not_called()


def test_rule_1_fallback_chain_degrades_to_rest(mocker):
    """Should fall back to REST if Secret Manager and Airflow both fail."""
    client = MetaAuthClient()
    
    mocker.patch.object(client, "_get_from_secret_manager", side_effect=Exception("SM Failed"))
    mocker.patch.object(client, "_get_from_airflow_connection", side_effect=Exception("Airflow Failed"))
    mock_rest = mocker.patch.object(client, "_get_from_legacy_rest", return_value={"status": "SUCCESS", "source": "legacy_rest_api"})

    res = client.get_credentials("test-secret", auth_method="auto")
    
    assert res["source"] == "legacy_rest_api"
    mock_rest.assert_called_once()
```

### Pass/Fail Criterion
* **Pass:** All assertions pass, proving environment suffixes are correctly appended, warnings are logged at $\le 300$ seconds, and the fallback chain degrades gracefully.
* **Fail:** Any assertion fails or an unexpected exception is raised.

---

## 3. External System Replacements & Integration

### Purpose
To verify that the client correctly interfaces with external systems: Google Cloud Secret Manager (via gRPC), Airflow Connection Backends, and Legacy REST endpoints (enforcing SSL/TLS verification).

### Setup
* Mock the underlying gRPC client for Google Secret Manager.
* Mock the Airflow `BaseHook` database connector.
* Mock the `requests.Session` to verify SSL verification parameters.

### Action
Run the following integration-mock tests:

```python
import pytest
from unittest.mock import MagicMock
import requests
from h_alis_rest_metaauth import MetaAuthClient, MetaAuthException

# --- Google Secret Manager Integration ---

def test_gcp_secret_manager_integration(mocker):
    # Mock the library availability and client
    mocker.patch("h_alis_rest_metaauth.HAS_SECRET_MANAGER", True)
    mock_sm_client = mocker.patch("google.cloud.secretmanager.SecretManagerServiceClient")
    
    mock_instance = MagicMock()
    mock_sm_client.return_value = mock_instance
    
    # Mock response payload
    mock_response = MagicMock()
    mock_response.payload.data = b'{"secret_value": "gcp-secret", "credential_type": "oauth_token"}'
    mock_instance.access_secret_version.return_value = mock_response

    client = MetaAuthClient(gcp_project_id="test-project")
    result = client.get_credentials("my-key", auth_method="secret_manager")

    assert result["source"] == "google_secret_manager"
    assert result["secret_value"] == "gcp-secret"
    mock_instance.access_secret_version.assert_called_once_with(
        request={"name": "projects/test-project/secrets/my-key-prod/versions/latest"}
    )


# --- Airflow Connection Integration ---

def test_airflow_connection_integration(mocker):
    mocker.patch("h_alis_rest_metaauth.HAS_AIRFLOW", True)
    mock_base_hook = mocker.patch("airflow.hooks.base.BaseHook")
    
    mock_connection = MagicMock()
    mock_connection.login = "sa_user"
    mock_connection.get_password.return_value = "airflow-pass"
    mock_connection.extra_dejson = {"credential_type": "service_account"}
    mock_base_hook.get_connection.return_value = mock_connection

    client = MetaAuthClient()
    result = client.get_credentials("conn-id", auth_method="airflow")

    assert result["source"] == "airflow_connection"
    assert result["secret_value"] == "airflow-pass"
    assert result["login"] == "sa_user"


# --- Legacy REST SSL Verification ---

def test_legacy_rest_ssl_enforcement(mocker):
    client = MetaAuthClient(rest_endpoint="https://secure-auth.internal")
    mock_post = mocker.patch.object(client.session, "post")
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"secret_value": "token123"}
    mock_post.return_value = mock_response

    client.get_credentials("legacy-key", auth_method="rest")
    
    # Verify that verify=True was explicitly passed to the post call
    _, kwargs = mock_post.call_args
    assert kwargs["verify"] is True
```

### Pass/Fail Criterion
* **Pass:** 
  * Secret Manager client requests the correct resource path (`projects/test-project/secrets/my-key-prod/versions/latest`).
  * Airflow connection extracts both credentials and JSON metadata.
  * Legacy REST client enforces strict SSL verification (`verify=True`).
* **Fail:** Any external call bypasses security parameters or fails to parse standard payloads.

---

## 4. Data Quality, Schema, & Exception Assertions

### Purpose
To verify that the returned payload strictly adheres to the designed schema, and that all network, permission, or parsing failures are safely caught and re-raised as a unified `MetaAuthException`.

### Setup
A Python test environment.

### Action
Run the following schema validation and error-handling tests:

```python
import pytest
import requests
from h_alis_rest_metaauth import MetaAuthClient, MetaAuthException

# --- Schema Validation ---

def test_output_schema_conformance(mocker):
    client = MetaAuthClient()
    mocker.patch.object(client, "_get_from_legacy_rest", return_value={
        "status": "SUCCESS",
        "source": "legacy_rest_api",
        "credential_type": "bearer_token",
        "secret_value": "token_val",
        "expires_in": 3600,
        "retrieved_at": "2023-10-27T10:14:00Z"
    })

    result = client.get_credentials("test-secret", auth_method="rest")
    
    # Assert exact schema structure and types
    assert isinstance(result, dict)
    assert result["status"] == "SUCCESS"
    assert result["source"] == "legacy_rest_api"
    assert isinstance(result["credential_type"], str)
    assert isinstance(result["secret_value"], str)
    assert isinstance(result["expires_in"], int)
    assert result["retrieved_at"].endswith("Z")


# --- Exception Handling Assertions ---

def test_rest_timeout_raises_meta_auth_exception(mocker):
    client = MetaAuthClient()
    mocker.patch.object(
        client.session, "post", 
        side_effect=requests.exceptions.Timeout("Connection timed out")
    )

    with pytest.raises(MetaAuthException) as exc_info:
        client.get_credentials("test-secret", auth_method="rest")
    assert "Connection timed out" in str(exc_info.value)


def test_invalid_json_raises_meta_auth_exception(mocker):
    client = MetaAuthClient()
    
    # Mock a 502 Bad Gateway HTML response
    mock_response = mocker.MagicMock()
    mock_response.status_code = 502
    mock_response.json.side_effect = ValueError("No JSON object could be decoded")
    mocker.patch.object(client.session, "post", return_value=mock_response)

    with pytest.raises(MetaAuthException):
        client.get_credentials("test-secret", auth_method="rest")
```

### Pass/Fail Criterion
* **Pass:** 
  * Successful credential retrievals match the output dictionary schema exactly.
  * Network timeouts and invalid JSON payloads are successfully caught and wrapped in a `MetaAuthException`.
* **Fail:** The client leaks raw library exceptions (e.g., `requests.exceptions.Timeout` or `JSONDecodeError`) to the caller, or returns a malformed dictionary.