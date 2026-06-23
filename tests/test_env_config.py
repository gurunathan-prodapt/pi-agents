# Legacy Source: .dw_init (environment variables) for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
Unit tests for the env_config.py module.
"""

import unittest
import os
from utils.env_config import EnvConfig

class TestEnvConfig(unittest.TestCase):

    def setUp(self):
        # Store original environment variables to restore them after tests
        self._original_bert_dir_root = os.environ.get("BERT_DIR_ROOT")
        self._original_gcp_project_id = os.environ.get("GCP_PROJECT_ID")
        self._original_dw_dir_root = os.environ.get("DW_DIR_ROOT")

        # Clear/set specific env vars for testing
        if "BERT_DIR_ROOT" in os.environ:
            del os.environ["BERT_DIR_ROOT"]
        if "GCP_PROJECT_ID" in os.environ:
            del os.environ["GCP_PROJECT_ID"]
        if "DW_DIR_ROOT" in os.environ:
            del os.environ["DW_DIR_ROOT"]


    def tearDown(self):
        # Restore original environment variables
        if self._original_bert_dir_root is not None:
            os.environ["BERT_DIR_ROOT"] = self._original_bert_dir_root
        elif "BERT_DIR_ROOT" in os.environ:
            del os.environ["BERT_DIR_ROOT"]

        if self._original_gcp_project_id is not None:
            os.environ["GCP_PROJECT_ID"] = self._original_gcp_project_id
        elif "GCP_PROJECT_ID" in os.environ:
            del os.environ["GCP_PROJECT_ID"]
        
        if self._original_dw_dir_root is not None:
            os.environ["DW_DIR_ROOT"] = self._original_dw_dir_root
        elif "DW_DIR_ROOT" in os.environ:
            del os.environ["DW_DIR_ROOT"]

        # Reset any dynamic changes to EnvConfig class attributes
        EnvConfig.BERT_DIR_ROOT = os.getenv("BERT_DIR_ROOT", "/vobs/dw_source/isrpt/isbert")
        EnvConfig.GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "<your-gcp-project-id>")
        EnvConfig.DW_DIR_ROOT = os.getenv("DW_DIR_ROOT", "/vobs/dw_source/isrpt/isbert")


    def test_default_values(self):
        self.assertEqual(EnvConfig.BERT_DIR_ROOT, "/vobs/dw_source/isrpt/isbert")
        self.assertEqual(EnvConfig.GCP_PROJECT_ID, "<your-gcp-project-id>")
        self.assertEqual(EnvConfig.DW_DIR_ROOT, "/vobs/dw_source/isrpt/isbert")
        self.assertEqual(EnvConfig.DW_DIR_PROT, "/tmp/logs")

    def test_environment_variable_override(self):
        os.environ["BERT_DIR_ROOT"] = "/custom/bert/root"
        os.environ["GCP_PROJECT_ID"] = "my-test-project"
        os.environ["DW_DIR_ROOT"] = "/custom/dw/root"

        # Re-initialize EnvConfig values based on new env vars
        EnvConfig.BERT_DIR_ROOT = os.getenv("BERT_DIR_ROOT", "/vobs/dw_source/isrpt/isbert")
        EnvConfig.GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "<your-gcp-project-id>")
        EnvConfig.DW_DIR_ROOT = os.getenv("DW_DIR_ROOT", "/vobs/dw_source/isrpt/isbert")


        self.assertEqual(EnvConfig.BERT_DIR_ROOT, "/custom/bert/root")
        self.assertEqual(EnvConfig.GCP_PROJECT_ID, "my-test-project")
        self.assertEqual(EnvConfig.DW_DIR_ROOT, "/custom/dw/root")

    def test_get_config(self):
        self.assertEqual(EnvConfig.get_config("DW_ORAUSER"), "default_oracle_user")
        self.assertEqual(EnvConfig.get_config("NON_EXISTENT_KEY", "default_val"), "default_val")

    def test_set_config(self):
        EnvConfig.set_config("NEW_SETTING", "new_value")
        self.assertEqual(EnvConfig.NEW_SETTING, "new_value")
        self.assertEqual(EnvConfig.get_config("NEW_SETTING"), "new_value")

if __name__ == "__main__":
    unittest.main()