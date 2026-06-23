# Legacy Source: f_alis_msgerr.ksh for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
Unit tests for the logging_utils.py module.
These tests verify that logging functions are called correctly
and exceptions are raised as expected.
"""

import unittest
import logging
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

from utils.logging_utils import (
    dwmsg_ermittle_nr, dwmsg_erzeuge_eintrag, dwmsg_setze_status_ok,
    dwmsg_setze_status_abbruch, dwmsg_melde_fehler, dwmsg_logdateiname,
    DWMSGError, FATAL, ERROR, WARNING, INFO
)
from utils.env_config import EnvConfig

class TestLoggingUtils(unittest.TestCase):

    def setUp(self):
        # Capture logs for assertions
        self.logger = logging.getLogger('utils.logging_utils')
        self.logger.setLevel(logging.DEBUG)
        self.log_stream = MagicMock()
        self.handler = logging.StreamHandler(self.log_stream)
        self.logger.addHandler(self.handler)
        self.addCleanup(self.logger.removeHandler, self.handler)
        self.log_messages = []
        self.handler.emit = lambda record: self.log_messages.append(record)

        # Ensure EnvConfig is set for dwmsg_logdateiname
        EnvConfig.DW_DIR_PROT = "/test/logs"
        self._original_dw_dir_prot = os.environ.get("DW_DIR_PROT")
        if "DW_DIR_PROT" in os.environ:
            del os.environ["DW_DIR_PROT"]

    def tearDown(self):
        if self._original_dw_dir_prot is not None:
            os.environ["DW_DIR_PROT"] = self._original_dw_dir_prot
        elif "DW_DIR_PROT" in os.environ:
            del os.environ["DW_DIR_PROT"]
        EnvConfig.DW_DIR_PROT = "/tmp/logs"


    def assertLogContains(self, level, message_part, exact_match=False):
        found = False
        for record in self.log_messages:
            if record.levelname == level and (message_part in record.message if not exact_match else record.message == message_part):
                found = True
                break
        self.assertTrue(found, f"Log message with level {level} and part '{message_part}' not found. All logs: {self.log_messages}")

    def test_dwmsg_ermittle_nr(self):
        entry_nr = dwmsg_ermittle_nr()
        self.assertIsNotNone(entry_nr)
        self.assertIsInstance(entry_nr, str)
        self.assertLogContains("DEBUG", "Generated new entry number")

    def test_dwmsg_erzeuge_eintrag(self):
        entry_nr = "123"
        job_id = "test_job"
        program_name = "test_prog.py"
        log_file = "/path/to/log.log"
        dwmsg_erzeuge_eintrag(entry_nr, job_id, program_name, log_file)
        self.assertLogContains("INFO", "New job entry created")
        self.assertLogContains("INFO", f"Program 'test_prog.py' with JobID 'test_job' and LogFile '/path/to/log.log'")

    def test_dwmsg_setze_status_ok(self):
        entry_nr = "456"
        dwmsg_setze_status_ok(entry_nr)
        self.assertLogContains("INFO", f"Job completed successfully (EntryNr: {entry_nr}).")

    def test_dwmsg_setze_status_abbruch(self):
        entry_nr = "789"
        dwmsg_setze_status_abbruch(entry_nr)
        self.assertLogContains("ERROR", f"Job aborted (EntryNr: {entry_nr}).")

    def test_dwmsg_melde_fehler_warning(self):
        entry_nr = "101"
        dwmsg_melde_fehler(entry_nr, WARNING, 100, "Minor issue")
        self.assertLogContains("WARNING", "Reporting error 100 (Level: W), Info1: 'Minor issue'")

    def test_dwmsg_melde_fehler_error(self):
        entry_nr = "102"
        dwmsg_melde_fehler(entry_nr, ERROR, 200, "Major issue", "Resolution needed")
        self.assertLogContains("ERROR", "Reporting error 200 (Level: E), Info1: 'Major issue', Info2: 'Resolution needed'")

    def test_dwmsg_melde_fehler_fatal_raises_exception(self):
        entry_nr = "103"
        with self.assertRaises(DWMSGError) as cm:
            dwmsg_melde_fehler(entry_nr, FATAL, 300, "Fatal problem")
        self.assertEqual(cm.exception.error_code, 300)
        self.assertLogContains("CRITICAL", "Reporting error 300 (Level: F), Info1: 'Fatal problem'")

    @patch('utils.logging_utils.datetime')
    def test_dwmsg_logdateiname(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 15, 10, 30)
        mock_datetime.strftime.side_effect = lambda *args, **kw: datetime(2023, 1, 15, 10, 30).strftime(*args, **kw)
        
        job_id = "MYJOB"
        entry_nr = "XYZ"
        expected_filename = "/test/logs/MYJOB_20230115_1030_XYZ.log"
        actual_filename = dwmsg_logdateiname(job_id, entry_nr)
        self.assertEqual(actual_filename, expected_filename)
        self.assertLogContains("DEBUG", f"Generated log filename: {expected_filename}")

    @patch('utils.logging_utils.datetime')
    def test_dwmsg_append_timing_infos(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 15, 11, 0)
        mock_datetime.strftime.side_effect = lambda fmt: datetime(2023, 1, 15, 11, 0).strftime(fmt)

        entry_nr = "987"
        info_text = "Process step 1"
        date_format = "%H:%M:%S"
        dwmsg_append_timing_infos(entry_nr, info_text, date_format)
        self.assertLogContains("INFO", "Appending timing info for EntryNr 987: 'Process step 1 11:00:00'")

    def test_dwmsg_append_timing_infos_missing_params(self):
        with self.assertRaises(DWMSGError) as cm:
            dwmsg_append_timing_infos("123", "info", None)
        self.assertIn("Missing parameters", str(cm.exception))
        self.assertLogContains("ERROR", "Missing parameters for dwmsg_append_timing_infos.")

    def test_dwmsg_setze_stichtag_info(self):
        entry_nr = "654"
        stichtag = "20230101"
        stichtag_fmt = "%Y%m%d"
        dwmsg_setze_stichtag_info(entry_nr, stichtag, stichtag_fmt)
        self.assertLogContains("INFO", "Setting Stichtag info for EntryNr 654: Date='20230101', Format='%Y%m%d'")

    def test_dwmsg_setze_stichtag_info_missing_params(self):
        with self.assertRaises(DWMSGError) as cm:
            dwmsg_setze_stichtag_info("123", None, "%Y%m%d")
        self.assertIn("Missing parameters", str(cm.exception))
        self.assertLogContains("ERROR", "Missing parameters for dwmsg_setze_stichtag_info.")

if __name__ == "__main__":
    unittest.main()