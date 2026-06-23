# Legacy Source: h_alis_date.ksh for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
Unit tests for the date_utils.py module.
These tests cover date calculations and validations.
"""

import unittest
from datetime import datetime
from unittest.mock import patch

from utils.date_utils import (
    dwdate_vormonat, dwdate_datum_check, dwdate_datum_le,
    dwdate_gib_zeitraum, letzter_tag_des_monats, tage_im_monat,
    addiere_datum, DWDateError
)

class TestDateUtils(unittest.TestCase):

    @patch('utils.date_utils.datetime')
    def test_dwdate_vormonat(self, mock_datetime):
        # Test with a specific date to ensure consistent "previous month" calculation
        mock_datetime.now.return_value = datetime(2023, 10, 26)
        mock_datetime.strptime.side_effect = datetime.strptime # Pass through for actual date parsing
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) # Allow datetime constructor
        mock_datetime.fromtimestamp.side_effect = datetime.fromtimestamp # Allow datetime.fromtimestamp
        
        self.assertEqual(dwdate_vormonat(), "20230901")
        self.assertEqual(dwdate_vormonat(date_format="%Y-%m"), "2023-09")

        # Test January -> December of previous year
        mock_datetime.now.return_value = datetime(2023, 1, 15)
        self.assertEqual(dwdate_vormonat(), "20221201")

    def test_dwdate_datum_check(self):
        self.assertTrue(dwdate_datum_check("20231026", "%Y%m%d"))
        self.assertFalse(dwdate_datum_check("2023-10-26", "%Y%m%d"))
        self.assertTrue(dwdate_datum_check("2023-10-26", "%Y-%m-%d"))
        self.assertFalse(dwdate_datum_check("invalid-date", "%Y%m%d"))
        self.assertFalse(dwdate_datum_check("20230230", "%Y%m%d")) # Invalid date value

    def test_dwdate_datum_le(self):
        self.assertTrue(dwdate_datum_le("20230101", "20230101"))
        self.assertTrue(dwdate_datum_le("20230101", "20230102"))
        self.assertFalse(dwdate_datum_le("20230102", "20230101"))
        
        with self.assertRaises(DWDateError):
            dwdate_datum_le("2023-01-01", "20230101", "%Y%m%d") # Format mismatch
        with self.assertRaises(DWDateError):
            dwdate_datum_le("invalid", "20230101")

    @patch('utils.date_utils.datetime')
    def test_dwdate_gib_zeitraum(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 10, 15)
        mock_datetime.strptime.side_effect = datetime.strptime
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        mock_datetime.fromtimestamp.side_effect = datetime.fromtimestamp
        
        # Day offset
        start, end = dwdate_gib_zeitraum(5, 'D')
        self.assertEqual(start, "20231015")
        self.assertEqual(end, "20231020") # 2023-10-15 + 5 days

        # Month offset (positive)
        start, end = dwdate_gib_zeitraum(2, 'M') # 2 months from today's month
        self.assertEqual(start, "20231001") # First day of current month
        self.assertEqual(end, "20231231") # Last day of current month + 2

        # Month offset (negative)
        start, end = dwdate_gib_zeitraum(-1, 'M') # 1 month before today's month
        self.assertEqual(start, "20230901") # First day of (current month - 1)
        self.assertEqual(end, "20231031") # Last day of current month
        
        # Year offset (positive)
        start, end = dwdate_gib_zeitraum(1, 'Y')
        self.assertEqual(start, "20230101") # First day of current year
        self.assertEqual(end, "20241231") # Last day of current year + 1

        # Year offset (negative)
        start, end = dwdate_gib_zeitraum(-1, 'Y')
        self.assertEqual(start, "20220101") # First day of current year - 1
        self.assertEqual(end, "20231231") # Last day of current year

        with self.assertRaises(DWDateError):
            dwdate_gib_zeitraum(1, 'X') # Invalid unit

    def test_letzter_tag_des_monats(self):
        self.assertTrue(letzter_tag_des_monats("20230131"))
        self.assertTrue(letzter_tag_des_monats("20240229")) # Leap year
        self.assertFalse(letzter_tag_des_monats("20230228"))
        self.assertTrue(letzter_tag_des_monats("20230228")) # Not leap year, 28th is last day
        self.assertFalse(letzter_tag_des_monats("20230115"))
        with self.assertRaises(DWDateError):
            letzter_tag_des_monats("invalid")

    def test_tage_im_monat(self):
        self.assertEqual(tage_im_monat(2023, 1), 31)
        self.assertEqual(tage_im_monat(2023, 2), 28)
        self.assertEqual(tage_im_monat(2024, 2), 29) # Leap year
        self.assertEqual(tage_im_monat(2023, 4), 30)
        with self.assertRaises(DWDateError):
            tage_im_monat(2023, 0)
        with self.assertRaises(DWDateError):
            tage_im_monat(2023, 13)

    def test_addiere_datum(self):
        self.assertEqual(addiere_datum("20230101", 1), "20230102")
        self.assertEqual(addiere_datum("20230131", 1), "20230201")
        self.assertEqual(addiere_datum("20240229", 1), "20240301") # Leap year
        self.assertEqual(addiere_datum("20230101", -1), "20221231")
        with self.assertRaises(DWDateError):
            addiere_datum("invalid", 1)

if __name__ == "__main__":
    unittest.main()