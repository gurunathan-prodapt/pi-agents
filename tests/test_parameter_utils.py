# Legacy Source: h_alis_parameter.ksh for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
Unit tests for the parameter_utils.py module.
These tests cover parameter validation and conversion logic.
"""

import unittest
from unittest.mock import patch

from utils.parameter_utils import (
    pruefe_parameter_gesetzt, konvertiere_kennzahl, konvertiere_system,
    konvertiere_sd_name, konvertiere_aufbau_stufe_xtra, pruefe_system_kennzahl,
    gib_bereich, gib_intervall, pruefe_zeitraum, pruefe_zahl_positiv,
    pruefe_zeit_parameter, konvertiere_zeitspanne, ParameterError
)
from utils.date_utils import DWDateError # For mocking if necessary

class TestParameterUtils(unittest.TestCase):

    def test_pruefe_parameter_gesetzt(self):
        pruefe_parameter_gesetzt("ValidParam", "some_value") # Should pass
        with self.assertRaisesRegex(ParameterError, "Parameter 'EmptyParam' is not set."):
            pruefe_parameter_gesetzt("EmptyParam", "")
        with self.assertRaisesRegex(ParameterError, "Parameter 'NoneParam' is not set."):
            pruefe_parameter_gesetzt("NoneParam", None)

    def test_konvertiere_kennzahl(self):
        self.assertEqual(konvertiere_kennzahl("Zugang"), "zug")
        self.assertEqual(konvertiere_kennzahl("bestand"), "bst")
        self.assertEqual(konvertiere_kennzahl("TEILNEHMERVERBINDUNGSDATEN"), "tvd")
        with self.assertRaisesRegex(ParameterError, "Unknown Kennzahl description: 'unknown_kennzahl'"):
            konvertiere_kennzahl("unknown_kennzahl")

    def test_konvertiere_system(self):
        self.assertEqual(konvertiere_system("SAP"), "sap")
        self.assertEqual(konvertiere_system("Xtra"), "xtra")
        with self.assertRaisesRegex(ParameterError, "Unknown System description: 'unknown_system'"):
            konvertiere_system("unknown_system")

    def test_konvertiere_sd_name(self):
        self.assertEqual(konvertiere_sd_name("rahmenvertrag"), "rv")
        self.assertEqual(konvertiere_sd_name("gutschrift"), "gut")
        with self.assertRaisesRegex(ParameterError, "Unknown SD Name description: 'unknown_sd'"):
            konvertiere_sd_name("unknown_sd")

    def test_konvertiere_aufbau_stufe_xtra(self):
        self.assertEqual(konvertiere_aufbau_stufe_xtra("zusammenfuehrung"), "mrg")
        self.assertEqual(konvertiere_aufbau_stufe_xtra("befuellung"), "fill")
        with self.assertRaisesRegex(ParameterError, "Unknown AufbauStufeXtra description: 'unknown_stufe'"):
            konvertiere_aufbau_stufe_xtra("unknown_stufe")

    def test_pruefe_system_kennzahl(self):
        # Valid combinations
        pruefe_system_kennzahl("SAP", "srs")
        pruefe_system_kennzahl("NNV", "tvd")
        pruefe_system_kennzahl("Brunet", "d1n")

        # Invalid combinations
        with self.assertRaisesRegex(ParameterError, "Ungueltige Kombination SAP mahn"):
            pruefe_system_kennzahl("SAP", "mahn")
        with self.assertRaisesRegex(ParameterError, "Ungueltige Kombination Carmen twe"):
            pruefe_system_kennzahl("Carmen", "twe")
        with self.assertRaisesRegex(ParameterError, "Ungueltige Kombination D1 gut"):
            pruefe_system_kennzahl("D1", "gut")
        with self.assertRaisesRegex(ParameterError, "Ungueltige Kombination Xtra twe"):
            pruefe_system_kennzahl("Xtra", "twe") # Xtra only allows 'rst'

    def test_gib_bereich(self):
        self.assertEqual(gib_bereich("zug"), "tn")
        self.assertEqual(gib_bereich("rst"), "us")
        self.assertEqual(gib_bereich("tvd"), "gd")
        self.assertEqual(gib_bereich("ksd"), "sd")
        self.assertEqual(gib_bereich("mds"), "md")
        with self.assertRaisesRegex(ParameterError, "Kennzahl 'non_existent' unknown for Bereich determination."):
            gib_bereich("non_existent")

    def test_gib_intervall(self):
        self.assertEqual(gib_intervall("zug"), "t")
        self.assertEqual(gib_intervall("bst"), "m")
        with self.assertRaisesRegex(ParameterError, "Kennzahl 'non_existent' unknown for Intervall determination."):
            gib_intervall("non_existent")

    def test_pruefe_zeitraum(self):
        # Valid
        pruefe_zeitraum("20230101", "20230131")
        pruefe_zeitraum("20230115", "20230115")
        
        # Invalid format
        with self.assertRaisesRegex(ParameterError, "Start date '2023-01-01' does not match format '%Y%m%d'."):
            pruefe_zeitraum("2023-01-01", "20230131")
        
        # Start after end
        with self.assertRaisesRegex(ParameterError, "Start date '20230131' is after end date '20230101'."):
            pruefe_zeitraum("20230131", "20230101")
        
        # Missing dates
        with self.assertRaisesRegex(ParameterError, "Start or end date is missing"):
            pruefe_zeitraum(None, "20230101")
        with self.assertRaisesRegex(ParameterError, "Start or end date is missing"):
            pruefe_zeitraum("20230101", "")

    def test_pruefe_zahl_positiv(self):
        pruefe_zahl_positiv("10", "TestNumber")
        pruefe_zahl_positiv("0", "TestNumber")
        
        with self.assertRaisesRegex(ParameterError, "Parameter 'NegativeNumber' must be greater than or equal to 0."):
            pruefe_zahl_positiv("-5", "NegativeNumber")
        with self.assertRaisesRegex(ParameterError, "Parameter 'NonNumeric' is not a numeric value."):
            pruefe_zahl_positiv("abc", "NonNumeric")

    @patch('utils.parameter_utils.pruefe_zahl_positiv')
    @patch('utils.parameter_utils.pruefe_zeitraum')
    def test_pruefe_zeit_parameter(self, mock_pruefe_zeitraum, mock_pruefe_zahl_positiv):
        # Valid: offset only
        pruefe_zeit_parameter(None, None, "10")
        mock_pruefe_zahl_positiv.assert_called_with("10", "Zeitspanne")
        mock_pruefe_zeitraum.assert_not_called()

        # Valid: start and end dates
        pruefe_zeit_parameter("20230101", "20230131", None)
        mock_pruefe_zeitraum.assert_called_with("20230101", "20230131")
        mock_pruefe_zahl_positiv.reset_mock()

        # Invalid: mix of offset and dates
        with self.assertRaisesRegex(ParameterError, "Only a time offset OR both start and end dates can be set, not a mix."):
            pruefe_zeit_parameter("20230101", None, "10")
        with self.assertRaisesRegex(ParameterError, "Only a time offset OR both start and end dates can be set, not a mix."):
            pruefe_zeit_parameter(None, "20230131", "10")

        # Invalid: missing both dates and offset
        with self.assertRaisesRegex(ParameterError, "Date values or time offset are missing."):
            pruefe_zeit_parameter(None, None, None)
        
        # Invalid: only one date
        with self.assertRaisesRegex(ParameterError, "Both start and end dates must be provided."):
            pruefe_zeit_parameter("20230101", None, None)
        with self.assertRaisesRegex(ParameterError, "Both start and end dates must be provided."):
            pruefe_zeit_parameter(None, "20230131", None)

    @patch('utils.parameter_utils.dwdate_gib_zeitraum')
    def test_konvertiere_zeitspanne(self, mock_dwdate_gib_zeitraum):
        mock_dwdate_gib_zeitraum.return_value = ("20230101", "20230131")
        
        start, end = konvertiere_zeitspanne("ANF_VAR", "END_VAR", 30, "zug")
        self.assertEqual(start, "20230101")
        self.assertEqual(end, "20230131")
        mock_dwdate_gib_zeitraum.assert_called_with(offset=-30, unit='D', result_format="%Y%m%d")

        start, end = konvertiere_zeitspanne("ANF_VAR", "END_VAR", 2, "bst")
        self.assertEqual(start, "20230101")
        self.assertEqual(end, "20230131")
        mock_dwdate_gib_zeitraum.assert_called_with(offset=-2, unit='M', result_format="%Y%m%d")

        mock_dwdate_gib_zeitraum.side_effect = DWDateError("Mocked DWDateError")
        with self.assertRaisesRegex(ParameterError, "Error calculating time span: Mocked DWDateError"):
            konvertiere_zeitspanne("ANF_VAR", "END_VAR", 1, "zug")

if __name__ == "__main__":
    unittest.main()