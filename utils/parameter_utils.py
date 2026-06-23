# Legacy Source: h_alis_parameter.ksh for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
This module provides parameter handling utilities,
mimicking the functionality of h_alis_parameter.ksh.

It includes functions for parameter validation, conversion of codes,
and date range checks. Error handling is done via custom exceptions.
"""

import logging
from typing import Optional, Any
from utils.date_utils import dwdate_datum_check, dwdate_datum_le, dwdate_gib_zeitraum, DWDateError

logger = logging.getLogger(__name__)

class ParameterError(Exception):
    """Custom exception for parameter-related errors."""
    def __init__(self, message, error_code=None, arg_info=None):
        super().__init__(message)
        self.error_code = error_code
        self.arg_info = arg_info

# Global error state, similar to ErrNr in ksh script.
# In Python, it's generally better to raise exceptions and handle them,
# but to mimic the ksh script's flow, we can use a mutable global/class variable
# or pass an error object around. For simplicity and Pythonic approach,
# we will use exceptions primarily.
_ERR_NR: int = 0
_ERR_ARG: Optional[str] = None

MODUL_NAME = "parameter_utils"
MODUL_VERSION = "V1.0.0"

def _set_error_state(error_code: int, arg_info: str):
    """Sets a global error state (for functions that prefer to return early)."""
    global _ERR_NR, _ERR_ARG
    _ERR_NR = error_code
    _ERR_ARG = arg_info
    logger.error(f"Parameter Error {error_code}: {arg_info}")

def _reset_error_state():
    """Resets the global error state."""
    global _ERR_NR, _ERR_ARG
    _ERR_NR = 0
    _ERR_ARG = None

def pruefe_parameter_gesetzt(param_name: str, param_value: Optional[Any]):
    """
    Mimics pruefeParameterGesetzt. Checks if a parameter has a non-empty value.
    Raises ParameterError if the parameter is not set.
    """
    if param_value is None or (isinstance(param_value, str) and not param_value.strip()):
        raise ParameterError(f"Parameter '{param_name}' is not set.", error_code=194, arg_info=param_name)
    logger.debug(f"Parameter '{param_name}' is set: '{param_value}'")

# Mappings for conversion functions
KENNZAHL_MAPPING = {
    "zugang": "zug", "abgang": "abg", "abgang_zukunft": "abz", "bestand": "bst",
    "tarifwechsel": "twe", "plan": "pln", "gutschrift": "gut", "aufladung": "auf",
    "restguthaben": "rst", "teilnehmerverbindungsdaten": "tvd", "uskonto": "usk",
    "usteilnehmer": "ust", "leistungsklasse": "lkl", "loeschung": "loe",
    "reaktivierung": "rak", "standard_rechnung": "srs", "standard_gutschrift": "sgs",
    "gutschrift_rv": "sg_rv", "rechnungen_rv_dpps": "sr_rv_dpps", "bewegart": "bwa",
    "kundenstamm": "ksd", "mahnstufe": "mahn", "metadatenstruktur": "mds",
    "d1news": "d1n", "rubrik": "rub", "liefermodus": "lmo",
    "netznutzungsklassen": "nnk", "tagesverkehrskurven": "tvk", "gespraechsziele": "gz",
    "gespraechslaengenverteilung": "glv", "zonenkennung": "zonek", "zonentyp": "zonet",
    "netznutzungsklassentyp": "nnkt", "tarifart": "trfa", "gespraechstyp": "gtyp",
    "basisdienst": "basisd", "nationalinternational": "natint", "glaengenintervall": "glint"
}

SYSTEM_MAPPING = {
    "sap": "sap", "carmen": "carmen", "dpps": "dpps", "d1": "d1", "xtra": "xtra",
    "ctel": "ctel", "nnv": "nnv", "dwh": "dwh", "brunet": "brunet", "sigma": "sigma"
}

SD_NAME_MAPPING = {
    "vo": "vo", "rahmenvertrag": "rv", "tarif": "trf", "tstatus": "ts", "zahlmodus": "zm",
    "kdg_grund": "kdg", "gutschrift": "gut", "aufladung": "auf", "leistung": "l_leist",
    "gutschrift_grund": "l_gutgr", "sap_gutschrift_grund": "sap_l_gutgr",
    "produkt": "l_prod", "mahnverfahren_sapist": "l_mahnv_ist", "mahnverfahren_sapfi": "l_mahnv_fi",
    "mahnstufentyp_sapist": "l_mahnstyp_ist", "bewegart": "bwa"
}

AUFBAU_STUFE_XTRA_MAPPING = {
    "zusammenfuehrung": "mrg", "befuellung": "fill"
}

def konvertiere_kennzahl(kennzahl_desc: str) -> str:
    """
    Mimics konvertiereKennzahl. Converts a descriptive Kennzahl to its abbreviation.
    Raises ParameterError if the description is unknown.
    """
    kennzahl = KENNZAHL_MAPPING.get(kennzahl_desc.lower())
    if kennzahl is None:
        raise ParameterError(f"Unknown Kennzahl description: '{kennzahl_desc}'", error_code=198, arg_info=kennzahl_desc)
    logger.debug(f"Converted Kennzahl '{kennzahl_desc}' to '{kennzahl}'")
    return kennzahl

def konvertiere_system(system_desc: str) -> str:
    """
    Mimics konvertiereSystem. Converts a descriptive System to its abbreviation.
    Raises ParameterError if the description is unknown.
    """
    system = SYSTEM_MAPPING.get(system_desc.lower())
    if system is None:
        raise ParameterError(f"Unknown System description: '{system_desc}'", error_code=195, arg_info=f"Unbekannte Datenherkunft {system_desc} !")
    logger.debug(f"Converted System '{system_desc}' to '{system}'")
    return system

def konvertiere_sd_name(sd_name_desc: str) -> str:
    """
    Mimics konvertiereSDName. Converts a descriptive Stammdaten-System to its abbreviation.
    Raises ParameterError if the description is unknown.
    """
    sd_name = SD_NAME_MAPPING.get(sd_name_desc.lower())
    if sd_name is None:
        raise ParameterError(f"Unknown SD Name description: '{sd_name_desc}'", error_code=195, arg_info=f"Unbekannte Stammdaten-Datenherkunft {sd_name_desc} !")
    logger.debug(f"Converted SD Name '{sd_name_desc}' to '{sd_name}'")
    return sd_name

def konvertiere_aufbau_stufe_xtra(stufe_desc: str) -> str:
    """
    Mimics konvertiereAufbStufeXtra. Converts a descriptive AufbauStufeXtra to its abbreviation.
    Raises ParameterError if the description is unknown.
    """
    stufe = AUFBAU_STUFE_XTRA_MAPPING.get(stufe_desc.lower())
    if stufe is None:
        raise ParameterError(f"Unknown AufbauStufeXtra description: '{stufe_desc}'", error_code=195, arg_info=f"Unbekannte Stufenangabe {stufe_desc} !")
    logger.debug(f"Converted AufbauStufeXtra '{stufe_desc}' to '{stufe}'")
    return stufe

def pruefe_system_kennzahl(system: str, kennzahl: str):
    """
    Mimics pruefeSystemKennzahl. Checks if a combination of system and kennzahl is valid.
    Raises ParameterError if the combination is invalid.
    """
    invalid_combination = False
    error_arg = f"Ungueltige Kombination {system} {kennzahl}"

    system_lower = system.lower()
    kennzahl_lower = kennzahl.lower()

    if system_lower == "nnv":
        if kennzahl_lower not in ("tvd", "lkl"):
            invalid_combination = True
    elif system_lower == "carmen":
        if kennzahl_lower in ("twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"):
            invalid_combination = True
    elif system_lower == "sap":
        if kennzahl_lower in ("zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"):
            invalid_combination = True
    elif system_lower == "dpps":
        if kennzahl_lower in ("twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"):
            invalid_combination = True
    elif system_lower == "ctel":
        if kennzahl_lower not in ("abg", "bst", "zug", "twe"):
            invalid_combination = True
    elif system_lower == "xtra":
        if kennzahl_lower != "rst":
            invalid_combination = True
    elif system_lower == "d1":
        if kennzahl_lower in ("gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"):
            invalid_combination = True
    elif system_lower == "dwh":
        if kennzahl_lower != "mds":
            invalid_combination = True
    elif system_lower == "brunet":
        if kennzahl_lower not in ("d1n", "rub", "lmo"):
            invalid_combination = True
    elif system_lower == "sigma":
        if kennzahl_lower not in ("nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"):
            invalid_combination = True

    if invalid_combination:
        raise ParameterError(error_arg, error_code=195, arg_info=error_arg)
    logger.debug(f"System-Kennzahl combination '{system}/{kennzahl}' is valid.")

def gib_bereich(kennzahl: str) -> str:
    """
    Mimics gibBereich. Returns the category (Bereich) for a given Kennzahl.
    Raises ParameterError if the Kennzahl is unknown.
    """
    kennzahl_lower = kennzahl.lower()
    bereich_map = {
        "tn": ["abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"],
        "us": ["gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"],
        "gd": ["tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"],
        "sd": ["ksd", "bwa"],
        "md": ["mds"]
    }

    for bereich, kennzahlen_list in bereich_map.items():
        if kennzahl_lower in kennzahlen_list:
            logger.debug(f"Kennzahl '{kennzahl}' belongs to Bereich '{bereich}'")
            return bereich
    
    raise ParameterError(f"Kennzahl '{kennzahl}' unknown for Bereich determination.", error_code=196, arg_info=f"Kuerzel '{kennzahl}' unbekannt")

def gib_intervall(kennzahl: str) -> str:
    """
    Mimics gibIntervall. Returns the interval type (t or m) for a given Kennzahl.
    Raises ParameterError if the Kennzahl is unknown.
    """
    kennzahl_lower = kennzahl.lower()
    intervall_map = {
        "t": ["abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"],
        "m": ["bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"]
    }

    for intervall, kennzahlen_list in intervall_map.items():
        if kennzahl_lower in kennzahlen_list:
            logger.debug(f"Kennzahl '{kennzahl}' belongs to Intervall '{intervall}'")
            return intervall
    
    raise ParameterError(f"Kennzahl '{kennzahl}' unknown for Intervall determination.", error_code=196, arg_info=f"Kuerzel '{kennzahl}' unbekannt")

def pruefe_zeitraum(anfang_date_str: str, ende_date_str: str, date_format: str = "%Y%m%d"):
    """
    Mimics pruefeZeitraum. Checks if a date range is valid (format and start <= end).
    Raises ParameterError if dates are invalid or sequence is wrong.
    """
    if not anfang_date_str or not ende_date_str:
        raise ParameterError("Start or end date is missing for Zeitraum check.", error_code=196, arg_info=f"{MODUL_NAME} {MODUL_VERSION} pruefeZeitraum")

    if not dwdate_datum_check(anfang_date_str, date_format):
        raise ParameterError(f"Start date '{anfang_date_str}' does not match format '{date_format}'.", error_code=195, arg_info="Anfangsdatum entspricht nicht dem Format")
    
    if not dwdate_datum_check(ende_date_str, date_format):
        raise ParameterError(f"End date '{ende_date_str}' does not match format '{date_format}'.", error_code=195, arg_info="Endedatum entspricht nicht dem Format")

    if not dwdate_datum_le(anfang_date_str, ende_date_str, date_format):
        raise ParameterError(f"Start date '{anfang_date_str}' is after end date '{ende_date_str}'.", error_code=195, arg_info="Anfangsdatum ist nicht kleiner gleich Endedatum")
    
    logger.debug(f"Date range {anfang_date_str}-{ende_date_str} is valid.")

def pruefe_zahl_positiv(number_str: str, param_name: str):
    """
    Mimics pruefeZahlPositiv. Checks if a string represents a positive integer (>= 0).
    Raises ParameterError if not a number or negative.
    """
    try:
        number = int(number_str)
        if number < 0:
            raise ParameterError(f"Parameter '{param_name}' must be greater than or equal to 0.", error_code=195, arg_info=f"Parameter {param_name} muss groesser gleich 0 sein")
    except ValueError:
        raise ParameterError(f"Parameter '{param_name}' is not a numeric value.", error_code=195, arg_info=f"Parameter {param_name} ist kein numerischer Wert")
    logger.debug(f"Number '{number_str}' for '{param_name}' is positive.")

def pruefe_zeit_parameter(anfangsdatum: Optional[str], endedatum: Optional[str], zeit_offset: Optional[str]):
    """
    Mimics pruefeZeitParameter. Checks for valid combinations of start_date, end_date, and time_offset.
    Raises ParameterError for invalid combinations.
    """
    if zeit_offset:
        if anfangsdatum or endedatum:
            raise ParameterError("Only a time offset OR both start and end dates can be set, not a mix.", error_code=195, arg_info="Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden")
        pruefe_zahl_positiv(zeit_offset, "Zeitspanne")
    else:
        if not anfangsdatum or not endedatum:
            if not anfangsdatum and not endedatum:
                raise ParameterError("Date values or time offset are missing.", error_code=195, arg_info="Datumswerte oder Zeitspanne fehlen")
            else:
                raise ParameterError("Both start and end dates must be provided.", error_code=195, arg_info="Sowohl Anfang- als auch Endedatum muessen angegeben werden")
        pruefe_zeitraum(anfangsdatum, endedatum)
    logger.debug("Time parameters are valid.")

def konvertiere_zeitspanne(var_anfang: str, var_ende: str, spanne: int, kennzahl: str, date_format: str = "%Y%m%d") -> tuple[str, str]:
    """
    Mimics konvertiereZeitspanne. Calculates start and end dates based on a span and kennzahl.
    Returns (start_date, end_date) as strings.
    Raises ParameterError if calculation fails.
    """
    offset_unit = 'D'
    if kennzahl.lower() == 'bst': # "bestand"
        offset_unit = 'M'

    try:
        anfangsdatum, endedatum = dwdate_gib_zeitraum(offset=-spanne, unit=offset_unit, result_format=date_format)
        logger.debug(f"Converted time span for Kennzahl '{kennzahl}': Start='{anfangsdatum}', End='{endedatum}'")
        return anfangsdatum, endedatum
    except DWDateError as e:
        raise ParameterError(f"Error calculating time span: {e}", error_code=85, arg_info="DWDate_Gib_Zeitraum")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Test pruefe_parameter_gesetzt
    try:
        pruefe_parameter_gesetzt("ValidParam", "some_value") # Should pass
        pruefe_parameter_gesetzt("EmptyParam", "")
    except ParameterError as e:
        print(f"Caught expected error: {e}")

    # Test konvertiere_kennzahl
    try:
        print(f"Konvertiere 'Zugang': {konvertiere_kennzahl('Zugang')}")
        print(f"Konvertiere 'bestand': {konvertiere_kennzahl('bestand')}")
        print(f"Konvertiere 'TEILNEHMERVERBINDUNGSDATEN': {konvertiere_kennzahl('TEILNEHMERVERBINDUNGSDATEN')}")
        konvertiere_kennzahl("unknown")
    except ParameterError as e:
        print(f"Caught expected error: {e}")

    # Test pruefe_system_kennzahl
    try:
        pruefe_system_kennzahl("SAP", "mahn") # This should be invalid based on source
    except ParameterError as e:
        print(f"Caught expected error: {e}")
    try:
        pruefe_system_kennzahl("Carmen", "twe") # Invalid
    except ParameterError as e:
        print(f"Caught expected error: {e}")
    try:
        pruefe_system_kennzahl("SAP", "srs") # Valid
    except ParameterError as e:
        print(f"Unexpected error: {e}")

    # Test gib_bereich
    try:
        print(f"Bereich for 'zug': {gib_bereich('zug')}")
        gib_bereich("non_existent")
    except ParameterError as e:
        print(f"Caught expected error: {e}")

    # Test gib_intervall
    try:
        print(f"Intervall for 'zug': {gib_intervall('zug')}")
        gib_intervall("non_existent")
    except ParameterError as e:
        print(f"Caught expected error: {e}")

    # Test pruefe_zeitraum
    try:
        pruefe_zeitraum("20230101", "20230131")
        pruefe_zeitraum("20230131", "20230101") # Invalid
    except ParameterError as e:
        print(f"Caught expected error: {e}")

    # Test konvertiere_zeitspanne
    try:
        start, end = konvertiere_zeitspanne("ANF_DATE", "END_DATE", 30, "zug")
        print(f"Zeitspanne for 30 days: Start={start}, End={end}")
        start_bst, end_bst = konvertiere_zeitspanne("ANF_DATE", "END_DATE", 2, "bst")
        print(f"Zeitspanne for 2 months (bst): Start={start_bst}, End={end_bst}")
    except ParameterError as e:
        print(f"Caught expected error: {e}")