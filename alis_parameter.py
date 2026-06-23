# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

import datetime
import calendar

# Constants
MODUL_NAME = "alis_parameter"
MODUL_VERSION = "V3.0.9"

# Custom Exception Classes
class ParameterError(ValueError):
    """Custom exception for parameter-related errors."""
    def __init__(self, message, error_code=195, arg=None):
        super().__init__(message)
        self.error_code = error_code
        self.arg = arg if arg is not None else message

class ValidationError(ValueError):
    """Custom exception for validation errors."""
    def __init__(self, message, error_code=195, arg=None):
        super().__init__(message)
        self.error_code = error_code
        self.arg = arg if arg is not None else message

# Helper functions for DWDate equivalents
def _dwdate_datum_check(date_string: str, date_format: str = "%Y%m%d") -> bool:
    """
    Checks if a date string conforms to the given format.
    Corresponds to DWDate_Datum_Check.
    """
    try:
        datetime.datetime.strptime(date_string, date_format)
        return True
    except ValueError:
        return False

def _dwdate_datum_le(date1_string: str, date2_string: str, date_format: str = "%Y%m%d") -> bool:
    """
    Compares two dates and returns true if the first is less than or equal to the second.
    Corresponds to DWDate_Datum_LE.
    """
    try:
        date1 = datetime.datetime.strptime(date1_string, date_format).date()
        date2 = datetime.datetime.strptime(date2_string, date_format).date()
        return date1 <= date2
    except ValueError:
        # This should ideally not happen if _dwdate_datum_check is called first.
        # But as a safeguard:
        raise ValidationError(f"Invalid date format for comparison: '{date1_string}' or '{date2_string}'")


def _dwdate_gib_zeitraum(span_value: int, unit: str, output_format: str = "%Y%m%d") -> tuple[str, str]:
    """
    Calculates a date range based on a span and unit, relative to today.
    Corresponds to DWDate_Gib_Zeitraum.
    span_value: Negative integer indicating periods into the past.
    unit: 'D' for Day, 'M' for Month.
    """
    today = datetime.date.today()

    if unit == 'D':
        # For days, it's simply today + timedelta(days=span_value)
        # Assuming span_value is negative, this gives a date in the past.
        # Start and end date are the same for a daily span.
        calculated_date = today + datetime.timedelta(days=span_value)
        start_date = calculated_date
        end_date = calculated_date
    elif unit == 'M':
        # For months, calculate first and last day of the month 'span_value' months ago
        # Example: if span_value = -1, get first and last day of previous month.
        # Add 1 to month to handle Python's 1-indexed months
        target_year = today.year
        target_month = today.month

        # Adjust month and year based on span_value
        total_months = target_year * 12 + target_month + span_value - 1
        target_year = total_months // 12
        target_month = (total_months % 12) + 1

        first_day_of_target_month = datetime.date(target_year, target_month, 1)
        last_day_of_target_month = datetime.date(target_year, target_month, calendar.monthrange(target_year, target_month)[1])

        start_date = first_day_of_target_month
        end_date = last_day_of_target_month
    else:
        raise ParameterError(f"Unknown unit for Zeitraum calculation: '{unit}'")

    return start_date.strftime(output_format), end_date.strftime(output_format)


def pruefe_parameter_gesetzt(param_name: str, param_value: str | None):
    """
    Checks if a parameter value is set (not None or empty string).
    Corresponds to pruefeParameterGesetzt.
    """
    if not isinstance(param_name, str) or not param_name:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} pruefe_parameter_gesetzt - Internal error: 'param_name' must be a non-empty string.",
            error_code=196
        )

    if param_value is None or (isinstance(param_value, str) and param_value == ""):
        raise ParameterError(f"Parameter '{param_name}' is not set.", error_code=194, arg=param_name)


def konvertiere_kennzahl(kennzahl: str) -> str:
    """
    Converts a key figure name to its standardized abbreviation.
    Corresponds to konvertiereKennzahl.
    """
    if not kennzahl:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} konvertiere_kennzahl - Input 'kennzahl' cannot be empty.",
            error_code=196
        )

    kennzahl_map = {
        "zugang": "zug",
        "abgang": "abg",
        "abgang_zukunft": "abz",
        "bestand": "bst",
        "tarifwechsel": "twe",
        "plan": "pln",
        "gutschrift": "gut",
        "aufladung": "auf",
        "restguthaben": "rst",
        "teilnehmerverbindungsdaten": "tvd",
        "uskonto": "usk",
        "usteilnehmer": "ust",
        "leistungsklasse": "lkl",
        "loeschung": "loe",
        "reaktivierung": "rak",
        "standard_rechnung": "srs",
        "standard_gutschrift": "sgs",
        "gutschrift_rv": "sg_rv",
        "rechnungen_rv_dpps": "sr_rv_dpps",
        "bewegart": "bwa",
        "kundenstamm": "ksd",
        "mahnstufe": "mahn",
        "metadatenstruktur": "mds",
        "d1news": "d1n",
        "rubrik": "rub",
        "liefermodus": "lmo",
        "netznutzungsklassen": "nnk",
        "tagesverkehrskurven": "tvk",
        "gespraechsziele": "gz",
        "gespraechslaengenverteilung": "glv",
        "zonenkennung": "zonek",
        "zonentyp": "zonet",
        "netznutzungsklassentyp": "nnkt",
        "tarifart": "trfa",
        "gespraechstyp": "gtyp",
        "basisdienst": "basisd",
        "nationalinternational": "natint",
        "glaengenintervall": "glint",
    }

    abbreviation = kennzahl_map.get(kennzahl.lower())
    if abbreviation is None:
        raise ValidationError(f"Unknown key figure: '{kennzahl}'", error_code=198, arg=kennzahl)
    return abbreviation


def konvertiere_system(system_name: str) -> str:
    """
    Converts a system name to its standardized abbreviation.
    Corresponds to konvertiereSystem.
    """
    if not system_name:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} konvertiere_system - Input 'system_name' cannot be empty.",
            error_code=196
        )

    system_map = {
        "sap": "sap",
        "carmen": "carmen",
        "dpps": "dpps",
        "d1": "d1",
        "xtra": "xtra",
        "ctel": "ctel",
        "nnv": "nnv",
        "dwh": "dwh",
        "brunet": "brunet",
        "sigma": "sigma",
    }

    abbreviation = system_map.get(system_name.lower())
    if abbreviation is None:
        raise ValidationError(f"Unknown data source system: '{system_name}'!", error_code=195, arg=system_name)
    return abbreviation


def konvertiere_sd_name(sd_name: str) -> str:
    """
    Converts a SD (Stammdaten) system name to its standardized abbreviation.
    Corresponds to konvertiereSDName.
    """
    if not sd_name:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} konvertiere_sd_name - Input 'sd_name' cannot be empty.",
            error_code=196
        )

    sd_map = {
        "vo": "vo",
        "rahmenvertrag": "rv",
        "tarif": "trf",
        "tstatus": "ts",
        "zahlmodus": "zm",
        "kdg_grund": "kdg",
        "gutschrift": "gut",
        "aufladung": "auf",
        "leistung": "l_leist",
        "gutschrift_grund": "l_gutgr",
        "sap_gutschrift_grund": "sap_l_gutgr",
        "produkt": "l_prod",
        "mahnverfahren_sapist": "l_mahnv_ist",
        "mahnverfahren_sapfi": "l_mahnv_fi",
        "mahnstufentyp_sapist": "l_mahnstyp_ist",
        "bewegart": "bwa",
    }

    abbreviation = sd_map.get(sd_name.lower())
    if abbreviation is None:
        raise ValidationError(f"Unknown master data source system: '{sd_name}'!", error_code=195, arg=sd_name)
    return abbreviation


def konvertiere_aufb_stufe_xtra(stufe_name: str) -> str:
    """
    Converts an 'Aufbereitungsstufe' (processing stage) name to its abbreviation.
    Corresponds to konvertiereAufbStufeXtra.
    """
    if not stufe_name:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} konvertiere_aufb_stufe_xtra - Input 'stufe_name' cannot be empty.",
            error_code=196
        )

    stufe_map = {
        "zusammenfuehrung": "mrg",
        "befuellung": "fill",
    }

    abbreviation = stufe_map.get(stufe_name.lower())
    if abbreviation is None:
        raise ValidationError(f"Unknown processing stage: '{stufe_name}'!", error_code=195, arg=stufe_name)
    return abbreviation


def pruefe_system_kennzahl(system: str, kennzahl: str):
    """
    Checks if the combination of system and key figure is allowed.
    Corresponds to pruefeSystemKennzahl.
    Assumes system and kennzahl are already converted to abbreviations.
    """
    if not system or not kennzahl:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} pruefe_system_kennzahl - 'system' and 'kennzahl' cannot be empty.",
            error_code=196
        )

    # Convert to lowercase for consistent comparison
    system_l = system.lower()
    kennzahl_l = kennzahl.lower()

    err_arg = None

    if system_l != "nnv" and (kennzahl_l == "tvd" or kennzahl_l == "lkl"):
        err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "carmen":
        if kennzahl_l in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "sap":
        if kennzahl_l in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "dpps":
        if kennzahl_l in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "ctel":
        if kennzahl_l not in ["abg", "bst", "zug", "twe"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "xtra":
        if kennzahl_l != "rst":
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "d1":
        if kennzahl_l in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "nnv":
        if not (kennzahl_l == "tvd" or kennzahl_l == "lkl"):
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "dwh":
        if kennzahl_l != "mds":
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "brunet":
        if kennzahl_l not in ["d1n", "rub", "lmo"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"
    elif system_l == "sigma":
        if kennzahl_l not in ["nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]:
            err_arg = f"Invalid combination {system_l} {kennzahl_l}"

    if err_arg:
        raise ValidationError(err_arg, error_code=195)


def gib_bereich(kennzahl: str) -> str:
    """
    Returns the associated 'Bereich' (area) for a given key figure.
    Corresponds to gibBereich.
    Assumes kennzahl is already converted to abbreviation.
    """
    if not kennzahl:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} gib_bereich - Input 'kennzahl' cannot be empty.",
            error_code=196
        )

    # Define the lists as sets for efficient lookup
    list_tn = {"abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"}
    list_us = {"gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
    list_gd = {"tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
    list_sd = {"ksd", "bwa"}
    list_md = {"mds"}

    kennzahl_l = kennzahl.lower()
    my_bereich = None

    if kennzahl_l in list_tn:
        my_bereich = "tn"
    elif kennzahl_l in list_us:
        my_bereich = "us"
    elif kennzahl_l in list_gd:
        my_bereich = "gd"
    elif kennzahl_l in list_sd:
        my_bereich = "sd"
    elif kennzahl_l in list_md:
        my_bereich = "md"

    if my_bereich is None:
        raise ValidationError(f"{MODUL_NAME} {MODUL_VERSION} gib_bereich - Abbreviation '{kennzahl}' unknown.", error_code=196)

    return my_bereich


def gib_intervall(kennzahl: str) -> str:
    """
    Returns the associated 'Intervall' (interval: 't' for daily, 'm' for monthly)
    for a given key figure.
    Corresponds to gibIntervall.
    Assumes kennzahl is already converted to abbreviation.
    """
    if not kennzahl:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} gib_intervall - Input 'kennzahl' cannot be empty.",
            error_code=196
        )

    # Define the lists as sets for efficient lookup
    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    kennzahl_l = kennzahl.lower()
    my_intervall = None

    if kennzahl_l in list_t:
        my_intervall = "t"
    elif kennzahl_l in list_m:
        my_intervall = "m"

    if my_intervall is None:
        raise ValidationError(f"{MODUL_NAME} {MODUL_VERSION} gib_intervall - Abbreviation '{kennzahl}' unknown.", error_code=196)

    return my_intervall


def pruefe_zahl_positiv(number_str: str, param_name: str):
    """
    Checks if the given string is a positive number (>= 0).
    Corresponds to pruefeZahlPositiv.
    """
    if not isinstance(param_name, str) or not param_name:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} pruefe_zahl_positiv - Internal error: 'param_name' must be a non-empty string.",
            error_code=196
        )
    if not isinstance(number_str, str) or not number_str:
        raise ParameterError(f"Parameter '{param_name}' is not set.", error_code=194, arg=param_name)

    try:
        number = int(number_str)
        if number < 0:
            raise ValidationError(f"Parameter '{param_name}' must be greater than or equal to 0.", error_code=195)
    except ValueError:
        raise ValidationError(f"Parameter '{param_name}' is not a numeric value.", error_code=195)


def pruefe_zeit_parameter(start_date: str | None, end_date: str | None, time_offset: str | None):
    """
    Validates date parameters (start date, end date, or time span).
    Corresponds to pruefeZeitParameter.
    """
    # Case 1: time_offset is set
    if time_offset is not None and time_offset != "":
        if (start_date is None or start_date == "") and (end_date is None or end_date == ""):
            # The time_offset itself must be a positive numeric value
            pruefe_zahl_positiv(time_offset, "Zeitspanne")
        else:
            raise ValidationError(
                "Only a time span OR both date values must be set, not a mix.",
                error_code=195
            )
    # Case 2: time_offset is empty/None
    else:
        if (start_date is not None and start_date != "") and (end_date is not None and end_date != ""):
            # Validate date semantics
            pruefe_zeitraum(start_date, end_date)
        else:
            if (start_date is None or start_date == "") and (end_date is None or end_date == ""):
                raise ValidationError("Date values or time span are missing.", error_code=195)
            else:
                raise ValidationError("Both start and end dates must be provided.", error_code=195)


def pruefe_zeitraum(anfang: str, ende: str, date_format: str = "%Y%m%d"):
    """
    Checks if two dates form a valid period (format and order).
    Corresponds to pruefeZeitraum.
    """
    if not anfang:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} pruefe_zeitraum - 'Anfang' date cannot be empty.",
            error_code=196
        )
    if not ende:
        raise ParameterError(
            f"{MODUL_NAME} {MODUL_VERSION} pruefe_zeitraum - 'Ende' date cannot be empty.",
            error_code=196
        )

    if not _dwdate_datum_check(anfang, date_format):
        raise ValidationError(f"Start date '{anfang}' does not match format {date_format}.", error_code=195, arg=f"Anfangsdatum entspricht nicht dem Format {date_format}")

    if not _dwdate_datum_check(ende, date_format):
        raise ValidationError(f"End date '{ende}' does not match format {date_format}.", error_code=195, arg=f"Endedatum entspricht nicht dem Format {date_format}")

    if not _dwdate_datum_le(anfang, ende, date_format):
        raise ValidationError("Start date is not less than or equal to end date.", error_code=195, arg="Anfangsdatum ist nicht kleiner gleich Endedatum")


def konvertiere_zeitspanne(span_value_str: str, kennzahl: str) -> tuple[str, str]:
    """
    Calculates start and end dates from a time span and key figure.
    Corresponds to konvertiereZeitspanne.
    Returns a tuple (start_date_str, end_date_str) in YYYYMMDD format.
    Assumes kennzahl is already converted to abbreviation.
    """
    pruefe_zahl_positiv(span_value_str, "Zeitspanne") # Ensures span_value_str is a positive integer string

    span_value = int(span_value_str)
    
    # Original script uses -$p_Spanne, so we need to negate it for _dwdate_gib_zeitraum
    adjusted_span = -span_value

    offset_unit = 'D'
    if kennzahl.lower() == "bst":
        offset_unit = 'M'

    try:
        start_date, end_date = _dwdate_gib_zeitraum(adjusted_span, offset_unit, "%Y%m%d")
        return start_date, end_date
    except ParameterError as e:
        # Wrap the internal error with more context
        raise ValidationError(f"Error during Zeitraum calculation: {e.arg}", error_code=85, arg="DWDate_Gib_Zeitraum")

---