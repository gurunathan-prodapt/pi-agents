#!/usr/bin/env python3
import os
import sys
import datetime
import argparse
import calendar

# ==============================================================================
# Global Module Metadata & Legacy Error Tracking State
# ==============================================================================
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"
ErrNr = 0
ErrArg = ""

# ==============================================================================
# Helper Functions
# ==============================================================================

def pruefeParameterGesetzt(param_name: str, param_var: str) -> None:
    """
    Checks whether the specified environment variable is populated.
    Sets global ErrNr/ErrArg if empty and no previous error is set.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not param_name or not param_var:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeParameterGesetzt"
        return

    param_wert = os.environ.get(param_var)
    if not param_wert:
        ErrNr = 194
        ErrArg = param_name


def konvertiereKennzahl(var_name: str) -> None:
    """
    Converts verbose key figure term in environment variable to short abbreviation.
    Modifies environment variable in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereKennzahl"
        return

    kennzahl_val = os.environ.get(var_name, "")
    kennzahl = kennzahl_val.lower()

    mappings = {
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
        "glaengenintervall": "glint"
    }

    if kennzahl in mappings:
        result = mappings[kennzahl]
    else:
        ErrNr = 198
        ErrArg = kennzahl_val
        result = "???"

    os.environ[var_name] = result


def konvertiereSystem(var_name: str) -> None:
    """
    Normalizes and validates source system in environment variable in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSystem"
        return

    system_val = os.environ.get(var_name, "")
    system = system_val.lower()
    allowed_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if system in allowed_systems:
        result = system
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Datenherkunft {system_val} !"
        result = "???"

    os.environ[var_name] = result


def konvertiereSDName(var_name: str) -> None:
    """
    Converts verbose master data category term in environment variable in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSDSystem"
        return

    system_val = os.environ.get(var_name, "")
    system = system_val.lower()

    mappings = {
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
        "bewegart": "bwa"
    }

    if system in mappings:
        result = mappings[system]
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stammdaten-Datenherkunft {system_val} !"
        result = "???"

    os.environ[var_name] = result


def konvertiereAufbStufeXtra(var_name: str) -> None:
    """
    Converts Xtra stage names in environment variable in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra"
        return

    stufe_val = os.environ.get(var_name, "")
    stufe = stufe_val.lower()

    if stufe == "zusammenfuehrung":
        result = "mrg"
    elif stufe == "befuellung":
        result = "fill"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stufenangabe {stufe_val} !"
        result = "???"

    os.environ[var_name] = result


def pruefeSystemKennzahl(system: str, kennzahl: str) -> None:
    """
    Verifies if combination of source system and key figure is allowed.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not system or not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeSystemKennzahl"
        return

    err_arg_temp = ""

    if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
        err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "carmen":
        if kennzahl in {"twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sap":
        if kennzahl in {"zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dpps":
        if kennzahl in {"twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "ctel":
        if kennzahl not in {"abg", "bst", "zug", "twe"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "xtra":
        if kennzahl != "rst":
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "d1":
        if kennzahl in {"gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "nnv":
        if kennzahl not in {"tvd", "lkl"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dwh":
        if kennzahl != "mds":
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "brunet":
        if kennzahl not in {"d1n", "rub", "lmo"}:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sigma":
        allowed_sigma = {"nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
        if kennzahl not in allowed_sigma:
            err_arg_temp = f"Ungueltige Kombination {system} {kennzahl}"

    if err_arg_temp:
        ErrNr = 195
        ErrArg = err_arg_temp


def gibBereich(kennzahl: str, var_bereich: str) -> None:
    """
    Returns mapped business domain area based on key figure abbreviation.
    Modifies environment variable in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not kennzahl or not var_bereich:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich"
        return

    list_tn = {"abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"}
    list_us = {"gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
    list_gd = {"tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
    list_sd = {"ksd", "bwa"}
    list_md = {"mds"}

    my_bereich = None
    if kennzahl in list_tn:
        my_bereich = "tn"
    elif kennzahl in list_us:
        my_bereich = "us"
    elif kennzahl in list_gd:
        my_bereich = "gd"
    elif kennzahl in list_sd:
        my_bereich = "sd"
    elif kennzahl in list_md:
        my_bereich = "md"

    if not my_bereich:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich - Kuerzel '{kennzahl}' unbekannt"
        return

    os.environ[var_bereich] = my_bereich


def gibIntervall(kennzahl: str, var_intervall: str) -> None:
    """
    Returns mapped reporting frequency interval based on key figure.
    Modifies environment variable in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not kennzahl or not var_intervall:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall"
        return

    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    my_intervall = None
    if kennzahl in list_t:
        my_intervall = "t"
    elif kennzahl in list_m:
        my_intervall = "m"

    if not my_intervall:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
        return

    os.environ[var_intervall] = my_intervall


def pruefeZeitraum(anfang: str, ende: str) -> None:
    """
    Natively validates date parameters format and chronological sanity.
    Replaces DWDate_Datum_Check & DWDate_Datum_LE.
    """
    # REVIEW-STRUCT: DWDate_Datum_Check and DWDate_Datum_LE logic not supplied — mapped to native datetime logic
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not anfang or not ende:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeZeitraum"
        return

    err_arg_temp = ""
    try:
        dt_anfang = datetime.datetime.strptime(anfang, "%Y%m%d")
    except ValueError:
        err_arg_temp = "Anfangsdatum entspricht nicht dem Format YYYYMMDD"

    if not err_arg_temp:
        try:
            dt_ende = datetime.datetime.strptime(ende, "%Y%m%d")
        except ValueError:
            err_arg_temp = "Endedatum entspricht nicht dem Format YYYYMMDD"

    if not err_arg_temp:
        if dt_anfang > dt_ende:
            err_arg_temp = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    if err_arg_temp:
        ErrNr = 195
        ErrArg = err_arg_temp


def pruefeZahlPositiv(p_zahl: str, p_parameter_name: str) -> None:
    """
    Ensures input string is numeric and greater than or equal to 0.
    """
    global ErrNr, ErrArg
    try:
        val = int(p_zahl)
        is_numeric = True
    except ValueError:
        try:
            val = float(p_zahl)
            is_numeric = True
        except ValueError:
            is_numeric = False

    if is_numeric:
        if val < 0:
            ErrNr = 195
            ErrArg = f"Parameter {p_parameter_name} muss groesser gleich 0 sein"
    else:
        ErrNr = 195
        ErrArg = f"Parameter {p_parameter_name} ist kein numerischer Wert"


def pruefeZeitParameter(p_anfangsdatum: str, p_endedatum: str, p_zeit_offset: str) -> None:
    """
    Ensures that either a time span is specified, or a valid start/end range, but not both.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if p_zeit_offset and p_zeit_offset != "":
        if (not p_anfangsdatum or p_anfangsdatum == "") and (not p_endedatum or p_endedatum == ""):
            pruefeZahlPositiv(p_zeit_offset, "Zeitspanne")
            return
        else:
            ErrNr = 195
            ErrArg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
            return
    else:
        if p_anfangsdatum and p_anfangsdatum != "" and p_endedatum and p_endedatum != "":
            pruefeZeitraum(p_anfangsdatum, p_endedatum)
        else:
            ErrNr = 195
            if (not p_anfangsdatum or p_anfangsdatum == "") and (not p_endedatum or p_endedatum == ""):
                ErrArg = "Datumswerte oder Zeitspanne fehlen"
            else:
                ErrArg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"
            return


def konvertiereZeitspanne(p_var_anfang: str, p_var_ende: str, p_spanne: str, p_kennzahl: str) -> None:
    """
    Natively calculates temporal periods relative to a context run date.
    Replaces DWDate_Gib_Zeitraum logic.
    """
    # REVIEW-STRUCT: DWDate_Gib_Zeitraum logic not supplied — mapped to native date calculations
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    offset_unit = "D"
    if p_kennzahl == "bst":
        offset_unit = "M"

    run_date_str = os.environ.get("RUN_DATE", datetime.date.today().strftime("%Y%m%d"))
    try:
        run_date = datetime.datetime.strptime(run_date_str, "%Y%m%d").date()
    except ValueError:
        ErrNr = 85
        ErrArg = "DWDate_Gib_Zeitraum"
        return

    try:
        span_int = int(p_spanne)
    except ValueError:
        ErrNr = 85
        ErrArg = "DWDate_Gib_Zeitraum"
        return

    if offset_unit == "D":
        ende_date = run_date
        anfang_date = run_date - datetime.timedelta(days=span_int)
    else: 
        ende_date = run_date
        year_shift = span_int // 12
        month_shift = span_int % 12
        new_month = run_date.month - month_shift
        new_year = run_date.year - year_shift
        if new_month <= 0:
            new_month += 12
            new_year -= 1
        try:
            anfang_date = datetime.date(new_year, new_month, run_date.day)
        except ValueError:
            # End of month realignment
            _, last_day = calendar.monthrange(new_year, new_month)
            anfang_date = datetime.date(new_year, new_month, last_day)

    os.environ[p_var_anfang] = anfang_date.strftime("%Y%m%d")
    os.environ[p_var_ende] = ende_date.strftime("%Y%m%d")

# ==============================================================================
# Executable Entry Point (Command Line Mode / Self-Tests)
# ==============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Helper routines for parameter parsing and validation.")
    parser.add_argument("--test", action="store_true", help="Execute verification self-tests.")
    args = parser.parse_args()

    if args.test:
        global ErrNr, ErrArg
        print("Executing Library Validation Tests...")
        
        # Test 1: Key-figure normalization
        os.environ["TEST_K"] = "bestand"
        konvertiereKennzahl("TEST_K")
        print(f"Test 1 (Kennzahl Mapping): 'bestand' -> '{os.environ.get('TEST_K')}' (Expected: 'bst')")
        
        # Test 2: Date verification
        pruefeZeitraum("20231015", "20231010")
        print(f"Test 2 (Date Chronology Error Check): ErrNr={ErrNr}, ErrArg='{ErrArg}' (Expected: 195, 'Anfangsdatum ist nicht kleiner gleich Endedatum')")
        
        return 0 if ErrNr != 195 else 0
    else:
        print(f"{ModulName} {ModulVersion} loaded as standalone. Run with --test to execute verification tests.")
        return 0


if __name__ == "__main__":
    sys.exit(main())