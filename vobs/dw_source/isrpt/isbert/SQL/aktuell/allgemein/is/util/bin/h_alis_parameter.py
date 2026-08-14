#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess

# Global Module Variables
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"

# Global Error Context state (replicating the legacy global state contract)
ErrNr = 0
ErrArg = ""

def reset_errors():
    global ErrNr, ErrArg
    ErrNr = 0
    ErrArg = ""

# Step 1: helper to simulate the global guard condition
def _has_error():
    global ErrNr
    return ErrNr != 0

# Step 2: pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name: str, param_var: str):
    """
    Checks if the environment variable named param_var is set and not empty.
    If not, sets ErrNr to 194 and ErrArg to param_name.
    """
    global ErrNr, ErrArg
    if _has_error():
        return

    if not param_name or not param_var:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeParameterGesetzt"
        return

    param_wert = os.environ.get(param_var, "")

    if not param_wert:
        ErrNr = 194
        ErrArg = param_name

# Step 3: konvertiereKennzahl
def konvertiereKennzahl(kennzahl_val: str) -> str:
    """
    Converts the metric name (Kennzahl) to its shortcode.
    """
    global ErrNr, ErrArg
    if _has_error():
        return kennzahl_val

    if not kennzahl_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereKennzahl"
        return "???"

    kennzahl = kennzahl_val.lower()
    
    mapping = {
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

    if kennzahl in mapping:
        return mapping[kennzahl]
    else:
        ErrNr = 198
        ErrArg = kennzahl
        return "???"

# Step 4: konvertiereSystem
def konvertiereSystem(system_val: str) -> str:
    """
    Converts/normalizes the system name to its lowercase shortcode.
    """
    global ErrNr, ErrArg
    if _has_error():
        return system_val

    if not system_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSystem"
        return "???"

    system = system_val.lower()
    allowed_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if system in allowed_systems:
        return system
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Datenherkunft {system} !"
        return "???"

# Step 5: konvertiereSDName
def konvertiereSDName(system_val: str) -> str:
    """
    Converts/normalizes Master Data (Stammdaten) source names.
    """
    global ErrNr, ErrArg
    if _has_error():
        return system_val

    if not system_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSDSystem"
        return "???"

    system = system_val.lower()
    
    mapping = {
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

    if system in mapping:
        return mapping[system]
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stammdaten-Datenherkunft {system} !"
        return "???"

# Step 6: konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(stufe_val: str) -> str:
    """
    Normalizes stage names for Xtra.
    """
    global ErrNr, ErrArg
    if _has_error():
        return stufe_val

    if not stufe_val:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra"
        return "???"

    stufe = stufe_val.lower()
    if stufe == "zusammenfuehrung":
        return "mrg"
    elif stufe == "befuellung":
        return "fill"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stufenangabe {stufe} !"
        return "???"

# Step 7: pruefeSystemKennzahl
def pruefeSystemKennzahl(system: str, kennzahl: str):
    """
    Validates combinations of system and metric.
    """
    global ErrNr, ErrArg
    if _has_error():
        return

    if not system or not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeSystemKennzahl"
        return

    invalid_combination = False
    
    if system != "nnv" and kennzahl in ["tvd", "lkl"]:
        invalid_combination = True
    elif system == "carmen":
        if kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            invalid_combination = True
    elif system == "sap":
        if kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            invalid_combination = True
    elif system == "dpps":
        if kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            invalid_combination = True
    elif system == "ctel":
        if kennzahl not in ["abg", "bst", "zug", "twe"]:
            invalid_combination = True
    elif system == "xtra":
        if kennzahl != "rst":
            invalid_combination = True
    elif system == "d1":
        if kennzahl in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            invalid_combination = True
    elif system == "nnv":
        if kennzahl not in ["tvd", "lkl"]:
            invalid_combination = True
    elif system == "dwh":
        if kennzahl != "mds":
            invalid_combination = True
    elif system == "brunet":
        if kennzahl not in ["d1n", "rub", "lmo"]:
            invalid_combination = True
    elif system == "sigma":
        allowed_sigma = ["nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]
        if kennzahl not in allowed_sigma:
            invalid_combination = True

    if invalid_combination:
        ErrArg = f"Ungueltige Kombination {system} {kennzahl}"
        ErrNr = 195

# Step 8: gibBereich
def gibBereich(kennzahl: str) -> str:
    """
    Returns the area for a given metric.
    """
    global ErrNr, ErrArg
    if _has_error():
        return ""

    if not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich"
        return ""

    list_tn = {"abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"}
    list_us = {"gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"}
    list_gd = {"tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"}
    list_sd = {"ksd", "bwa"}
    list_md = {"mds"}

    if kennzahl in list_tn:
        return "tn"
    elif kennzahl in list_us:
        return "us"
    elif kennzahl in list_gd:
        return "gd"
    elif kennzahl in list_sd:
        return "sd"
    elif kennzahl in list_md:
        return "md"
    else:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich - Kuerzel '{kennzahl}' unbekannt"
        return ""

# Step 9: gibIntervall
def gibIntervall(kennzahl: str) -> str:
    """
    Returns the granularity interval (daily 't' or monthly 'm') for a metric.
    """
    global ErrNr, ErrArg
    if _has_error():
        return ""

    if not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall"
        return ""

    list_t = {"abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"}
    list_m = {"bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"}

    if kennzahl in list_t:
        return "t"
    elif kennzahl in list_m:
        return "m"
    else:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall - Kuerzel '{kennzahl}' unbekannt"
        return ""

# Step 10: pruefeZeitraum
def pruefeZeitraum(anfang: str, ende: str):
    """
    Checks if start and end dates represent a valid period.
    """
    global ErrNr, ErrArg
    if _has_error():
        return

    if not anfang or not ende:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeZeitraum"
        return

    format_mask = "YYYYMMDD"
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_file = f"/tmp/tmp_h_alis_parameter.py_{timestamp}_{os.getpid()}.tmp"

    err_arg_local = ""

    try:
        with open(tmp_file, "a+") as f:
            for label, val in [("Anfang", anfang), ("Ende", ende)]:
                # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with GCP-native equivalent if needed
                res = subprocess.run(["DWDate_Datum_Check", val, format_mask], stdout=f, stderr=subprocess.STDOUT)
                if res.returncode != 0:
                    err_arg_local = f"{label}datum entspricht nicht dem Format {format_mask}"
                    break

            if not err_arg_local:
                # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with GCP-native equivalent if needed
                res_le = subprocess.run(["DWDate_Datum_LE", anfang, ende], stdout=f, stderr=subprocess.STDOUT)
                if res_le.returncode != 0:
                    err_arg_local = "Anfangsdatum ist nicht kleiner gleich Endedatum"
                    
        if err_arg_local:
            ErrNr = 195
            ErrArg = err_arg_local
            if os.path.exists(tmp_file):
                with open(tmp_file, "r") as f_read:
                    print(f_read.read(), file=sys.stderr)
    finally:
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

# Step 11: pruefeZahlPositiv
def pruefeZahlPositiv(p_Zahl, p_ParameterName: str):
    """
    Checks if the given parameter is numeric and >= 0.
    """
    global ErrNr, ErrArg
    try:
        val = int(p_Zahl)
        if val < 0:
            ErrNr = 195
            ErrArg = f"Parameter {p_ParameterName} muss groesser gleich 0 sein"
    except (ValueError, TypeError):
        ErrNr = 195
        ErrArg = f"Parameter {p_ParameterName} ist kein numerischer Wert"

# Step 12: pruefeZeitParameter
def pruefeZeitParameter(p_Anfangsdatum: str, p_Endedatum: str, p_ZeitOffset: str):
    """
    Validates either starting/end date or a relative offset is given.
    """
    global ErrNr, ErrArg
    if _has_error():
        return

    if p_ZeitOffset:
        if not p_Anfangsdatum and not p_Endedatum:
            pruefeZahlPositiv(p_ZeitOffset, "Zeitspanne")
            return
        else:
            ErrNr = 195
            ErrArg = "Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden"
            return
    else:
        if p_Anfangsdatum and p_Endedatum:
            pruefeZeitraum(p_Anfangsdatum, p_Endedatum)
        else:
            ErrNr = 195
            if not p_Anfangsdatum and not p_Endedatum:
                ErrArg = "Datumswerte oder Zeitspanne fehlen"
            else:
                ErrArg = "Sowohl Anfang- als auch Endedatum muessen angegeben werden"
            return

# Step 13: konvertiereZeitspanne
def konvertiereZeitspanne(p_Spanne: str, p_Kennzahl: str) -> tuple:
    """
    Converts a relative offset into an explicit date range.
    """
    global ErrNr, ErrArg
    if _has_error():
        return ("", "")

    offset_unit = "D"
    if p_Kennzahl == "bst":
        offset_unit = "M"

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_file = f"/tmp/tmp_h_alis_parameter.py_{timestamp}_{os.getpid()}.tmp"
    
    anfangsdatum = ""
    endedatum = ""

    try:
        negative_spanne = f"-{p_Spanne}"
        # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with GCP-native equivalent if needed
        res = subprocess.run(
            ["DWDate_Gib_Zeitraum", negative_spanne, offset_unit, "YYYYMMDD", "Anfangsdatum", "Endedatum"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
            
        if res.returncode != 0:
            ErrNr = 85
            ErrArg = "DWDate_Gib_Zeitraum"
            print(res.stderr, file=sys.stderr)
        else:
            lines = res.stdout.strip().split("
")
            for line in lines:
                if "Anfangsdatum=" in line:
                    anfangsdatum = line.split("=")[1].strip()
                elif "Endedatum=" in line:
                    endedatum = line.split("=")[1].strip()
    finally:
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass
            
    return (anfangsdatum, endedatum)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Helper for parsing parameter validations")
    parser.add_argument("--test-kennzahl", help="Test konvertiereKennzahl")
    parser.add_argument("--test-system", help="Test konvertiereSystem")
    parser.add_argument("--test-sdname", help="Test konvertiereSDName")
    parser.add_argument("--test-bereich", help="Test gibBereich")
    parser.add_argument("--test-intervall", help="Test gibIntervall")
    parser.add_argument("--test-zeitraum", nargs=2, metavar=("ANFANG", "ENDE"), help="Test pruefeZeitraum")
    parser.add_argument("--test-zeitparam", nargs=3, metavar=("ANFANG", "ENDE", "OFFSET"), help="Test pruefeZeitParameter")

    args = parser.parse_args()

    # If no arguments are provided, just exit successfully or print module status
    if not any(vars(args).values()):
        print(f"Module {ModulName} ({ModulVersion}) loaded successfully.", file=sys.stderr)
        return 0

    global ErrNr, ErrArg

    if args.test_kennzahl:
        res = konvertiereKennzahl(args.test_kennzahl)
        print(f"Result: {res}, ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    if args.test_system:
        res = konvertiereSystem(args.test_system)
        print(f"Result: {res}, ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    if args.test_sdname:
        res = konvertiereSDName(args.test_sdname)
        print(f"Result: {res}, ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    if args.test_bereich:
        res = gibBereich(args.test_bereich)
        print(f"Result: {res}, ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    if args.test_intervall:
        res = gibIntervall(args.test_intervall)
        print(f"Result: {res}, ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    if args.test_zeitraum:
        pruefeZeitraum(args.test_zeitraum[0], args.test_zeitraum[1])
        print(f"ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    if args.test_zeitparam:
        anf = args.test_zeitparam[0] if args.test_zeitparam[0] != "None" else ""
        end = args.test_zeitparam[1] if args.test_zeitparam[1] != "None" else ""
        off = args.test_zeitparam[2] if args.test_zeitparam[2] != "None" else ""
        pruefeZeitParameter(anf, end, off)
        print(f"ErrNr: {ErrNr}, ErrArg: {ErrArg}")

    return ErrNr

if __name__ == "__main__":
    sys.exit(main())