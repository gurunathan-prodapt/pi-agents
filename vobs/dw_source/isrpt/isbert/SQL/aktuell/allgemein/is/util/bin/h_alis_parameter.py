#!/usr/bin/env python3
import os
import sys
import datetime

# Global variables mirroring the shell script's state
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"

# Initialize global error state from environment variables if present
ErrNr = int(os.environ.get("ErrNr", 0))
ErrArg = os.environ.get("ErrArg", "")

def set_error(nr, arg):
    """Sets the global and process-level environment error state."""
    global ErrNr, ErrArg
    ErrNr = nr
    ErrArg = arg
    os.environ["ErrNr"] = str(nr)
    os.environ["ErrArg"] = arg

def get_error():
    """Gets the global error state, keeping it in sync with the environment."""
    global ErrNr, ErrArg
    ErrNr = int(os.environ.get("ErrNr", ErrNr))
    ErrArg = os.environ.get("ErrArg", ErrArg)
    return ErrNr, ErrArg

# REVIEW-STRUCT: external function DWDate_Datum_Check not supplied in this extraction — behaviour simulated via standard datetime parsing in Python.
def DWDate_Datum_Check(datum_str, format_mask):
    """
    Simulates DWDate_Datum_Check.
    Returns 0 if valid according to format_mask, 1 otherwise.
    """
    if format_mask == "YYYYMMDD":
        py_format = "%Y%m%d"
    else:
        py_format = "%Y%m%d"
        
    try:
        datetime.datetime.strptime(datum_str, py_format)
        return 0
    except ValueError:
        return 1

# REVIEW-STRUCT: external function DWDate_Datum_LE not supplied in this extraction — behaviour simulated via native Python comparison operators.
def DWDate_Datum_LE(datum1_str, datum2_str):
    """
    Simulates DWDate_Datum_LE.
    Returns 0 if datum1 <= datum2, 1 otherwise.
    """
    try:
        d1 = datetime.datetime.strptime(datum1_str, "%Y%m%d")
        d2 = datetime.datetime.strptime(datum2_str, "%Y%m%d")
        if d1 <= d2:
            return 0
        else:
            return 1
    except ValueError:
        return 1

# REVIEW-STRUCT: external function DWDate_Gib_Zeitraum not supplied in this extraction — behaviour simulated using Python's datetime.
def DWDate_Gib_Zeitraum(offset, unit, format_mask):
    """
    Simulates DWDate_Gib_Zeitraum.
    Returns (anfangsdatum, endedatum) as strings.
    """
    today = datetime.datetime.now()
    
    if unit == "D":
        delta = datetime.timedelta(days=offset)
        past_date = today + delta
    elif unit == "M":
        # Approximate monthly subtraction safely handling month overflow
        total_months = today.month - 1 + offset
        year_offset = total_months // 12
        new_month = (total_months % 12) + 1
        new_year = today.year + year_offset
        
        new_day = today.day
        while True:
            try:
                past_date = datetime.datetime(new_year, new_month, new_day, today.hour, today.minute, today.second)
                break
            except ValueError:
                new_day -= 1
    else:
        past_date = today

    if offset < 0:
        anfang = past_date.strftime("%Y%m%d")
        ende = today.strftime("%Y%m%d")
    else:
        anfang = today.strftime("%Y%m%d")
        ende = past_date.strftime("%Y%m%d")
        
    return anfang, ende

# Step 1: pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name, param_var):
    global ErrNr, ErrArg
    ErrNr, ErrArg = get_error()
    if ErrNr != 0:
        return

    if not param_name or not param_var:
        set_error(196, f"{ModulName} {ModulVersion} pruefeParameterGesetzt")
        return

    param_wert = os.environ.get(param_var, "")

    if not param_wert:
        set_error(194, param_name)

# Step 2: konvertiereKennzahl
def konvertiereKennzahl(VarName):
    global ErrNr, ErrArg
    ErrNr, ErrArg = get_error()
    if ErrNr != 0:
        return

    if not VarName:
        set_error(196, f"{ModulName} {ModulVersion} konvertiereKennzahl")
        return

    Kennzahl = os.environ.get(VarName, "").lower()

    if Kennzahl == "zugang":
        Kennzahl = "zug"
    elif Kennzahl == "abgang":
        Kennzahl = "abg"
    elif Kennzahl == "abgang_zukunft":
        Kennzahl = "abz"
    elif Kennzahl == "bestand":
        Kennzahl = "bst"
    elif Kennzahl == "tarifwechsel":
        Kennzahl = "twe"
    elif Kennzahl == "plan":
        Kennzahl = "pln"
    elif Kennzahl == "gutschrift":
        Kennzahl = "gut"
    elif Kennzahl == "aufladung":
        Kennzahl = "auf"
    elif Kennzahl == "restguthaben":
        Kennzahl = "rst"
    elif Kennzahl == "teilnehmerverbindungsdaten":
        Kennzahl = "tvd"
    elif Kennzahl == "uskonto":
        Kennzahl = "usk"
    elif Kennzahl == "usteilnehmer":
        Kennzahl = "ust"
    elif Kennzahl == "leistungsklasse":
        Kennzahl = "lkl"
    elif Kennzahl == "loeschung":
        Kennzahl = "loe"
    elif Kennzahl == "reaktivierung":
        Kennzahl = "rak"
    elif Kennzahl == "standard_rechnung":
        Kennzahl = "srs"
    elif Kennzahl == "standard_gutschrift":
        Kennzahl = "sgs"
    elif Kennzahl == "gutschrift_rv":
        Kennzahl = "sg_rv"
    elif Kennzahl == "rechnungen_rv_dpps":
        Kennzahl = "sr_rv_dpps"
    elif Kennzahl == "bewegart":
        Kennzahl = "bwa"
    elif Kennzahl == "kundenstamm":
        Kennzahl = "ksd"
    elif Kennzahl == "mahnstufe":
        Kennzahl = "mahn"
    elif Kennzahl == "metadatenstruktur":
        Kennzahl = "mds"
    elif Kennzahl == "d1news":
        Kennzahl = "d1n"
    elif Kennzahl == "rubrik":
        Kennzahl = "rub"
    elif Kennzahl == "liefermodus":
        Kennzahl = "lmo"
    elif Kennzahl == "netznutzungsklassen":
        Kennzahl = "nnk"
    elif Kennzahl == "tagesverkehrskurven":
        Kennzahl = "tvk"
    elif Kennzahl == "gespraechsziele":
        Kennzahl = "gz"
    elif Kennzahl == "gespraechslaengenverteilung":
        Kennzahl = "glv"
    elif Kennzahl == "zonenkennung":
        Kennzahl = "zonek"
    elif Kennzahl == "zonentyp":
        Kennzahl = "zonet"
    elif Kennzahl == "netznutzungsklassentyp":
        Kennzahl = "nnkt"
    elif Kennzahl == "tarifart":
        Kennzahl = "trfa"
    elif Kennzahl == "gespraechstyp":
        Kennzahl = "gtyp"
    elif Kennzahl == "basisdienst":
        Kennzahl = "basisd"
    elif Kennzahl == "nationalinternational":
        Kennzahl = "natint"
    elif Kennzahl == "glaengenintervall":
        Kennzahl = "glint"
    else:
        set_error(198, Kennzahl)
        Kennzahl = "???"

    os.environ[VarName] = Kennzahl

# Step 3: konvertiereSystem
def konvertiereSystem(VarName):
    global ErrNr, ErrArg
    ErrNr, ErrArg = get_error()
    if ErrNr != 0:
        return

    if not VarName:
        set_error(196, f"{ModulName} {ModulVersion} konvertiereSystem")
        return

    System = os.environ.get(VarName, "").lower()

    if System in ["sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"]:
        pass
    else:
        set_error(195, f"Unbekannte Datenherkunft {System} !")
        System = "???"

    os.environ[VarName] = System

# Step 4: konvertiereSDName
def konvertiereSDName(VarName):
    global ErrNr, ErrArg
    ErrNr, ErrArg = get_error()
    if ErrNr != 0:
        return

    if not VarName:
        set_error(196, f"{ModulName} {ModulVersion} konvertiereSDSystem")
        return

    System = os.environ.get(VarName, "").lower()

    if System == "vo":
        pass
    elif System == "rahmenvertrag":
        System = "rv"
    elif System == "tarif":
        System = "trf"
    elif System == "tstatus":
        System = "ts"
    elif System == "zahlmodus":
        System = "zm"
    elif System == "kdg_grund":
        System = "kdg"
    elif System == "gutschrift":
        System = "gut"
    elif System == "aufladung":
        System = "auf"
    elif System == "leistung":
        System = "l_leist"
    elif System == "gutschrift_grund":
        System = "l_gutgr"
    elif System == "sap_gutschrift_grund":
        System = "sap_l_gutgr"
    elif System == "produkt":
        System = "l_prod"
    elif System == "mahnverfahren_sapist":
        System = "l_mahnv_ist"
    elif System == "mahnverfahren_sapfi":
        System = "l_mahnv_fi"
    elif System == "mahnstufentyp_sapist":
        System = "l_mahnstyp_ist"
    elif System == "bewegart":
        System = "bwa"
    else:
        set_error(195, f"Unbekannte Stammdaten-Datenherkunft {System} !")
        System = "???"

    os.environ[VarName] = System

# Step 5: konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(VarName):
    global ErrNr, ErrArg
    ErrNr, ErrArg = get_error()
    if ErrNr != 0:
        return

    if not VarName:
        set_error(196, f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra")
        return

    Stufe = os.environ.get(VarName, "").lower()

    if Stufe == "zusammenfuehrung":
        Stufe = "mrg"
    elif Stufe == "befuellung":
        Stufe = "fill"
    else:
        set_error(195, f"Unbekannte Stufenangabe {Stufe} !")
        Stufe = "???"

    os.environ[VarName] = Stufe

# Step 6: pruefeSystemKennzahl
def pruefeSystemKennzahl(System, Kennzahl):
    global ErrNr, ErrArg
    ErrNr, ErrArg = get_error()
    if ErrNr != 0:
        return

    if not System or not Kennzahl:
        set_error(196, f"{ModulName} {ModulVersion} pruefeSystemKennzahl")
        return

    local_ErrArg = ""

    if System != "nnv" and (Kennzahl == "tvd" or Kennzahl == "lkl"):
        local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "carmen":
        if Kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "sap":
        if Kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "dpps":
        if Kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "ctel":
        if Kennzahl not in ["abg", "bst", "zug", "twe"]:
            local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "xtra":
        if Kennzahl != "rst":
            local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "d1":
        if Kennzahl in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            local_ErrArg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "nnv":
        if Kennzahl not in ["tvd", "lkl"]:
            local_ErrArg = f