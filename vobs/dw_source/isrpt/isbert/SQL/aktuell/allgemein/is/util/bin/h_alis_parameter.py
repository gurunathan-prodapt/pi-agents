#!/usr/bin/env python3
import os
import sys
import datetime
from dateutil.relativedelta import relativedelta

# Global module parameters as per KSH source environment logic
ErrNr = 0
ErrArg = ""
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"

# ==============================================================================
# Step 1: pruefeParameterGesetzt
# ==============================================================================
def pruefeParameterGesetzt(param_name, param_var):
    """
    Checks if the specified environment variable is set and contains a value.
    If not, updates global ErrNr and ErrArg.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not param_name or not param_var:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeParameterGesetzt"
        return

    param_wert = os.environ.get(param_var, "")

    if not param_wert:
        ErrNr = 194
        ErrArg = param_name

# ==============================================================================
# Step 2: konvertiereKennzahl
# ==============================================================================
def konvertiereKennzahl(VarName):
    """
    Converts descriptive metric names into normalized abbreviations.
    Modifies the variable in os.environ in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereKennzahl"
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
        ErrNr = 198
        ErrArg = Kennzahl
        Kennzahl = "???"

    os.environ[VarName] = Kennzahl

# ==============================================================================
# Step 3: konvertiereSystem
# ==============================================================================
def konvertiereSystem(VarName):
    """
    Validates and normalizes source system names.
    Modifies the variable in os.environ in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSystem"
        return

    System = os.environ.get(VarName, "").lower()

    if System in ["sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"]:
        pass
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Datenherkunft {System} !"
        System = "???"

    os.environ[VarName] = System

# ==============================================================================
# Step 4: konvertiereSDName
# ==============================================================================
def konvertiereSDName(VarName):
    """
    Converts master data system labels into standard abbreviations.
    Modifies the variable in os.environ in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSDSystem"
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
        ErrNr = 195
        ErrArg = f"Unbekannte Stammdaten-Datenherkunft {System} !"
        System = "???"

    os.environ[VarName] = System

# ==============================================================================
# Step 5: konvertiereAufbStufeXtra
# ==============================================================================
def konvertiereAufbStufeXtra(VarName):
    """
    Normalizes processing phase names for Xtra.
    Modifies the variable in os.environ in-place.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra"
        return

    Stufe = os.environ.get(VarName, "").lower()

    if Stufe == "zusammenfuehrung":
        Stufe = "mrg"
    elif Stufe == "befuellung":
        Stufe = "fill"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stufenangabe {Stufe} !"
        Stufe = "???"

    os.environ[VarName] = Stufe

# ==============================================================================
# Step 6: pruefeSystemKennzahl
# ==============================================================================
def pruefeSystemKennzahl(System, Kennzahl):
    """
    Validates if the specified system and metric combination is supported.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not System or not Kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeSystemKennzahl"
        return

    err_arg_local = ""

    if System != "nnv" and (Kennzahl == "tvd" or Kennzahl == "lkl"):
        err_arg_local = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "carmen":
        if Kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            err_arg_local = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "sap":
        if Kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            err_arg_local = f