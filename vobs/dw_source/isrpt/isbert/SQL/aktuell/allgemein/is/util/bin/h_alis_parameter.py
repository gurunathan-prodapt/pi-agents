#!/usr/bin/env python3

import os
import sys
import datetime
from dateutil.relativedelta import relativedelta
import argparse

# Module Metadata
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"

# Global Error State to mimic legacy shell behavior
ErrNr = 0
ErrArg = ""

# Step 1: pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name, param_var):
    """
    Checks if the specified environment variable contains a value.
    If not, sets the appropriate global error state.
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

# Step 2: konvertiereKennzahl
def konvertiereKennzahl(var_name):
    """
    Converts a long key figure description to a standardized 3-character abbreviation.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereKennzahl"
        return

    kennzahl = os.environ.get(var_name, "").lower()

    if kennzahl == "zugang":
        kennzahl = "zug"
    elif kennzahl == "abgang":
        kennzahl = "abg"
    elif kennzahl == "abgang_zukunft":
        kennzahl = "abz"
    elif kennzahl == "bestand":
        kennzahl = "bst"
    elif kennzahl == "tarifwechsel":
        kennzahl = "twe"
    elif kennzahl == "plan":
        kennzahl = "pln"
    elif kennzahl == "gutschrift":
        kennzahl = "gut"
    elif kennzahl == "aufladung":
        kennzahl = "auf"
    elif kennzahl == "restguthaben":
        kennzahl = "rst"
    elif kennzahl == "teilnehmerverbindungsdaten":
        kennzahl = "tvd"
    elif kennzahl == "uskonto":
        kennzahl = "usk"
    elif kennzahl == "usteilnehmer":
        kennzahl = "ust"
    elif kennzahl == "leistungsklasse":
        kennzahl = "lkl"
    elif kennzahl == "loeschung":
        kennzahl = "loe"
    elif kennzahl == "reaktivierung":
        kennzahl = "rak"
    elif kennzahl == "standard_rechnung":
        kennzahl = "srs"
    elif kennzahl == "standard_gutschrift":
        kennzahl = "sgs"
    elif kennzahl == "gutschrift_rv":
        kennzahl = "sg_rv"
    elif kennzahl == "rechnungen_rv_dpps":
        kennzahl = "sr_rv_dpps"
    elif kennzahl == "bewegart":
        kennzahl = "bwa"
    elif kennzahl == "kundenstamm":
        kennzahl = "ksd"
    elif kennzahl == "mahnstufe":
        kennzahl = "mahn"
    elif kennzahl == "metadatenstruktur":
        kennzahl = "mds"
    elif kennzahl == "d1news":
        kennzahl = "d1n"
    elif kennzahl == "rubrik":
        kennzahl = "rub"
    elif kennzahl == "liefermodus":
        kennzahl = "lmo"
    elif kennzahl == "netznutzungsklassen":
        kennzahl = "nnk"
    elif kennzahl == "tagesverkehrskurven":
        kennzahl = "tvk"
    elif kennzahl == "gespraechsziele":
        kennzahl = "gz"
    elif kennzahl == "gespraechslaengenverteilung":
        kennzahl = "glv"
    elif kennzahl == "zonenkennung":
        kennzahl = "zonek"
    elif kennzahl == "zonentyp":
        kennzahl = "zonet"
    elif kennzahl == "netznutzungsklassentyp":
        kennzahl = "nnkt"
    elif kennzahl == "tarifart":
        kennzahl = "trfa"
    elif kennzahl == "gespraechstyp":
        kennzahl = "gtyp"
    elif kennzahl == "basisdienst":
        kennzahl = "basisd"
    elif kennzahl == "nationalinternational":
        kennzahl = "natint"
    elif kennzahl == "glaengenintervall":
        kennzahl = "glint"
    else:
        ErrNr = 198
        ErrArg = kennzahl
        kennzahl = "???"

    os.environ[var_name] = kennzahl

# Step 3: konvertiereSystem
def konvertiereSystem(var_name):
    """
    Normalizes a source system name to lowercase and validates it.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSystem"
        return

    system = os.environ.get(var_name, "").lower()

    valid_systems = {
        "sap", "carmen", "dpps", "d1", "xtra", "ctel",
        "nnv", "dwh", "brunet", "sigma"
    }

    if system in valid_systems:
        pass
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Datenherkunft {system} !"
        system = "???"

    os.environ[var_name] = system

# Step 4: konvertiereSDName
def konvertiereSDName(var_name):
    """
    Normalizes a master data source name to standard abbreviations.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        # Note: KSH source literally says "konvertiereSDSystem" here
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSDSystem"
        return

    system = os.environ.get(var_name, "").lower()

    if system == "vo":
        pass
    elif system == "rahmenvertrag":
        system = "rv"
    elif system == "tarif":
        system = "trf"
    elif system == "tstatus":
        system = "ts"
    elif system == "zahlmodus":
        system = "zm"
    elif system == "kdg_grund":
        system = "kdg"
    elif system == "gutschrift":
        system = "gut"
    elif system == "aufladung":
        system = "auf"
    elif system == "leistung":
        system = "l_leist"
    elif system == "gutschrift_grund":
        system = "l_gutgr"
    elif system == "sap_gutschrift_grund":
        system = "sap_l_gutgr"
    elif system == "produkt":
        system = "l_prod"
    elif system == "mahnverfahren_sapist":
        system = "l_mahnv_ist"
    elif system == "mahnverfahren_sapfi":
        system = "l_mahnv_fi"
    elif system == "mahnstufentyp_sapist":
        system = "l_mahnstyp_ist"
    elif system == "bewegart":
        system = "bwa"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stammdaten-Datenherkunft {system} !"
        system = "???"

    os.environ[var_name] = system

# Step 5: konvertiereAufbStufeXtra
def konvertiereAufbStufeXtra(var_name):
    """
    Converts Xtra aggregation step name to a standardized key.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not var_name:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra"
        return

    stufe = os.environ.get(var_name, "").lower()

    if stufe == "zusammenfuehrung":
        stufe = "mrg"
    elif stufe == "befuellung":
        stufe = "fill"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stufenangabe {stufe} !"
        stufe = "???"

    os.environ[var_name] = stufe

# Step 6: pruefeSystemKennzahl
def pruefeSystemKennzahl(system, kennzahl):
    """
    Checks if a combination of system and KPI is valid and supported.
    """
    global ErrNr, ErrArg
    if ErrNr != 0:
        return

    if not system or not kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeSystemKennzahl"
        return

    err_arg_tmp = ""

    if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
        err_arg_tmp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "carmen":
        if kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            err_arg_tmp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sap":
        if kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            err_arg_tmp = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dpps":
        if kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            err_arg_tmp = f