#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime, timedelta

# Zweck:
#    Hilfsroutinen fuer das Parsen von Parametern
# ModulName und Version entsprechend dem originalen KornShell-Skript
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"

# Globale Fehler-Variablen, die von den Funktionen modifiziert werden
ErrNr = 0
ErrArg = ""

def reset_error():
    """
    Setzt den globalen Fehlerstatus zurueck.
    """
    global ErrNr, ErrArg
    ErrNr = 0
    ErrArg = ""

def pruefeParameterGesetzt(param_name, param_var):
    """
    prueft, ob die uebergebene Environment-Variable einen Wert
    beinhaltet.
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

def konvertiereKennzahl(VarName):
    """
    konvertiert die Kennzahlbezeichnung basierend auf dem Namenskonzept
    in eine gueltige Abkuerzung fuer Kennzahlen.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereKennzahl"
        return

    raw_val = os.environ.get(VarName, "")
    Kennzahl = raw_val.lower()

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

    if Kennzahl in mapping:
        Kennzahl = mapping[Kennzahl]
    else:
        ErrNr = 198
        ErrArg = raw_val
        Kennzahl = "???"

    os.environ[VarName] = Kennzahl

def konvertiereSystem(VarName):
    """
    konvertiert die Systembezeichnung basierend auf dem Namenskonzept
    in eine gueltige Abkuerzung fuer Liefersysteme.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSystem"
        return

    raw_val = os.environ.get(VarName, "")
    System = raw_val.lower()

    valid_systems = {"sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"}

    if System not in valid_systems:
        ErrNr = 195
        ErrArg = f"Unbekannte Datenherkunft {System} !"
        System = "???"

    os.environ[VarName] = System

def konvertiereSDName(VarName):
    """
    konvertiert die Systembezeichnung basierend auf dem Namenskonzept
    in eine gueltige Abkuerzung fuer Stammdaten-Liefersysteme.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereSDSystem"
        return

    raw_val = os.environ.get(VarName, "")
    System = raw_val.lower()

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

    if System in mapping:
        System = mapping[System]
    elif System == "vo":
        pass
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stammdaten-Datenherkunft {System} !"
        System = "???"

    os.environ[VarName] = System

def konvertiereAufbStufeXtra(VarName):
    """
    konvertiert die Aufbereitungsstufenname in eine normierte Abkuerzung.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not VarName:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra"
        return

    raw_val = os.environ.get(VarName, "")
    Stufe = raw_val.lower()

    if Stufe == "zusammenfuehrung":
        Stufe = "mrg"
    elif Stufe == "befuellung":
        Stufe = "fill"
    else:
        ErrNr = 195
        ErrArg = f"Unbekannte Stufenangabe {Stufe} !"
        Stufe = "???"

    os.environ[VarName] = Stufe

def pruefeSystemKennzahl(System, Kennzahl):
    """
    prueft, ob die Kombination von System und Kennzahl erlaubt ist, d.h.
    vom IS unterstuetzt wird.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not System or not Kennzahl:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeSystemKennzahl"
        return

    local_err_arg = ""

    if System != "nnv" and (Kennzahl == "tvd" or Kennzahl == "lkl"):
        local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "carmen":
        if Kennzahl in ["twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "sap":
        if Kennzahl in ["zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk", "ust", "lkl", "loe", "rak", "ksd", "bwa"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "dpps":
        if Kennzahl in ["twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "ctel":
        if Kennzahl not in ["abg", "bst", "zug", "twe"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "xtra":
        if Kennzahl != "rst":
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "d1":
        if Kennzahl in ["gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv", "sr_rv_dpps", "bwa"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "nnv":
        if Kennzahl not in ["tvd", "lkl"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "dwh":
        if Kennzahl != "mds":
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "brunet":
        if Kennzahl not in ["d1n", "rub", "lmo"]:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"
    elif System == "sigma":
        sigma_valid = [
            "nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa",
            "gtyp", "basisd", "natint", "glint"
        ]
        if Kennzahl not in sigma_valid:
            local_err_arg = f"Ungueltige Kombination {System} {Kennzahl}"

    if local_err_arg:
        ErrNr = 195
        ErrArg = local_err_arg

def gibBereich(Kennzahl, VarBereich):
    """
    gibt in Abhaengigkeit zu einer Kennzahl/Eingangsgroesse den
    entsprechenden Bereich aus.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not Kennzahl or not VarBereich:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich"
        return

    list_tn = ["abg", "abz", "bst", "pln", "twe", "zug", "loe", "rak"]
    list_us = ["gut", "rst", "auf", "ust", "usk", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"]
    list_gd = ["tvd", "lkl", "d1n", "rub", "lmo", "nnk", "tvk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd", "natint", "glint"]
    list_sd = ["ksd", "bwa"]
    list_md = ["mds"]

    my_Bereich = ""
    if Kennzahl in list_tn:
        my_Bereich = "tn"
    elif Kennzahl in list_us:
        my_Bereich = "us"
    elif Kennzahl in list_gd:
        my_Bereich = "gd"
    elif Kennzahl in list_sd:
        my_Bereich = "sd"
    elif Kennzahl in list_md:
        my_Bereich = "md"

    if not my_Bereich:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibBereich - Kuerzel '{Kennzahl}' unbekannt"
        return

    os.environ[VarBereich] = my_Bereich

def gibIntervall(Kennzahl, VarIntervall):
    """
    gibt in Abhaengigkeit zu einer Kennzahl/Eingangsgroesse das
    entsprechenden Intervall (t,m) aus.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not Kennzahl or not VarIntervall:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall"
        return

    list_t = ["abg", "abz", "twe", "zug", "gut", "auf", "rst", "ust", "usk", "rak", "loe", "srs", "sgs", "ksd", "mahn", "mds", "tvk", "sr_rv_dpps", "gtyp", "basisd", "bwa"]
    list_m = ["bst", "pln", "tvd", "lkl", "sg_rv", "d1n", "rub", "lmo", "nnk", "gz", "glv", "zonek", "zonet", "nnkt", "trfa", "natint", "glint"]

    my_Intervall = ""
    if Kennzahl in list_t:
        my_Intervall = "t"
    elif Kennzahl in list_m:
        my_Intervall = "m"

    if not my_Intervall:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} gibIntervall - Kuerzel '{Kennzahl}' unbekannt"
        return

    os.environ[VarIntervall] = my_Intervall

def pruefeZeitraum(Anfang, Ende):
    """
    Die Funktion prueft, ob die beiden Parameter einen gueltigen
    Zeitraum beschreiben.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    if not Anfang or not Ende:
        ErrNr = 196
        ErrArg = f"{ModulName} {ModulVersion} pruefeZeitraum"
        return

    local_err_arg = ""

    # Datumsformat-Pruefungen
    try:
        dt_anfang = datetime.strptime(Anfang, "%Y%m%d")
    except ValueError:
        local_err_arg = "Anfangdatum entspricht nicht dem Format YYYYMMDD"

    if not local_err_arg:
        try:
            dt_ende = datetime.strptime(Ende, "%Y%m%d")
        except ValueError:
            local_err_arg = "Endedatum entspricht nicht dem Format YYYYMMDD"

    # Chronologische Pruefung
    if not local_err_arg:
        if dt_anfang > dt_ende:
            local_err_arg = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    if local_err_arg:
        ErrNr = 195
        ErrArg = local_err_arg

def pruefeZahlPositiv(p_Zahl, p_ParameterName):
    """
    prueft ob der uebergebene Parameter numerisch und >= 0 ist.
    """
    global ErrNr, ErrArg

    try:
        # Original checks both -ne 0 and -eq 0
        val = int(p_Zahl)
        if val < 0:
            ErrNr = 195
            ErrArg = f"Parameter {p_ParameterName} muss groesser gleich 0 sein"
    except (ValueError, TypeError):
        ErrNr = 195
        ErrArg = f"Parameter {p_ParameterName} ist kein numerischer Wert"

def pruefeZeitParameter(p_Anfangsdatum, p_Endedatum, p_ZeitOffset):
    """
    prueft ob genau Anfangs und Endedatum oder Zeitraum gesetzt und
    gueltig sind.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
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

def konvertiereZeitspanne(p_VarAnfang, p_VarEnde, p_Spanne, p_Kennzahl):
    """
    Berechnet aus der Zeitspanne und der Kennzahl Anfangs und Endedatum.
    """
    global ErrNr, ErrArg

    if ErrNr != 0:
        return

    Offset_Unit = "D"
    if p_Kennzahl == "bst":
        Offset_Unit = "M"

    try:
        spanne_val = int(p_Spanne)
        dt_ende = datetime.today()

        if Offset_Unit == "D":
            dt_anfang = dt_ende - timedelta(days=spanne_val)
        else:
            # Monate abziehen
            source_date = dt_ende
            month = source_date.month - 1 - spanne_val
            year = source_date.year + month // 12
            month = month % 12 + 1
            day = min(source_date.day, [
                31,
                29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                31, 30, 31, 30, 31, 31, 30, 31, 30, 31
            ][month - 1])
            dt_anfang = datetime(year, month, day)

        os.environ[p_VarAnfang] = dt_anfang.strftime("%Y%m%d")
        os.environ[p_VarEnde] = dt_ende.strftime("%Y%m%d")

    except Exception as e:
        ErrNr = 85
        ErrArg = "DWDate_Gib_Zeitraum"

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Python utility module to replace h_alis_parameter.ksh")
    parser.add_argument("--action", choices=[
        "pruefeParameterGesetzt", "konvertiereKennzahl", "konvertiereSystem",
        "konvertiereSDName", "konvertiereAufbStufeXtra", "pruefeSystemKennzahl",
        "gibBereich", "gibIntervall", "pruefeZeitraum", "pruefeZahlPositiv",
        "pruefeZeitParameter", "konvertiereZeitspanne"
    ], required=True, help="Module routine to execute.")
    parser.add_argument("--arg1", help="Argument 1")
    parser.add_argument("--arg2", help="Argument 2")
    parser.add_argument("--arg3", help="Argument 3")
    parser.add_argument("--arg4", help="Argument 4")

    args = parser.parse_args()

    global ErrNr, ErrArg

    if args.action == "pruefeParameterGesetzt":
        pruefeParameterGesetzt(args.arg1, args.arg2)
    elif args.action == "konvertiereKennzahl":
        konvertiereKennzahl(args.arg1)
    elif args.action == "konvertiereSystem":
        konvertiereSystem(args.arg1)
    elif args.action == "konvertiereSDName":
        konvertiereSDName(args.arg1)
    elif args.action == "konvertiereAufbStufeXtra":
        konvertiereAufbStufeXtra(args.arg1)
    elif args.action == "pruefeSystemKennzahl":
        pruefeSystemKennzahl(args.arg1, args.arg2)
    elif args.action == "gibBereich":
        gibBereich(args.arg1, args.arg2)
    elif args.action == "gibIntervall":
        gibIntervall(args.arg1, args.arg2)
    elif args.action == "pruefeZeitraum":
        pruefeZeitraum(args.arg1, args.arg2)
    elif args.action == "pruefeZahlPositiv":
        pruefeZahlPositiv(args.arg1, args.arg2)
    elif args.action == "pruefeZeitParameter":
        pruefeZeitParameter(args.arg1, args.arg2, args.arg3)
    elif args.action == "konvertiereZeitspanne":
        konvertiereZeitspanne(args.arg1, args.arg2, args.arg3, args.arg4)

    if ErrNr != 0:
        print(f"ERROR: ErrNr={ErrNr}, ErrArg={ErrArg}", file=sys.stderr)
        return ErrNr
    else:
        return 0

if __name__ == "__main__":
    sys.exit(main())