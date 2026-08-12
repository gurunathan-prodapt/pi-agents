#!/usr/bin/env python3

import os
import sys
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Modul-Informationen
ModulName = "alis_parameter"
ModulVersion = "V3.0.9"


def get_err_nr():
    """Gets the global error number from the environment, defaulting to 0."""
    return int(os.environ.get("ErrNr", "0"))


def set_err_nr(val):
    """Sets the global error number in the environment."""
    os.environ["ErrNr"] = str(val)


def get_err_arg():
    """Gets the global error argument from the environment, defaulting to empty string."""
    return os.environ.get("ErrArg", "")


def set_err_arg(val):
    """Sets the global error argument in the environment."""
    os.environ["ErrArg"] = str(val)


def pruefeParameterGesetzt(param_name, param_var):
    """
    Prueft, ob die uebergebene Environment-Variable einen Wert beinhaltet.
    Falls dies nicht der Fall ist, wird ein standardisierter Fehlerzustand generiert.
    """
    if get_err_nr() != 0:
        return

    if not param_name or not param_var:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} pruefeParameterGesetzt")
        return

    param_wert = os.environ.get(param_var)

    if not param_wert or param_wert.strip() == "":
        set_err_nr(194)
        set_err_arg(param_name)


def konvertiereKennzahl(var_name):
    """
    Konvertiert die Kennzahlbezeichnung basierend auf dem Namenskonzept 
    in eine gueltige Abkuerzung fuer Kennzahlen.
    """
    if get_err_nr() != 0:
        return

    if not var_name:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} konvertiereKennzahl")
        return

    kennzahl = os.environ.get(var_name, "").lower().strip()

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
        set_err_nr(198)
        set_err_arg(kennzahl)
        kennzahl = "???"

    os.environ[var_name] = kennzahl


def konvertiereSystem(var_name):
    """
    Konvertiert die Systembezeichnung basierend auf dem Namenskonzept
    in eine gueltige Abkuerzung fuer Liefersysteme.
    """
    if get_err_nr() != 0:
        return

    if not var_name:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} konvertiereSystem")
        return

    system = os.environ.get(var_name, "").lower().strip()

    valid_systems = {
        "sap", "carmen", "dpps", "d1", "xtra", "ctel", "nnv", "dwh", "brunet", "sigma"
    }

    if system in valid_systems:
        pass
    else:
        set_err_nr(195)
        set_err_arg(f"Unbekannte Datenherkunft {system} !")
        system = "???"

    os.environ[var_name] = system


def konvertiereSDName(var_name):
    """
    Konvertiert die Systembezeichnung basierend auf dem Namenskonzept
    in eine gueltige Abkuerzung fuer Stammdaten-Liefersysteme.
    """
    if get_err_nr() != 0:
        return

    if not var_name:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} konvertiereSDSystem")
        return

    system = os.environ.get(var_name, "").lower().strip()

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
        set_err_nr(195)
        set_err_arg(f"Unbekannte Stammdaten-Datenherkunft {system} !")
        system = "???"

    os.environ[var_name] = system


def konvertiereAufbStufeXtra(var_name):
    """
    Konvertiert den Aufbereitungsstufenname in eine normierte Abkuerzung.
    """
    if get_err_nr() != 0:
        return

    if not var_name:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} konvertiereAufbStufeXtra")
        return

    stufe = os.environ.get(var_name, "").lower().strip()

    if stufe == "zusammenfuehrung":
        stufe = "mrg"
    elif stufe == "befuellung":
        stufe = "fill"
    else:
        set_err_nr(195)
        set_err_arg(f"Unbekannte Stufenangabe {stufe} !")
        stufe = "???"

    os.environ[var_name] = stufe


def pruefeSystemKennzahl(system, kennzahl):
    """
    Prueft, ob die Kombination von System und Kennzahl erlaubt ist, d.h. vom IS unterstuetzt wird.
    """
    if get_err_nr() != 0:
        return

    if not system or not kennzahl:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} pruefeSystemKennzahl")
        return

    err_arg_local = ""

    if system != "nnv" and (kennzahl == "tvd" or kennzahl == "lkl"):
        err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "carmen":
        if kennzahl in [
            "twe", "pln", "rst", "srs", "sgs", "ust", "mahn", "sg_rv", "sr_rv_dpps", "bwa"
        ]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sap":
        if kennzahl in [
            "zug", "abg", "abz", "bst", "twe", "pln", "gut", "auf", "rst", "tvd", "usk",
            "ust", "lkl", "loe", "rak", "ksd", "bwa"
        ]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dpps":
        if kennzahl in [
            "twe", "pln", "loe", "rak", "srs", "sgs", "mahn", "sg_rv", "sr_rv_dpps"
        ]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "ctel":
        if kennzahl not in ["abg", "bst", "zug", "twe"]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "xtra":
        if kennzahl != "rst":
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "d1":
        if kennzahl in [
            "gut", "auf", "loe", "rak", "sgs", "srs", "twe", "ksd", "mahn", "sg_rv",
            "sr_rv_dpps", "bwa"
        ]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "nnv":
        if kennzahl not in ["tvd", "lkl"]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "dwh":
        if kennzahl != "mds":
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "brunet":
        if kennzahl not in ["d1n", "rub", "lmo"]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"
    elif system == "sigma":
        if kennzahl not in [
            "nnk", "tvk", "glv", "gz", "zonek", "zonet", "nnkt", "trfa", "gtyp", "basisd",
            "natint", "glint"
        ]:
            err_arg_local = f"Ungueltige Kombination {system} {kennzahl}"

    if err_arg_local:
        set_err_nr(195)
        set_err_arg(err_arg_local)


def gibBereich(kennzahl, var_bereich):
    """
    Gibt in Abhaengigkeit zu einer Kennzahl/Eingangsgroesse den entsprechenden Bereich aus.
    """
    if get_err_nr() != 0:
        return

    if not kennzahl or not var_bereich:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} gibBereich")
        return

    list_tn = "abg abz bst pln twe zug loe rak".split()
    list_us = "gut rst auf ust usk srs sgs mahn sg_rv sr_rv_dpps".split()
    list_gd = (
        "tvd lkl d1n rub lmo nnk tvk gz glv zonek zonet nnkt trfa gtyp basisd "
        "natint glint"
    ).split()
    list_sd = "ksd bwa".split()
    list_md = ["mds"]

    list_bereich = "tn us gd sd md".split()
    my_bereich = ""

    for bk in list_bereich:
        if bk == "tn":
            current_list = list_tn
        elif bk == "us":
            current_list = list_us
        elif bk == "gd":
            current_list = list_gd
        elif bk == "sd":
            current_list = list_sd
        elif bk == "md":
            current_list = list_md
        else:
            current_list = []

        if not my_bereich:
            for groesse in current_list:
                if groesse == kennzahl:
                    my_bereich = bk
                    break

    if not my_bereich:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} gibBereich - Kuerzel '{kennzahl}' unbekannt")
        return

    os.environ[var_bereich] = my_bereich


def gibIntervall(kennzahl, var_intervall):
    """
    Gibt in Abhaengigkeit zu einer Kennzahl/Eingangsgroesse das entsprechende Intervall (t,m) aus.
    """
    if get_err_nr() != 0:
        return

    if not kennzahl or not var_intervall:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} gibIntervall")
        return

    list_t = (
        "abg abz twe zug gut auf rst ust usk rak loe srs sgs ksd mahn mds tvk "
        "sr_rv_dpps gtyp basisd bwa"
    ).split()
    list_m = (
        "bst pln tvd lkl sg_rv d1n rub lmo nnk gz glv zonek zonet nnkt trfa "
        "natint glint"
    ).split()

    list_intervall = ["t", "m"]
    my_intervall = ""

    for ik in list_intervall:
        if ik == "t":
            current_list = list_t
        elif ik == "m":
            current_list = list_m
        else:
            current_list = []

        if not my_intervall:
            for groesse in current_list:
                if groesse == kennzahl:
                    my_intervall = ik
                    break

    if not my_intervall:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} gibIntervall - Kuerzel '{kennzahl}' unbekannt")
        return

    os.environ[var_intervall] = my_intervall


# REVIEW-STRUCT: external utilities [DWDate_Datum_Check] and [DWDate_Datum_LE] simulated natively
def pruefeZeitraum(anfang, ende):
    """
    Prueft, ob die beiden Parameter einen gueltigen Zeitraum beschreiben.
    """
    if get_err_nr() != 0:
        return

    if not anfang or not ende:
        set_err_nr(196)
        set_err_arg(f"{ModulName} {ModulVersion} pruefeZeitraum")
        return

    err_arg_local = ""

    anfang_valid = True
    try:
        dt_anfang = datetime.strptime(str(anfang), "%Y%m%d")
    except ValueError:
        err_arg_local = "Anfangsdatum entspricht nicht dem Format YYYYMMDD"
        anfang_valid = False

    ende_valid = True
    try:
        dt_ende = datetime.strptime(str(ende), "%Y%m%d")
    except ValueError:
        if not err_arg_local:
            err_arg_local = "Endedatum entspricht nicht dem Format YYYYMMDD"
        ende_valid = False

    if anfang_valid and ende_valid:
        if dt_anfang > dt_ende:
            err_arg_local = "Anfangsdatum ist nicht kleiner gleich Endedatum"

    if err_arg_local:
        set_err_nr(195)
        set_err_arg(err_arg_local)


def pruefeZahlPositiv(p_zahl, p_parameter_name):
    """
    Prueft, ob der uebergebene Parameter numerisch und >= 0 ist.
    """
    try:
        val = int(p_zahl)
        if val < 0:
            set_err_nr(195)
            set_err_arg(f"Parameter {p_parameter_name} muss groesser gleich 0 sein")
    except ValueError:
        set_err_nr(195)
        set_err_arg(f"Parameter {p_parameter_name} ist kein numerischer Wert")


def pruefeZeitParameter(p_anfangsdatum, p_endedatum, p_zeit_offset):
    """
    Prueft, ob genau Anfangs- und Endedatum oder Zeitraum gesetzt und gueltig sind.
    """
    if get_err_nr() != 0:
        return

    if p_zeit_offset and p_zeit_offset.strip() != "":
        if (not p_anfangsdatum or p_anfangsdatum.strip() == "") and (not p_endedatum or p_endedatum.strip() == ""):
            pruefeZahlPositiv(p_zeit_offset, "Zeitspanne")
            return
        else:
            set_err_nr(195)
            set_err_arg("Es darf nur eine Zeitspanne oder beide Datumwerte gesetzt werden")
            return
    else:
        if p_anfangsdatum and p_endedatum:
            pruefeZeitraum(p_anfangsdatum, p_endedatum)
        else:
            set_err_nr(195)
            if not p_anfangsdatum and not p_endedatum:
                set_err_arg("Datumswerte oder Zeitspanne fehlen")
            else:
                set_err_arg("Sowohl Anfang- als auch Endedatum muessen angegeben werden")
            return


# REVIEW-STRUCT: external utility [DWDate_Gib_Zeitraum] simulated natively
def konvertiereZeitspanne(p_var_anfang, p_var_ende, p_spanne, p_kennzahl):
    """
    Berechnet aus der Zeitspanne und der Kennzahl Anfangs- und Endedatum.
    """
    if get_err_nr() != 0:
        return

    offset_unit = "D"
    if p_kennzahl == "bst":
        offset_unit = "M"

    try:
        span_val = int(p_spanne)
        today = datetime.now()

        if offset_unit == "D":
            dt_anfang = today - timedelta(days=span_val)
            dt_ende = today
        elif offset_unit == "M":
            dt_anfang = today - relativedelta(months=span_val)
            dt_ende = today
        else:
            dt_anfang = today
            dt_ende = today

        os.environ[p_var_anfang] = dt_anfang.strftime("%Y%m%d")
        os.environ[p_var_ende] = dt_ende.strftime("%Y%m%d")

    except Exception as e:
        set_err_nr(85)
        set_err_arg(f"DWDate_Gib_Zeitraum failed: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Python port of h_alis_parameter.ksh")
    subparsers = parser.add_subparsers(dest="command", help="Function to execute")

    # Command: pruefeParameterGesetzt
    p_set = subparsers.add_parser("pruefeParameterGesetzt")
    p_set.add_argument("param_name")
    p_set.add_argument("param_var")

    # Command: konvertiereKennzahl
    p_kenn = subparsers.add_parser("konvertiereKennzahl")
    p_kenn.add_argument("var_name")

    # Command: konvertiereSystem
    p_sys = subparsers.add_parser("konvertiereSystem")
    p_sys.add_argument("var_name")

    # Command: konvertiereSDName
    p_sd = subparsers.add_parser("konvertiereSDName")
    p_sd.add_argument("var_name")

    # Command: konvertiereAufbStufeXtra
    p_xtra = subparsers.add_parser("konvertiereAufbStufeXtra")
    p_xtra.add_argument("var_name")

    # Command: pruefeSystemKennzahl
    p_sk = subparsers.add_parser("pruefeSystemKennzahl")
    p_sk.add_argument("system")
    p_sk.add_argument("kennzahl")

    # Command: gibBereich
    p_gb = subparsers.add_parser("gibBereich")
    p_gb.add_argument("kennzahl")
    p_gb.add_argument("var_bereich")

    # Command: gibIntervall
    p_gi = subparsers.add_parser("gibIntervall")
    p_gi.add_argument("kennzahl")
    p_gi.add_argument("var_intervall")

    # Command: pruefeZeitraum
    p_pz = subparsers.add_parser("pruefeZeitraum")
    p_pz.add_argument("anfang")
    p_pz.add_argument("ende")

    # Command: pruefeZahlPositiv
    p_zp = subparsers.add_parser("pruefeZahlPositiv")
    p_zp.add_argument("p_zahl")
    p_zp.add_argument("p_parameter_name")

    # Command: pruefeZeitParameter
    p_pzp = subparsers.add_parser("pruefeZeitParameter")
    p_pzp.add_argument("--anfang", default="")
    p_pzp.add_argument("--ende", default="")
    p_pzp.add_argument("--offset", default="")

    # Command: konvertiereZeitspanne
    p_kz = subparsers.add_parser("konvertiereZeitspanne")
    p_kz.add_argument("p_var_anfang")
    p_kz.add_argument("p_var_ende")
    p_kz.add_argument("p_spanne")
    p_kz.add_argument("p_kennzahl")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    # Ensure environment tracking variables are initialized
    if "ErrNr" not in os.environ:
        os.environ["ErrNr"] = "0"
    if "ErrArg" not in os.environ:
        os.environ["ErrArg"] = ""

    if args.command == "pruefeParameterGesetzt":
        pruefeParameterGesetzt(args.param_name, args.param_var)
    elif args.command == "konvertiereKennzahl":
        konvertiereKennzahl(args.var_name)
    elif args.command == "konvertiereSystem":
        konvertiereSystem(args.var_name)
    elif args.command == "konvertiereSDName":
        konvertiereSDName(args.var_name)
    elif args.command == "konvertiereAufbStufeXtra":
        konvertiereAufbStufeXtra(args.var_name)
    elif args.command == "pruefeSystemKennzahl":
        pruefeSystemKennzahl(args.system, args.kennzahl)
    elif args.command == "gibBereich":
        gibBereich(args.kennzahl, args.var_bereich)
    elif args.command == "gibIntervall":
        gibIntervall(args.kennzahl, args.var_intervall)
    elif args.command == "pruefeZeitraum":
        pruefeZeitraum(args.anfang, args.ende)
    elif args.command == "pruefeZahlPositiv":
        pruefeZahlPositiv(args.p_zahl, args.p_parameter_name)
    elif args.command == "pruefeZeitParameter":
        pruefeZeitParameter(args.anfang, args.ende, args.offset)
    elif args.command == "konvertiereZeitspanne":
        konvertiereZeitspanne(args.p_var_anfang, args.p_var_ende, args.p_spanne, args.p_kennzahl)

    # Print mutations
    print(f"ErrNr: {os.environ.get('ErrNr', '0')}")
    print(f"ErrArg: {os.environ.get('ErrArg', '')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())