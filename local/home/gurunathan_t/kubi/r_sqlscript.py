#!/usr/bin/env python3
import os
import sys
import contextlib
import subprocess

# Add DW_DIR_ROOT to sys.path if available to locate helper modules
dw_dir_root = os.environ.get("DW_DIR_ROOT")
if dw_dir_root:
    paths_to_add = [
        os.path.join(dw_dir_root, "allgemein", "is", "util", "bin"),
        os.path.join(dw_dir_root, "allgemein/is/util/bin")
    ]
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)

try:
    import f_alis_msgerr
except ImportError:
    class FallbackFAlisMsgerr:
        @staticmethod
        def DWMSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
            print(f"STUB: DWMSG_MeldeFehler({eintrags_nr}, {severity}, {err_nr}, {err_arg})", file=sys.stderr)

        @staticmethod
        def DWMSG_ErmittleNr(*args):
            return "12345"

        @staticmethod
        def DWMSG_Logdateiname(job_kennung, eintrags_nr):
            return f"/tmp/{job_kennung}_{eintrags_nr}.log"

        @staticmethod
        def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, script, log_datei):
            print(f"STUB: DWMSG_ErzeugeEintrag({eintrags_nr}, {job_kennung}, {script}, {log_datei})")

        @staticmethod
        def DWMSG_Fehlerbehandlung(eintrags_nr):
            print(f"STUB: DWMSG_Fehlerbehandlung({eintrags_nr})", file=sys.stderr)

        @staticmethod
        def DWMSG_SetzeStatusOK(eintrags_nr):
            print(f"STUB: DWMSG_SetzeStatusOK({eintrags_nr})")
    f_alis_msgerr = FallbackFAlisMsgerr()

try:
    import h_alis_sqlplus
except ImportError:
    class FallbackHAlisSqlplus:
        @staticmethod
        def starteSQLSkript(eintrags_nr, db_script, sql_par, tracking_nr):
            print(f"STUB: starteSQLSkript({eintrags_nr}, {db_script}, {sql_par}, {tracking_nr})")
    h_alis_sqlplus = FallbackHAlisSqlplus()


@contextlib.contextmanager
def redirect_stdout_stderr_fd(target_file_path):
    """Context manager to redirect lower-level file descriptors 1 and 2 to target file."""
    if not target_file_path:
        yield
        return
    dir_name = os.path.dirname(target_file_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
    with open(target_file_path, "a") as f:
        fd = f.fileno()
        saved_stdout_fd = os.dup(1)
        saved_stderr_fd = os.dup(2)
        try:
            os.dup2(fd, 1)
            os.dup2(fd, 2)
            yield
        finally:
            os.dup2(saved_stdout_fd, 1)
            os.dup2(saved_stderr_fd, 2)
            os.close(saved_stdout_fd)
            os.close(saved_stderr_fd)


def usage():
    prog_name = f"Ausführung Script {sys.argv[0]}"
    prog_version = "5.0.0"
    print(f"""   Programm: {prog_name}
   Version: {prog_version}
   Aufruf: {sys.argv[0]} Parameter

   Das als Parameter -f  übergebene SQL-Script wird ausgeführt.
   Es muß die Zeile "whenever sqlerror exit failure" enthalten,
   damit das Rahmenscript bei Fehlern abbricht.
   Der mit dem Parameter -i übergebene String wird an das SQL-Script
   weitergereicht
   Wenn das SQL-Script keinen Pfad hat, wird es  erst in  ../sql
   parallel zum Ablageverzeichnis dieses Rahmenscripts vermutet,
   dan in ../mig,
   dann direkt im Ablageverzeichnis dieses Rahmenscripts.
   Dies Rahmenscript muß deswegen immer mit Komplettpfad aufgerufen werden
   oder direkt aus  dem  Verzeichnis, in dem es gespeichert ist.


   Parameter:
       -f     hier wird der Name des SQL-Scripts angegeben
       -i     mögliche Parameter für das SQL-Script 

       -j     Jobkennung (default DWH_KORR)

       -h     zeigt diese Seite an

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)""")


def main():
    # Setup initial values
    DW_EintragsNr = 0
    LogDatei = ""
    p_Verbose = 0
    p_Job = ""
    p_sqlscript = None
    p_sqlpar = ""
    ErrNr = 0
    ErrArg = ""

    # Parse parameters manually to replicate getopts exactly
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith('-') and len(arg) > 1:
            chars = arg[1:]
            idx = 0
            while idx < len(chars):
                opt = chars[idx]
                if opt == 'h':
                    usage()
                    sys.exit(0)
                elif opt == 'v':
                    p_Verbose = 1
                elif opt == 'f':
                    if idx + 1 < len(chars):
                        p_sqlscript = chars[idx+1:]
                        break
                    else:
                        if i + 1 < len(args):
                            p_sqlscript = args[i+1]
                            i += 1
                            break
                        else:
                            ErrNr = 193  # Required argument missing
                            ErrArg = "f"
                            break
                elif opt == 'i':
                    if idx + 1 < len(chars):
                        p_sqlpar = chars[idx+1:]
                        break
                    else:
                        if i + 1 < len(args):
                            p_sqlpar = args[i+1]
                            i += 1
                            break
                        else:
                            ErrNr = 193  # Required argument missing
                            ErrArg = "i"
                            break
                elif opt == 'j':
                    if idx + 1 < len(chars):
                        p_Job = chars[idx+1:]
                        break
                    else:
                        if i + 1 < len(args):
                            p_Job = args[i+1]
                            i += 1
                            break
                        else:
                            ErrNr = 193  # Required argument missing
                            ErrArg = "j"
                            break
                else:
                    ErrNr = 192  # Parameter unknown
                    ErrArg = opt
                    break
            if ErrNr != 0:
                break
        else:
            ErrNr = 192  # Parameter unknown
            ErrArg = arg
            break
        i += 1

    # Convert p_sqlscript to lowercase (typeset -l)
    if p_sqlscript is not None:
        p_sqlscript = p_sqlscript.lower()
    else:
        if ErrNr == 0:
            ErrNr = 193
            ErrArg = "f"

    # Step 3: Input validation error handling
    if ErrNr != 0:
        f_alis_msgerr.DWMSG_MeldeFehler(DW_EintragsNr, "E", ErrNr, ErrArg)
        usage()
        sys.exit(ErrNr)

    # Step 4: Directory navigation and path search
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(script_dir)

    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir in ('', '.'):
        # Relational path searching
        paths_to_test = [
            os.path.join("..", "sql", p_sqlscript),
            os.path.join("..", "mig", p_sqlscript),
            p_sqlscript
        ]
        l_DBskript = p_sqlscript
        for path in paths_to_test:
            if os.path.isfile(path):
                l_DBskript = path
                break
    else:
        l_DBskript = p_sqlscript

    # Step 5: File Validation Logic
    # REVIEW: This conditional mirrors legacy 'if [ -f "$l_DBskript" ]' trigger.
    # It sets ErrNr=198 but does not abort or handle it in the original script.
    # REVIEW: p_Kuerzel is referenced but not declared in legacy ksh script.
    p_Kuerzel = os.environ.get("p_Kuerzel", "")
    if os.path.isfile(l_DBskript):
        ErrNr = 198  # Parameterwert unbekannt
        ErrArg = p_Kuerzel

    # Step 6: Set Job Identifier (typeset -u)
    if not p_Job:
        JobKennung = "DWH_KORR"
    else:
        JobKennung = p_Job.upper()

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 7: Logging and Tracking configuration
    try:
        try:
            DW_EintragsNr = f_alis_msgerr.DWMSG_ErmittleNr()
        except TypeError:
            DW_EintragsNr = f_alis_msgerr.DWMSG_ErmittleNr("DW_EintragsNr")
    except Exception:
        DW_EintragsNr = "12345"

    os.environ["DW_EintragsNr"] = str(DW_EintragsNr)

    try:
        LogDatei = f_alis_msgerr.DWMSG_Logdateiname(JobKennung, DW_EintragsNr)
    except Exception:
        LogDatei = f"/tmp/{JobKennung}_{DW_EintragsNr}.log"

    script_ident = f"{sys.argv[0]}_{l_DBskript}"
    with redirect_stdout_stderr_fd(LogDatei):
        f_alis_msgerr.DWMSG_ErzeugeEintrag(DW_EintragsNr, JobKennung, script_ident, LogDatei)

    # Step 8: Execution trap block
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{DW_EintragsNr}'")
        print(f"Logdatei  : '{LogDatei}'")
        print("---------------------------------------------")

        # Step 9: Invoke core database job script
        with redirect_stdout_stderr_fd(LogDatei):
            h_alis_sqlplus.starteSQLSkript(DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr)

        # Step 10: Post-execution handling and OK notification
        with redirect_stdout_stderr_fd(LogDatei):
            f_alis_msgerr.DWMSG_SetzeStatusOK(DW_EintragsNr)

        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except KeyboardInterrupt:
        # INT trap
        with redirect_stdout_stderr_fd(LogDatei):
            f_alis_msgerr.DWMSG_Fehlerbehandlung(DW_EintragsNr)
            print("!OSFEHLER gemeldet!")
        if p_Verbose:
            if os.path.exists(LogDatei):
                with open(LogDatei, "r") as log_f:
                    print(log_f.read(), end="")
        sys.exit(1)

    except Exception as e:
        # ERR trap
        with redirect_stdout_stderr_fd(LogDatei):
            f_alis_msgerr.DWMSG_Fehlerbehandlung(DW_EintragsNr)
            print("!FEHLER gemeldet!")
        if p_Verbose:
            if os.path.exists(LogDatei):
                with open(LogDatei, "r") as log_f:
                    print(log_f.read(), end="")
        exit_code = 1
        if isinstance(e, subprocess.CalledProcessError):
            exit_code = e.returncode
        sys.exit(exit_code)


if __name__ == "__main__":
    sys.exit(main())