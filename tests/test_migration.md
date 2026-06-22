As a senior data-migration QA engineer, I've analyzed the provided migration design for `r_aurd_rechstan.ksh` to `r_aurd_rechstan.py` and its Airflow orchestration. The core challenge is that the actual data processing logic within `k_aurd_rechstan.ksh` is currently unknown and unmigrated. Therefore, the tests below focus primarily on the wrapper's behavior: parameter parsing, defaulting, date handling, error handling, logging, and the correct invocation of the (currently mocked) core processing step.

The tests are designed to prove behavioral equivalence where possible, and highlight intentional or unavoidable differences.

---

## Migration Validation Tests for `r_aurd_rechstan.ksh`

### Overall Test Strategy & Limitations

The `r_aurd_rechstan.ksh` script acts as an orchestrator. Its primary responsibilities are:
*   Parsing command-line arguments (`-s Stichtag`, `-l Wiederanlaufwert`).
*   Applying default values for `Stichtag` (system date) and `Wiederanlaufwert` (0).
*   Performing basic parameter validation.
*   Initializing logging and error handling.
*   Invoking the core processing script (`k_aurd_rechstan.ksh`) with derived parameters.

**Key Limitations:**
1.  **Core Logic Unknown:** The content of `k_aurd_rechstan.ksh` is explicitly stated as "unknown" and "blocked." This means tests for data transformation correctness (joins, aggregations, filters, type handling, NULL handling, edge cases related to data processing) cannot be performed at this stage. The `run_core_job` function in `r_aurd_rechstan.py` is a placeholder, and `k_aurd_rechstan.ksh` is mocked to capture its invocation parameters.
2.  **External System Replacements:** The `r_aurd_rechstan.ksh` script itself does not directly interact with external systems like Oracle, SFTP, or S3. These interactions would occur within `k_aurd_rechstan.ksh`. Therefore, tests for external system replacements are deferred until `k_aurd_rechstan.ksh` is analyzed and migrated.
3.  **Data Quality/Row Count/Schema Assertions:** These assertions apply to the output of the core data processing, which is currently unknown. They will be part of the testing for the migrated `k_aurd_rechstan.ksh` logic (BigQuery ETL).

**Test Focus:**
The tests will concentrate on:
*   **Output Parity:** Comparing log messages, exit codes, and the parameters passed to the core processing step.
*   **Transformation Correctness (Wrapper Logic):** Correct parsing of command-line arguments, application of default values, date calculation, and validation logic.
*   **Error Handling:** Verifying that the script exits with appropriate codes and messages under various error conditions.

### Test Environment Setup (for runnable code)

To execute the legacy KornShell script and compare its behavior with the migrated Python script, a controlled test environment is necessary. This involves creating mock versions of the legacy utility scripts that `r_aurd_rechstan.ksh` sources, and a mock `k_aurd_rechstan.ksh` to capture its invocation.

The following `pytest` fixtures and helper functions facilitate this setup:

```python
import subprocess
import os
import sys
import datetime
import pytest
from unittest.mock import patch, MagicMock

# Assume r_aurd_rechstan.py and utils.py are in the same directory as the test file
# or properly installed/imported.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from r_aurd_rechstan import main as r_aurd_rechstan_main
from r_aurd_rechstan import run_core_job as r_aurd_rechstan_run_core_job
from r_aurd_rechstan import usage as r_aurd_rechstan_usage
from r_aurd_rechstan import validate_date as r_aurd_rechstan_validate_date
from utils import DWError, DWMSG_MeldeFehler, get_date_formatted, pruefeParameterGesetzt

# --- Helper for Legacy Script Execution ---
@pytest.fixture(scope="module")
def legacy_env_setup(tmp_path_factory):
    """
    Sets up a temporary directory with mock files for the legacy KSH script.
    Returns the path to the legacy script and the mock k_aurd_rechstan.ksh output file.
    """
    base_dir = tmp_path_factory.mktemp("legacy_test_env")
    home_dir = base_dir / "home"
    home_dir.mkdir()
    bert_root = base_dir / "bert_root"
    bert_root.mkdir()

    # Create .dw_init
    (home_dir / ".dw_init").write_text(f"export BERT_DIR_ROOT={bert_root}\n")

    # Create mock utility scripts
    util_bin = bert_root / "allgemein" / "is" / "util" / "bin"
    util_bin.mkdir(parents=True, exist_ok=True)

    # Mock f_alis_msgerr.ksh to include all DWMSG_* mocks for capturing calls
    (util_bin / "f_alis_msgerr.ksh").write_text(f"""
#!/bin/ksh
# Mock f_alis_msgerr.ksh and other DWMSG_* functions
DWMSG_MeldeFehler() {{
    echo "MOCK_DWMSG_MeldeFehler: $@" >&2
}}
DWMSG_ErmittleNr() {{
    # Return a fixed timestamp for consistent testing
    echo "MOCK_DWMSG_ErmittleNr: 20230101103000"
}}
DWMSG_Logdateiname() {{
    echo "MOCK_DWMSG_Logdateiname: $1_$2.log"
}}
DWMSG_ErzeugeEintrag() {{
    echo "MOCK_DWMSG_ErzeugeEintrag: $@"
}}
DWMSG_SetzeStichtagInfo() {{
    echo "MOCK_DWMSG_SetzeStichtagInfo: $@"
}}
DWMSG_Fehlerbehandlung() {{
    echo "MOCK_DWMSG_Fehlerbehandlung: $@" >&2
}}
DWMSG_SetzeStatusOK() {{
    echo "MOCK_DWMSG_SetzeStatusOK: $@"
}}
""")
    (util_bin / "f_alis_msgerr.ksh").chmod(0o755)

    (util_bin / "h_alis_parameter.ksh").write_text("""#!/bin/ksh""") # Empty, getopts is built-in

    (util_bin / "h_alis_date.ksh").write_text("""
#!/bin/ksh
# Mock h_alis_date.ksh
DWDate_Gib_Zeitraum() {
    # Simulate returning a fixed date for consistent testing
    # Arguments: $1=offset, $2=unit, $3=format, $4=var_name_for_date, $5=var_name_for_dummy
    if [ "$3" = "DDMMYYYY" ]; then
        eval "$4='01012023'" # Fixed date for consistent testing
    else
        eval "$4='20230101'" # Default to YYYYMMDD if format is different
    fi
    eval "$5='dummy_value'"
}

pruefeParameterGesetzt() {
    # Arguments: $1=param_name, $2=param_value_var_name
    local param_name="$1"
    local param_value=$(eval echo "\$$2")
    if [ -z "$param_value" ]; then
        echo "MOCK_pruefeParameterGesetzt: Parameter '$param_name' is not set." >&2
        ErrNr=193 # Simulate error
        ErrArg="$param_name"
    fi
}
""")
    (util_bin / "h_alis_date.ksh").chmod(0o755)

    # Create mock k_aurd_rechstan.ksh to capture arguments
    aufbereitung_bin = bert_root / "aufbereitung" / "bin"
    aufbereitung_bin.mkdir(parents=True, exist_ok=True)
    mock_k_aurd_rechstan_output_file = base_dir / "mock_k_aurd_rechstan_output.log"
    (aufbereitung_bin / "k_aurd_rechstan.ksh").write_text(f"""
#!/bin/ksh
echo "MOCK_k_aurd_rechstan.ksh invoked with args: $@" >> {mock_k_aurd_rechstan_output_file}
# Simulate success by default, or failure if a specific arg is passed
if [[ "$*" == *"--fail"* ]]; then
    exit 1
fi
exit 0
""")
    (aufbereitung_bin / "k_aurd_rechstan.ksh").chmod(0o755)

    # Copy the actual legacy script into the test environment
    legacy_script_path = base_dir / "r_aurd_rechstan.ksh"
    # Assuming the legacy script content is provided in the prompt
    legacy_script_content = """
#!/bin/ksh
# Zweck:
#
# Erzeugt am: 20.02.2002
# Versions-Anmerkungen: Loebbers (initial)
#         
ProgName="Erzeugung eines Abzugs der Rechnungsdaten"
ProgVersion="V2.0.5"

#####################################
# Funktion:
#    usage - Ausgabe der Programmbeschreibung
usage(){
cat <<EOF
    Programm: $ProgName
    Version:  $ProgVersion
    Aufruf:   $0 Parameter
    Parameter:
	-h     zeigt diese Seite an
	-s     Stichtag DDMMYYYY
	-l     Wiederanlaufwert 
               wird dieser Wert gesetzt, so werden nur Vertraege zu 
               DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle
               geschrieben (die Eintraege bzgl. Werten >= diesem
               Wert werden geloescht)

    Beschreibung:
        Dieser Job erzeugt einen Stichtags-Abzug der Vertrags-Cache
	im DWH und stellt sie Forderungsscoring zur Verfuegung.
	Zu beachten ist hierbei, dass eine bereits bereitgestellte
	Tabelle dann geloescht wird, wenn keine aktive Vertragscache
	existiert, die noch nicht abgeholt worden ist.
	Eine solche Abholung muss vom FOS-Loader entsprechend markiert
	worden sein.
	Es werden jeweils Records selektiert, fuer die 
               Gueltig_von <= Stichtag < Gueltig_bis AND
	       LADEDATUM   < Stichtag
	gilt.
	Falls der Stichtag nicht gesetzt wird, dann wird das 
        MINIMUM aus aktuellem Systemdatum und maximalem Ladedatum
                (Quelltabelle)
        herangezogen.

EOF
}


##########################
# Vorbereitende Massnahmen
#    Einlesen der Umgebung
. $HOME/.dw_init


#    Fehlerkonzept einschalten
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh

set -e

ErrNr=0
ErrArg=""

# Globale Fehlerbehandlung
ErrVal=0

DW_EintragsNr=0        

#    Hilfsskripte zum Parsen der Parameter
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh
#    Hilfsskripte zur Datumbehandlung
#AL?? . ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_fos_date.ksh
. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh

#####################
# Lesen der Parameter
ParamList="s:l:" # Notation gemaess getopts(1)

# lese mit Hilfe getopts die Parameter
while getopts ":h$ParamList" param
do
    case $param in
        h)  
            usage
            exit;;
	s)  
	    p_stichtag=$OPTARG;;
        l)  
	    p_wiederanlaufWert=$OPTARG;;
        :)
            ErrNr=193  # Notwendiges Argument fehlt
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unbekannt
            ErrArg="$OPTARG";;
    esac
done

#################################
# Wiederanlaufwert initialisieren
# falls nicht gesetzt
#################################
if [[ -z "$p_wiederanlaufWert" ]]
then
  p_wiederanlaufWert=0
fi

##############
# hole sysdate
##############
DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY' v_sysdate dummy

###################################
# Datumsbestimmung (falls Stichtag
# nicht gesetzt)
###################################
if [[ -z "$p_stichtag" ]]
then
    ################################
    # hole MIN(sysdate,maxladedatum)
    # fuer die Synchronisation ist
    # dieses Vorgehen notwendig
    ################################
    #AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum
    #AL?? p_stichtag=$v_ladedatum;
    p_stichtag=$v_sysdate;
fi

# Pruefe, ob notwendige Parameter gesetzt worden sind
pruefeParameterGesetzt Stichtag p_stichtag

# Falls Fehler aufgetreten, abbrechen
if [ ! $ErrNr -eq 0 ]
then
    #Ausgabe gemaess Fehlerkonzept
    DWMSG_MeldeFehler $DW_EintragsNr E $ErrNr $ErrArg
    usage
    #Austieg gemaess Nummernkreisen
    exit $ErrNr
fi

Name_Kernskript="${BERT_DIR_ROOT}/aufbereitung/bin/k_aurd_rechstan.ksh"


####################
# Fehlermeldekonzept
####################
typeset -u JobKennung="BERT_RKOPF_STAN"

DW_EintragsNr=$(DWMSG_ErmittleNr) # Capture output of mock
LogDatei=$(DWMSG_Logdateiname $JobKennung $DW_EintragsNr) # Capture output of mock
DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 \
                     $LogDatei >> $LogDatei 2>&1
DWMSG_SetzeStichtagInfo $DW_EintragsNr $v_sysdate 'DDMMYYYY'

# Setze traps#
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'OSError: Abbruch'; exit 1" INT STOP CONT 
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'AppError: Abbruch'" ERR

print " ----------------- Job -----------------------"
print " Job-Nr    : '$DW_EintragsNr'"
print " JobKennung: '$JobKennung'"
print " Logdatei  : '$LogDatei'"
print " Stichtag  : '$p_stichtag'"
print " ---------------------------------------------"

${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert} >> $LogDatei 2>&1 

# hier kommt das Skript nur an, wenn alles OK war
print "Die Abarbeitung wurde ohne erkennbare Fehler beendet" | tee -a $LogDatei
DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1

trap INT STOP CONT ERR

exit 0
"""
    legacy_script_path.write_text(legacy_script_content)
    legacy_script_path.chmod(0o755)

    yield legacy_script_path, mock_k_aurd_rechstan_output_file, base_dir

def run_legacy_script(legacy_script_path, args, legacy_env_setup):
    """
    Runs the legacy KSH script with given arguments in the mocked environment.
    Returns stdout, stderr, exit_code, and k_aurd_rechstan_args.
    """
    script_dir = legacy_script_path.parent
    base_dir = legacy_env_setup[2] # The base_dir from fixture
    mock_k_aurd_rechstan_output_file = legacy_env_setup[1]
    
    # Clear mock k_aurd_rechstan output before each run
    if mock_k_aurd_rechstan_output_file.exists():
        mock_k_aurd_rechstan_output_file.unlink()

    command = [str(legacy_script_path)] + args
    env = os.environ.copy()
    env['HOME'] = str(base_dir / "home") # Point HOME to our mock .dw_init location
    # Ensure BERT_DIR_ROOT is set correctly for sourced scripts
    env['BERT_DIR_ROOT'] = str(base_dir / "bert_root")
    
    process = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(script_dir) # Run from the directory where the script is
    )

    k_aurd_rechstan_args = ""
    if mock_k_aurd_rechstan_output_file.exists():
        k_aurd_rechstan_args = mock_k_aurd_rechstan_output_file.read_text().strip()

    return process.stdout, process.stderr, process.returncode, k_aurd_rechstan_args

# --- Helper for Migrated Script Execution ---
def run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys):
    """
    Runs the migrated Python script with given arguments and mocks.
    Returns stdout, stderr, exit_code, and captured run_core_job args.
    """
    mock_run_core_job.reset_mock() # Clear calls from previous tests
    mock_datetime_now.reset_mock()

    # Mock sys.argv for argparse
    original_argv = sys.argv
    sys.argv = ['r_aurd_rechstan.py'] + args

    captured_run_core_job_args = {}
    mock_run_core_job.side_effect = lambda job_kennung, stichtag, wiederanlaufwert: captured_run_core_job_args.update({
        'job_kennung': job_kennung,
        'stichtag': stichtag,
        'wiederanlaufwert': wiederanlaufwert
    })

    exit_code = 0
    try:
        r_aurd_rechstan_main()
    except SystemExit as e:
        exit_code = e.code
    finally:
        sys.argv = original_argv # Restore sys.argv

    stdout, stderr = capsys.readouterr()
    return stdout, stderr, exit_code, captured_run_core_job_args

# --- Pytest Fixtures for Mocks ---
@pytest.fixture
def mock_run_core_job():
    with patch('r_aurd_rechstan.run_core_job') as mock:
        yield mock

@pytest.fixture
def mock_datetime_now():
    # Mock datetime.datetime.now() to return a fixed date for consistent testing
    fixed_date = datetime.datetime(2023, 1, 1, 10, 30, 0)
    with patch('datetime.datetime') as mock_dt:
        mock_dt.now.return_value = fixed_date
        mock_dt.strptime.side_effect = datetime.datetime.strptime # Ensure strptime still works
        mock_dt.side_effect = lambda *args, **kwargs: datetime.datetime(*args, **kwargs) # Allow datetime.datetime() calls
        yield mock_dt
```

---

### Test Case 1: Basic Execution - No Parameters

*   **Purpose**: Verify that when no command-line parameters are provided, the script correctly defaults `Stichtag` to the system date and `Wiederanlaufwert` to `0`.
*   **Setup**: Execute both scripts without any command-line arguments.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` (legacy).
    *   Run `r_aurd_rechstan.py` (migrated).
*   **Pass/Fail Criterion**:
    *   Both scripts must exit with code `0`.
    *   Both scripts' standard output/logs must indicate `Stichtag` as `01012023` (mocked system date) and `Wiederanlaufwert` as `0`.
    *   The parameters passed to `k_aurd_rechstan.ksh` (captured by mock) and `run_core_job` (captured by mock) must be equivalent.

```python
def test_no_parameters(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Basic execution with no parameters.
    Verifies default Stichtag (system date) and Wiederanlaufwert (0).
    """
    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script([], legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script([], mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 0
    assert migrated_exit_code == 0

    # Output parity: Check Stichtag and Wiederanlaufwert in logs
    assert "Stichtag  : '01012023'" in legacy_stdout
    assert "Stichtag not provided, defaulting to current system date: 01012023" in migrated_stdout
    assert "Stichtag  : '01012023'" in migrated_stdout # Final log output
    assert "Wiederanlaufwert: '0'" in legacy_stdout
    assert "Wiederanlaufwert: '0'" in migrated_stdout

    # Core job invocation parity
    assert "-j BERT_RKOPF_STAN -s 01012023 -f MOCK_DWMSG_ErmittleNr: 20230101103000 -l 0" in legacy_k_args
    assert migrated_k_args == {
        'job_kennung': 'BERT_RKOPF_STAN',
        'stichtag': '01012023',
        'wiederanlaufwert': 0
    }
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in legacy_stdout
    assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in migrated_stdout
```

### Test Case 2: Stichtag Parameter Provided

*   **Purpose**: Verify that the `Stichtag` parameter (`-s`) is correctly parsed and used, overriding the default system date.
*   **Setup**: Execute both scripts with `-s 15032023`.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-s", "15032023"]`.
    *   Run `r_aurd_rechstan.py` with `["-s", "15032023"]`.
*   **Pass/Fail Criterion**:
    *   Both scripts must exit with code `0`.
    *   Both scripts' standard output/logs must indicate `Stichtag` as `15032023`.
    *   The parameters passed to `k_aurd_rechstan.ksh` and `run_core_job` must include `Stichtag=15032023` and `Wiederanlaufwert=0`.

```python
def test_stichtag_parameter_provided(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Stichtag parameter provided.
    Verifies Stichtag is correctly parsed and used.
    """
    test_stichtag = "15032023"
    args = ["-s", test_stichtag]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 0
    assert migrated_exit_code == 0

    assert f"Stichtag  : '{test_stichtag}'" in legacy_stdout
    assert f"Stichtag  : '{test_stichtag}'" in migrated_stdout

    assert f"-j BERT_RKOPF_STAN -s {test_stichtag} -f MOCK_DWMSG_ErmittleNr: 20230101103000 -l 0" in legacy_k_args
    assert migrated_k_args == {
        'job_kennung': 'BERT_RKOPF_STAN',
        'stichtag': test_stichtag,
        'wiederanlaufwert': 0
    }
```

### Test Case 3: Wiederanlaufwert Parameter Provided

*   **Purpose**: Verify that the `Wiederanlaufwert` parameter (`-l`) is correctly parsed and used, overriding the default `0`.
*   **Setup**: Execute both scripts with `-l 12345`.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-l", "12345"]`.
    *   Run `r_aurd_rechstan.py` with `["-l", "12345"]`.
*   **Pass/Fail Criterion**:
    *   Both scripts must exit with code `0`.
    *   Both scripts' standard output/logs must indicate `Wiederanlaufwert` as `12345`.
    *   The parameters passed to `k_aurd_rechstan.ksh` and `run_core_job` must include `Stichtag=01012023` and `Wiederanlaufwert=12345`.

```python
def test_wiederanlaufwert_parameter_provided(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Wiederanlaufwert parameter provided.
    Verifies Wiederanlaufwert is correctly parsed and used.
    """
    test_wiederanlaufwert = 12345
    args = ["-l", str(test_wiederanlaufwert)]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 0
    assert migrated_exit_code == 0

    assert f"Wiederanlaufwert: '{test_wiederanlaufwert}'" in legacy_stdout
    assert f"Wiederanlaufwert: '{test_wiederanlaufwert}'" in migrated_stdout

    assert f"-j BERT_RKOPF_STAN -s 01012023 -f MOCK_DWMSG_ErmittleNr: 20230101103000 -l {test_wiederanlaufwert}" in legacy_k_args
    assert migrated_k_args == {
        'job_kennung': 'BERT_RKOPF_STAN',
        'stichtag': '01012023',
        'wiederanlaufwert': test_wiederanlaufwert
    }
```

### Test Case 4: Both Stichtag and Wiederanlaufwert Provided

*   **Purpose**: Verify that both `Stichtag` and `Wiederanlaufwert` parameters are correctly parsed and used.
*   **Setup**: Execute both scripts with `-s 20042023 -l 54321`.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-s", "20042023", "-l", "54321"]`.
    *   Run `r_aurd_rechstan.py` with `["-s", "20042023", "-l", "54321"]`.
*   **Pass/Fail Criterion**:
    *   Both scripts must exit with code `0`.
    *   Both scripts' standard output/logs must indicate `Stichtag` as `20042023` and `Wiederanlaufwert` as `54321`.
    *   The parameters passed to `k_aurd_rechstan.ksh` and `run_core_job` must reflect these values.

```python
def test_both_parameters_provided(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Both Stichtag and Wiederanlaufwert parameters provided.
    Verifies both are correctly parsed and used.
    """
    test_stichtag = "20042023"
    test_wiederanlaufwert = 54321
    args = ["-s", test_stichtag, "-l", str(test_wiederanlaufwert)]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 0
    assert migrated_exit_code == 0

    assert f"Stichtag  : '{test_stichtag}'" in legacy_stdout
    assert f"Wiederanlaufwert: '{test_wiederanlaufwert}'" in legacy_stdout
    assert f"Stichtag  : '{test_stichtag}'" in migrated_stdout
    assert f"Wiederanlaufwert: '{test_wiederanlaufwert}'" in migrated_stdout

    assert f"-j BERT_RKOPF_STAN -s {test_stichtag} -f MOCK_DWMSG_ErmittleNr: 20230101103000 -l {test_wiederanlaufwert}" in legacy_k_args
    assert migrated_k_args == {
        'job_kennung': 'BERT_RKOPF_STAN',
        'stichtag': test_stichtag,
        'wiederanlaufwert': test_wiederanlaufwert
    }
```

### Test Case 5: Invalid Stichtag Format

*   **Purpose**: Verify error handling when an invalid date format is provided for `Stichtag`.
*   **Setup**: Execute both scripts with `-s 20230420` (YYYYMMDD instead of DDMMYYYY).
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-s", "20230420"]`.
    *   Run `r_aurd_rechstan.py` with `["-s", "20230420"]`.
*   **Pass/Fail Criterion**:
    *   **Legacy Script**: The provided legacy script snippet does not include explicit date format validation in the wrapper. It only checks if the parameter is *set*. Thus, it is expected to pass the invalid date to `k_aurd_rechstan.ksh` and exit `0` from the wrapper.
    *   **Migrated Script**: The Python script explicitly validates the date format. It must exit with code `193` (mimicking legacy error for invalid argument) and log an error message about the invalid format. The core job should *not* be invoked.
    *   **Note on Behavioral Difference**: This test highlights an intentional improvement in the migrated Python script, which adds explicit date format validation at the wrapper level. The legacy script would likely fail later in `k_aurd_rechstan.ksh` if it tried to use the malformed date.

```python
def test_invalid_stichtag_format(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Invalid Stichtag format.
    Verifies error handling for incorrect date format.
    """
    invalid_stichtag = "20230420" # Expected DDMMYYYY
    args = ["-s", invalid_stichtag]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    # Legacy script does not validate date format in the wrapper, so it should succeed (exit 0)
    # and pass the invalid date to the core script.
    assert legacy_exit_code == 0
    assert f"-j BERT_RKOPF_STAN -s {invalid_stichtag} -f MOCK_DWMSG_ErmittleNr: 20230101103000 -l 0" in legacy_k_args

    # Migrated script *does* validate date format and should fail with exit code 193.
    assert migrated_exit_code == 193
    assert f"ERROR: Invalid Stichtag format: {invalid_stichtag}. Expected DDMMYYYY." in migrated_stderr
    assert not mock_run_core_job.called # Core job should not be called
```

### Test Case 6: Unknown Parameter

*   **Purpose**: Verify error handling for unknown command-line arguments.
*   **Setup**: Execute both scripts with `-x unknown_arg`.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-x", "unknown_arg"]`.
    *   Run `r_aurd_rechstan.py` with `["-x", "unknown_arg"]`.
*   **Pass/Fail Criterion**:
    *   **Legacy Script**: Must exit with code `192` and log an error message indicating an unknown parameter.
    *   **Migrated Script**: Must exit with code `2` (standard `argparse` error for unknown arguments) and log an error message. The core job should *not* be invoked.
    *   **Note on Behavioral Difference**: The exit code differs (`192` vs `2`). This is an expected difference as the migration design specifies replacing custom error handling with cloud-native approaches.

```python
def test_unknown_parameter(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Unknown parameter.
    Verifies error handling for unknown command-line arguments.
    """
    unknown_arg = "unknown_param"
    args = ["-x", unknown_arg]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 192
    assert "MOCK_DWMSG_MeldeFehler: 0 E 192 x" in legacy_stderr # Legacy getopts reports '?' for unknown, then ErrArg is 'x'
    assert "Parameter unbekannt" in legacy_stdout # usage output
    assert not legacy_k_args # Core job should not be called

    assert migrated_exit_code == 2 # argparse default for unknown argument
    assert "unrecognized arguments: -x" in migrated_stderr
    assert not mock_run_core_job.called
```

### Test Case 7: Missing Argument for Parameter

*   **Purpose**: Verify error handling when a parameter requiring an argument (e.g., `-s`) is provided without one.
*   **Setup**: Execute both scripts with `-s`.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-s"]`.
    *   Run `r_aurd_rechstan.py` with `["-s"]`.
*   **Pass/Fail Criterion**:
    *   **Legacy Script**: Must exit with code `193` and log an error message indicating a missing argument.
    *   **Migrated Script**: Must exit with code `2` (standard `argparse` error for missing arguments) and log an error message. The core job should *not* be invoked.
    *   **Note on Behavioral Difference**: The exit code differs (`193` vs `2`). This is an expected difference as per the migration design.

```python
def test_missing_argument_for_parameter(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Missing argument for a parameter (e.g., -s without a date).
    Verifies error handling.
    """
    args = ["-s"]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 193
    assert "MOCK_DWMSG_MeldeFehler: 0 E 193 s" in legacy_stderr # Legacy getopts reports ':' for missing, then ErrArg is 's'
    assert "Notwendiges Argument fehlt" in legacy_stdout # usage output
    assert not legacy_k_args # Core job should not be called

    assert migrated_exit_code == 2 # argparse default for missing argument
    assert "argument -s: expected one argument" in migrated_stderr
    assert not mock_run_core_job.called
```

### Test Case 8: Help Flag (-h)

*   **Purpose**: Verify that the help message is displayed correctly and the script exits successfully when the `-h` flag is used.
*   **Setup**: Execute both scripts with `-h`.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with `["-h"]`.
    *   Run `r_aurd_rechstan.py` with `["-h"]`.
*   **Pass/Fail Criterion**:
    *   Both scripts must exit with code `0`.
    *   Both scripts' standard output must contain the usage message, including parameter descriptions.
    *   The core job should *not* be invoked.

```python
def test_help_flag(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Help flag (-h).
    Verifies help message is displayed and script exits successfully.
    """
    args = ["-h"]

    # --- Legacy Script Execution ---
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = run_legacy_script(args, legacy_env_setup)

    # --- Migrated Script Execution ---
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script(args, mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 0
    assert "Programm: Erzeugung eines Abzugs der Rechnungsdaten" in legacy_stdout
    assert "Parameter:" in legacy_stdout
    assert "-s     Stichtag DDMMYYYY" in legacy_stdout
    assert not legacy_k_args # Core job should not be called

    assert migrated_exit_code == 0
    assert "Programm: Erzeugung eines Abzugs der Rechnungsdaten" in migrated_stdout
    assert "Parameter:" in migrated_stdout
    assert "-s     Stichtag DDMMYYYY" in migrated_stdout
    assert not mock_run_core_job.called
```

### Test Case 9: Core Job Failure

*   **Purpose**: Verify that if the core processing step (`k_aurd_rechstan.ksh` or `run_core_job`) fails, the wrapper script correctly captures the error, logs it, and exits with a non-zero status.
*   **Setup**: Configure the mock `k_aurd_rechstan.ksh` to exit with an error, and configure `mock_run_core_job` to raise an exception.
*   **Action**:
    *   Run `r_aurd_rechstan.ksh` with an argument that triggers the mock core job failure (e.g., `--fail`).
    *   Run `r_aurd_rechstan.py` with `mock_run_core_job` configured to raise an exception.
*   **Pass/Fail Criterion**:
    *   Both scripts must exit with a non-zero code (legacy: `1`, migrated: `1`).
    *   Both scripts' standard error/logs must contain messages indicating the failure.

```python
def test_core_job_failure(legacy_env_setup, mock_run_core_job, mock_datetime_now, capsys):
    """
    Test case: Core job (k_aurd_rechstan.ksh / run_core_job) fails.
    Verifies wrapper script handles the error and exits with a non-zero code.
    """
    # --- Legacy Script Execution ---
    # Pass a special argument to mock k_aurd_rechstan.ksh to make it fail
    legacy_stdout, legacy_stderr, legacy_exit_code, legacy_k_args = \
        run_legacy_script(["--fail"], legacy_env_setup) # --fail is handled by mock k_aurd_rechstan.ksh

    # --- Migrated Script Execution ---
    # Make mock_run_core_job raise an exception
    mock_run_core_job.side_effect = Exception("Simulated core job failure")
    migrated_stdout, migrated_stderr, migrated_exit_code, migrated_k_args = \
        run_migrated_script([], mock_run_core_job, mock_datetime_now, capsys)

    # --- Assertions ---
    assert legacy_exit_code == 1 # Legacy script exits 1 on trap ERR
    assert "AppError: Abbruch" in legacy_stderr # Check for trap message
    assert "-j BERT_RKOPF_STAN -s 01012023 -f MOCK_DWMSG_ErmittleNr: 20230101103000 -l 0 --fail" in legacy_k_args # Ensure mock was called with fail arg

    assert migrated_exit_code == 1 # Migrated script exits 1 on generic exception
    assert "Job failed with error: Simulated core job failure" in migrated_stderr
    assert mock_run_core_job.called # Ensure core job was attempted
```