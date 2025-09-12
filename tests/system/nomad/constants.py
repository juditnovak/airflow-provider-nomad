from pathlib import Path

SYSTEST_ROOT = Path(__file__).resolve().parent
TEST_CONFIG_PATH = SYSTEST_ROOT / "config"
TEST_SCRIPTS_PATH = SYSTEST_ROOT / "scripts"
TEST_DATA_PATH = SYSTEST_ROOT / "data"
TEST_DAGS_PATH = SYSTEST_ROOT / "dags"
TEST_DAGS_LOCALEXECUTOR_PATH = SYSTEST_ROOT / "dags_localexecutor"
