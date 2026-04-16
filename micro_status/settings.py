import json
import os

from dotenv import load_dotenv

try:
    from . import local_settings as _local_settings
except ImportError:
    _local_settings = None


def _local_override(name, default=None):
    if _local_settings and hasattr(_local_settings, name):
        return getattr(_local_settings, name)
    return default


def _setting(name, default=None):
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value
    return _local_override(name, default)


def _bool_setting(name, default=False):
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value.lower() in {"1", "true", "yes", "on"}
    return _local_override(name, default)


def _list_setting(name, default=None):
    env_value = os.getenv(name)
    if env_value is not None:
        return [x.strip() for x in env_value.split(',') if x.strip()]
    value = _local_override(name, default or [])
    return list(value)


def _dict_setting(name, default=None):
    env_value = os.getenv(name)
    if env_value is not None:
        return json.loads(env_value)
    value = _local_override(name, default or {})
    return dict(value)


os.umask(0o002)
load_dotenv()

FASTSTORE_ACQUISITION_FOLDER = _setting("FASTSTORE_ACQUISITION_FOLDER", "/path/to/faststore/Acquire")
RSCM_FASTSTORE_ACQUISITION_FOLDER = _setting(
    "RSCM_FASTSTORE_ACQUISITION_FOLDER",
    os.path.join(FASTSTORE_ACQUISITION_FOLDER, "RSCM"),
)
MESOSPIM_FASTSTORE_ACQUISITION_FOLDER = _setting(
    "MESOSPIM_FASTSTORE_ACQUISITION_FOLDER",
    os.path.join(FASTSTORE_ACQUISITION_FOLDER, "MesoSPIM"),
)
HIVE_ACQUISITION_FOLDER = _setting("HIVE_ACQUISITION_FOLDER", "/path/to/hive/Acquire")
RSCM_HIVE_ACQUISITION_FOLDER = _setting(
    "RSCM_HIVE_ACQUISITION_FOLDER",
    os.path.join(HIVE_ACQUISITION_FOLDER, "RSCM"),
)
MESOSPIM_HIVE_ACQUISITION_FOLDER = _setting(
    "MESOSPIM_HIVE_ACQUISITION_FOLDER",
    os.path.join(HIVE_ACQUISITION_FOLDER, "MesoSPIM"),
)
DB_LOCATION = _setting("DB_LOCATION", "/path/to/RSCM_MesoSPIM_datasets.db")
DB_BACKUPS_DIR = _setting("DB_BACKUPS_DIR", "/path/to/db_backups")
FASTSTORE_TRASH_LOCATION = _setting("FASTSTORE_TRASH_LOCATION", "/path/to/faststore/trash")
HIVE_TRASH_LOCATION = _setting("HIVE_TRASH_LOCATION", "/path/to/hive/trash")

SLACK_URL = _setting("SLACK_URL", "https://slack.com/api/chat.postMessage")
SLACK_CHANNEL_ID = _setting("SLACK_CHANNEL", "")
SLACK_HEADERS = {
    'content-type': 'application/json',
    'Accept-Charset': 'UTF-8',
    'Authorization': f'Bearer {_setting("SLACK_TOKEN", "")}',
}
PROGRESS_TIMEOUT = int(_setting("PROGRESS_TIMEOUT", 1200))
RSCM_FOLDER_STITCHING = _setting("RSCM_FOLDER_STITCHING", "/path/to/clusterStitchTEST")
RSCM_FOLDER_BUILDING_IMS = _setting("RSCM_FOLDER_BUILDING_IMS", "/path/to/clusterStitch")
CBPY_FOLDER = _setting("CBPY_FOLDER", "/path/to/clusterPy")
CHROME_DRIVER_PATH = _setting("CHROME_DRIVER_PATH", "/path/to/chromedriver")
MAX_ALLOWED_STORAGE_PERCENT = int(_setting("MAX_ALLOWED_STORAGE_PERCENT", 94))
STORAGE_THRESHOLD_0 = int(_setting("STORAGE_THRESHOLD_0", 85))
STORAGE_THRESHOLD_1 = int(_setting("STORAGE_THRESHOLD_1", 90))
CHECKING_TIFFS_ENABLED = _bool_setting("CHECKING_TIFFS_ENABLED", True)
MESSAGES_ENABLED = _bool_setting("MESSAGES_ENABLED", True)
WHERE_PROCESSING_HAPPENS = _dict_setting(
    "WHERE_PROCESSING_HAPPENS",
    {
        'stitch': 'faststore',
        'build_composites': 'faststore',
        'denoise': 'faststore',
        'build_ims': 'hive',
    },
)
DATA_LOCATION = _dict_setting(
    "DATA_LOCATION",
    {
        'faststore': RSCM_FASTSTORE_ACQUISITION_FOLDER,
        'hive': RSCM_HIVE_ACQUISITION_FOLDER,
    },
)
DATETIME_FORMAT = _setting("DATETIME_FORMAT", "%Y-%m-%d_%H-%M-%S")
PEACE_JSON_FOLDER = _setting("PEACE_JSON_FOLDER", "/path/to/json")
BRAIN_DATA_PRODUCERS = _list_setting("BRAIN_DATA_PRODUCERS", [])
OME_ZARR_PIS = set(_list_setting("OME_ZARR_PIS", []))

DASK_JSON_PATH = _setting("DASK_JSON_PATH", "/path/to/dask_scheduler_info.json")
if os.path.exists(DASK_JSON_PATH):
    with open(DASK_JSON_PATH, "r") as f:
        dask_json = json.load(f)
    DASK_DASHBOARD = dask_json['address'].replace("tcp", "http")[:-4] + '8787/'
else:
    dask_json = None
    DASK_DASHBOARD = _setting("DASK_DASHBOARD", None)

RESTRICT_MOVING_TIME = _bool_setting("RESTRICT_MOVING_TIME", True)
MOVE_TIMES = _dict_setting("MOVE_TIMES", {'start': 17, 'stop': 6})

MESOSPIM_AUTO_STITCH_FOLDER = _setting("MESOSPIM_AUTO_STITCH_FOLDER", "/path/to/stitch_jobs")
NEW_DATASET_MARKER_FILENAME = _setting("NEW_DATASET_MARKER_FILENAME", ".microstatus.json")
