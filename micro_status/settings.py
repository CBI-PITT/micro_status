import os
import json

from dotenv import load_dotenv


os.umask(0o002)

FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire/"
RSCM_FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire/RSCM"
MESOSPIM_FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire/MesoSPIM"
HIVE_ACQUISITION_FOLDER = "/h20/Acquire/"
RSCM_HIVE_ACQUISITION_FOLDER = "/h20/Acquire/RSCM"
MESOSPIM_HIVE_ACQUISITION_FOLDER = "/h20/Acquire/MesoSPIM"
DB_LOCATION = "/CBI_FastStore/Iana/RSCM_MesoSPIM_datasets.db"
DB_BACKUPS_DIR = "/CBI_FastStore/Iana/db_backups"
FASTSTORE_TRASH_LOCATION = "/CBI_FastStore/trash"
HIVE_TRASH_LOCATION = "/h20/trash"

SLACK_URL = "https://slack.com/api/chat.postMessage"
load_dotenv()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL")
SLACK_HEADERS = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {os.getenv("SLACK_TOKEN")}'}
PROGRESS_TIMEOUT = 1200  # seconds
RSCM_FOLDER_STITCHING = "/CBI_FastStore/clusterStitchTEST"
RSCM_FOLDER_BUILDING_IMS = "/CBI_FastStore/clusterStitch"
CBPY_FOLDER = "/CBI_FastStore/clusterPy"
# DASK_DASHBOARD = os.getenv("DASK_DASHBOARD")
CHROME_DRIVER_PATH = '/h20/CBI/Iana/projects/internal/micro_status/chromedriver'
MAX_ALLOWED_STORAGE_PERCENT = 94
STORAGE_THRESHOLD_0 = 85
STORAGE_THRESHOLD_1 = 90
CHECKING_TIFFS_ENABLED = True
MESSAGES_ENABLED = True
# MESSAGES_ENABLED = False
WHERE_PROCESSING_HAPPENS = {
    'stitch': 'faststore',
    'build_composites': 'faststore',
    'denoise': 'faststore',
    'build_ims': 'hive'
}
DATA_LOCATION = {
    'faststore': RSCM_FASTSTORE_ACQUISITION_FOLDER,
    'hive': RSCM_HIVE_ACQUISITION_FOLDER
}
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
PEACE_JSON_FOLDER = "/h20/CBI/Iana/json"
BRAIN_DATA_PRODUCERS = ["klimstra", "cebra", "dutta", "dermody"]
OME_ZARR_PIS = set('klimstra-w', 'delima-s')

DASK_JSON_PATH = "/CBI_FastStore/cbiPythonTools/RSCM/RSCM/dask_scheduler_info.json"
if os.path.exists(DASK_JSON_PATH):
    dask_json = json.load(open(DASK_JSON_PATH, "r"))
    DASK_DASHBOARD = dask_json['address'].replace("tcp", "http")[:-4] + '8787/'
else:
    dask_json = None
    DASK_DASHBOARD = None

ALLOW_MOVES_ANYTIME = True
RESTRICT_MOVING_TIME = False
MOVE_TIMES = {'start': 17, 'stop': 6}

MESOSPIM_AUTO_STITCH_FOLDER = "/CBI_FastStore/tmp/stitch_jobs"
MOVE_JOBS_DIR = "/CBI_FastStore/tmp/move_jobs"
NEW_DATASET_MARKER_FILENAME = ".microstatus.json"
