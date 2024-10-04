import os
import json

from dotenv import load_dotenv


FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire"
HIVE_ACQUISITION_FOLDER = "/h20/Acquire"
DB_LOCATION = "/CBI_FastStore/Iana/RSCM_datasets.db"
FASTSTORE_TRASH_LOCATION = "/CBI_FastStore/trash"
HIVE_TRASH_LOCATION = "/h20/trash"

SLACK_URL = "https://slack.com/api/chat.postMessage"
load_dotenv()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL")
SLACK_HEADERS = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {os.getenv("SLACK_TOKEN")}'}
PROGRESS_TIMEOUT = 600  # seconds
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
WHERE_PROCESSING_HAPPENS = {
    'stitch': 'faststore',
    'build_composites': 'faststore',
    'denoise': 'faststore',
    'build_ims': 'hive'
}
DATA_LOCATION = {
    'faststore': FASTSTORE_ACQUISITION_FOLDER,
    'hive': HIVE_ACQUISITION_FOLDER
}
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
PEACE_JSON_FOLDER = "/h20/CBI/Iana/json"
BRAIN_DATA_PRODUCERS = ["klimstra", "cebra", "dutta", "dermody"]

dask_json = json.load(open("/CBI_FastStore/cbiPythonTools/RSCM/RSCM/dask_scheduler_info.json", "r"))
DASK_DASHBOARD = dask_json['address'].replace("tcp", "http")[:-4] + '8787/'
