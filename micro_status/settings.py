import os

from dotenv import load_dotenv


FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire"
HIVE_ACQUISITION_FOLDER = "/CBI_Hive/Acquire"
DB_LOCATION = "/CBI_FastStore/Iana/RSCM_datasets.db"

SLACK_URL = "https://slack.com/api/chat.postMessage"
load_dotenv()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL")
SLACK_HEADERS = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {os.getenv("SLACK_TOKEN")}'}
PROGRESS_TIMEOUT = 600  # seconds
RSCM_FOLDER_STITCHING = "/CBI_FastStore/clusterStitchTEST"
RSCM_FOLDER_BUILDING_IMS = "/CBI_FastStore/clusterStitch"
CBPY_FOLDER = "/CBI_FastStore/clusterPy"
DASK_DASHBOARD = os.getenv("DASK_DASHBOARD")
CHROME_DRIVER_PATH = '/CBI_Hive/CBI/Iana/projects/internal/micro_status/chromedriver'
MAX_ALLOWED_STORAGE_PERCENT = 94
STORAGE_THRESHOLD_0 = 85
STORAGE_THRESHOLD_1 = 90
CHECKING_TIFFS_ENABLED = True
MESSAGES_ENABLED = True
WHERE_PROCESSING_HAPPENS = {
    'stitch': 'faststore',
    'build_composites': 'faststore',
    'denoise': 'faststore',
    'build_ims': 'faststore'
}
DATA_LOCATION = {
    'faststore': FASTSTORE_ACQUISITION_FOLDER,
    'hive': HIVE_ACQUISITION_FOLDER
}
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
