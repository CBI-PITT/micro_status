FASTSTORE_ACQUISITION_FOLDER = "/path/to/folder"
RSCM_FASTSTORE_ACQUISITION_FOLDER = "/path/to/folder"
MESOSPIM_FASTSTORE_ACQUISITION_FOLDER = "/path/to/folder"
HIVE_ACQUISITION_FOLDER = "/path/to/folder"
RSCM_HIVE_ACQUISITION_FOLDER = "/path/to/folder"
MESOSPIM_HIVE_ACQUISITION_FOLDER = "/path/to/folder"
DB_LOCATION = "/path/to/folder"
DB_BACKUPS_DIR = "/path/to/folder"
FASTSTORE_TRASH_LOCATION = "/path/to/folder"
HIVE_TRASH_LOCATION = "/path/to/folder"
RSCM_FOLDER_STITCHING = "/path/to/folder"
RSCM_FOLDER_BUILDING_IMS = "/path/to/folder"
CBPY_FOLDER = "/path/to/folder"
CHROME_DRIVER_PATH = "/path/to/folder"
PEACE_JSON_FOLDER = "/path/to/folder"
BRAIN_DATA_PRODUCERS = []
OME_ZARR_PIS = []
DASK_JSON_PATH = "/path/to/file.json"
MESOSPIM_AUTO_STITCH_FOLDER = "/path/to/folder"
WHERE_PROCESSING_HAPPENS = {
    'stitch': 'faststore',
    'build_composites': 'faststore',
    'denoise': 'faststore',
    'build_ims': 'hive',
}
DATA_LOCATION = {
    'faststore': RSCM_FASTSTORE_ACQUISITION_FOLDER,
    'hive': RSCM_HIVE_ACQUISITION_FOLDER,
}
MOVE_TIMES = {'start': 17, 'stop': 6}
