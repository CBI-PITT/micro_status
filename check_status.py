"""
Database schema:

Dataset:
    id
    name
    path_on_fast_store
    vs_series_file - OneToOne to VSSeriesFile
    cl_number - ForeignKey to CLNumber
    pi - ForeignKey to PI
    imaging_status (in_progress, paused, finished)
    processing_status (not_started, started, stitched, moved_to_hive, denoised, built_ims, finished)
    path_on_hive
    job_number
    imaris_file_path
    channels
    z_layers_total
    z_layers_current
    ribbons_total
    ribbons_finished
    imaging_no_progress_time
    processing_no_progress_time
    z_layers_checked
    keep_composites
    delete_405

VSSeriesFile:
    id
    path

PI:
    id
    name
    public_folfer_name

CLNumber:
    id
    name
    pi = ForeignKey to PI

Warning:
    id
    type  (space_hive_thr0, space_hive_thr1, low_space_hive, space_faststore_thr0, space_faststore_thr1, low_space_faststore)
    active
    message_sent

-------------------------------------
Messages:
    1) imaging started
    2) imaging paused (crashed?)
    2a) imaging resumed
    3) imaging finished
    4) processing started
    5) processing_paused (crashed?)
    6) processing finished

Other warnings:
    1) Low space on Hive
    2) Low space on FastStore


pip install python-dotenv
"""

import json
import logging
import os
import re
import requests
import subprocess
import sqlite3
import time
from datetime import datetime
from glob import glob
from pathlib import Path, PureWindowsPath

import tifffile
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from imaris_ims_file_reader import ims

console_handler = logging.StreamHandler()
LOG_FILE_NAME_PATTERN = "/CBI_FastStore/Iana/bot_logs/{}_{}.txt"
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
file_handler = logging.FileHandler(
    LOG_FILE_NAME_PATTERN.format(
        os.uname().nodename,
        datetime.now().strftime(DATETIME_FORMAT)
    )
)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s - %(levelname)s - %(message)s',
    handlers=[console_handler, file_handler]
)
log = logging.getLogger(__name__)


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


def check_if_new(file_path):
    con = sqlite3.connect(DB_LOCATION)
    con.row_factory = lambda cursor, row: row[0]
    cur = con.cursor()
    vs_series_file_records = cur.execute('SELECT path FROM vsseriesfile').fetchall()
    con.close()
    return file_path not in vs_series_file_records


def read_dataset_record(file_path):
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    record = cur.execute(f'SELECT * FROM dataset WHERE path_on_fast_store="{str(file_path)}"').fetchone()
    con.close()

    if not record:
        print("WARNING: broken/partial dataset", file_path)
        return

    pi_id = record[5]
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    pi_name = cur.execute(f'SELECT name FROM pi WHERE id="{pi_id}"').fetchone()
    con.close()
    if pi_name:
        pi_name = pi_name[0]

    cl_number_id = record[4]
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    cl_number = cur.execute(f'SELECT name FROM clnumber WHERE id="{cl_number_id}"').fetchone()
    con.close()
    if cl_number:
        cl_number = cl_number[0]

    dataset = Dataset(
        db_id = record[0],
        name = record[1],
        path_on_fast_store = record[2],
        cl_number = cl_number,
        pi = pi_name,
        imaging_status = record[6],
        processing_status = record[7],
        channels = record[11],
        z_layers_total = record[12],
        z_layers_current = record[13],
        ribbons_total = record[14],
        ribbons_finished = record[15],
        imaging_no_progress_time = record[16],
        processing_no_progress_time = record[17],
        z_layers_checked = record[19],
        keep_composites = record[20],
        delete_405 = record[21]
    )
    return dataset


class Found(BaseException):
    pass


class Dataset:
    def __init__(self, **kwargs):
        self.db_id = kwargs.get('db_id')
        self.name = kwargs.get('name')
        self.path_on_fast_store = kwargs.get('path_on_fast_store')
        self.cl_number = kwargs.get('cl_number')
        self.pi = kwargs.get('pi')
        self.imaging_status = kwargs.get('imaging_status')
        self.processing_status = kwargs.get('processing_status')
        self.path_on_hive = kwargs.get('path_on_hive')
        self.job_number = kwargs.get('job_number')
        self.imaris_file_path = kwargs.get('imaris_file_path')
        self.channels = kwargs.get('channels')
        self.z_layers_total = kwargs.get('z_layers_total')
        self.z_layers_current = kwargs.get('z_layers_current')
        self.z_layers_checked = kwargs.get('z_layers_checked')
        self.ribbons_total = kwargs.get('ribbons_total')
        self.ribbons_finished = kwargs.get('ribbons_finished')
        self.imaging_no_progress_time = kwargs.get('imaging_no_progress_time')
        self.processing_no_progress_time = kwargs.get('processing_no_progress_time')
        self.z_layers_checked = kwargs.get('z_layers_checked')
        self.keep_composites = kwargs.get('keep_composites')
        self.delete_405 = kwargs.get('delete_405')

    def __str__(self):
        return f"{self.db_id} {self.pi} {self.cl_number} {self.name}"

    @classmethod
    def create(cls, file_path):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'INSERT OR IGNORE INTO vsseriesfile(path) VALUES("{file_path}")')
        vs_series_file_id = cur.lastrowid
        con.commit()
        con.close()

        file_path = Path(file_path)
        path_parts = file_path.parts
        last_name_pattern = r"^[A-Za-z '-]+$"
        pi_name = path_parts[3] if re.findall(last_name_pattern, path_parts[3]) else None
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'SELECT id FROM pi WHERE name = "{pi_name}"')
        pi_id = res.fetchone()
        con.close()
        if pi_id:
            pi_id = pi_id[0]
        else:
            con = sqlite3.connect(DB_LOCATION)
            cur = con.cursor()
            res = cur.execute(f'INSERT OR IGNORE INTO pi(name) VALUES("{pi_name}")')
            pi_id = cur.lastrowid
            con.commit()
            con.close()

        cl_number = [x for x in path_parts if 'CL' in x.upper()]
        cl_number = None if len(cl_number) == 0 else cl_number[0]
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'SELECT id FROM clnumber WHERE name = "{cl_number}"')
        cl_number_id = res.fetchone()
        con.close()

        if cl_number_id:
            cl_number_id = cl_number_id[0]
        else:
            con = sqlite3.connect(DB_LOCATION)
            cur = con.cursor()
            res = cur.execute(f'INSERT OR IGNORE INTO clnumber(name, pi) VALUES("{cl_number}", {pi_id})')
            cl_number_id = cur.lastrowid
            con.commit()
            con.close()

        dataset_name = file_path.parent.name if "stack" in file_path.parent.name else None

        print("Path", file_path, "pi_name", pi_name, "cl_number", cl_number, "dataset_name", dataset_name)

        with open(file_path, 'r') as f:
            data = f.read()

        soup = BeautifulSoup(data, "xml")
        z_layers = int(soup.find('stack_slice_count').text)
        ribbons_in_z_layer = int(soup.find('grid_cols').text)
        # layer_dirs = [x.path for x in os.scandir(file_path.parent) if x.is_dir()]
        # for layer in range(int(z_layers) -1, 0, -1):

        ribbons_finished = 0
        subdirs = os.scandir(file_path.parent)
        for subdir in subdirs:
            if subdir.is_file() or 'layer' not in subdir.name:
                continue
            color_dirs = [x.path for x in os.scandir(subdir.path) if x.is_dir()]
            channels = len(color_dirs)
            for color_dir in color_dirs:
                images_dir = os.path.join(color_dir, 'images')
                ribbons = len(os.listdir(images_dir))
                ribbons_finished += ribbons
                if ribbons < ribbons_in_z_layer:
                    break
        current_z_layer = re.findall(r"\d+", subdir.name)[-1]
        # imaging_status = "finished" if current_z_layer == z_layers else "in_progress"
        ribbons_total = z_layers * channels * ribbons_in_z_layer

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'''INSERT OR IGNORE INTO dataset(name, path_on_fast_store, vs_series_file, cl_number, pi, 
        imaging_status, processing_status, channels, z_layers_total, z_layers_current, ribbons_total, ribbons_finished) 
        VALUES("{dataset_name}", "{file_path}", "{vs_series_file_id}", "{cl_number_id}", "{pi_id}", 
        "in_progress", "not_started", "{channels}", "{z_layers}", "{current_z_layer}", "{ribbons_total}", "{ribbons_finished}")
        '''
        )
        dataset_id = cur.lastrowid
        con.commit()
        con.close()

        dataset = cls(
            db_id=dataset_id,
            path_on_fast_store=file_path,
            pi=pi_name,
            cl_number=cl_number,
            name=dataset_name,
            imaging_status="in_progress",
            processing_status="not_started",
            channels=channels,
            z_layers_total=z_layers,
            ribbons_total=ribbons_total,
            z_layers_current=z_layers - 1,
            ribbons_finished=0,
            imaging_no_progress_time=None,
            processing_no_progress_time=None,
            keep_composites=0,
            delete_405=0
        )
        return dataset

    def check_imaging_progress(self):
        error_flag = False
        file_path = Path(self.path_on_fast_store)
        ribbons_finished = 0  # TODO: optimize, start with current z layer, not mrom 0
        subdirs = sorted(glob(os.path.join(file_path.parent, '*')), reverse=True)
        subdirs = [x for x in subdirs if os.path.isdir(x) and 'layer' in x]
        if len(subdirs) > 1000:
            len4 = lambda x: len(re.findall(r"\d+", os.path.basename(x))[-1]) == 4
            len3 = lambda x: len(re.findall(r"\d+", os.path.basename(x))[-1]) == 3
            subdirs_0 = filter(len4, subdirs)
            subdirs_1 = filter(len3, subdirs)
            subdirs = list(subdirs_0) + list(subdirs_1)

        try:
            for subdir in subdirs:
                color_dirs = [x.path for x in os.scandir(subdir) if x.is_dir()]
                for color_dir in color_dirs:
                    images_dir = os.path.join(color_dir, 'images')
                    ribbons = len(os.listdir(images_dir))
                    ribbons_finished += ribbons
                    if ribbons < self.ribbons_in_z_layer:
                        raise Found
        except Found:
            pass
        finally:
            z_layers_current = re.findall(r"\d+", os.path.basename(subdir))[-1]
        print("current imaging z layer :", z_layers_current)

        finished = ribbons_finished == self.ribbons_total

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET ribbons_finished = {ribbons_finished} WHERE id={self.db_id}')
        con.commit()
        con.close()
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET z_layers_current = {z_layers_current} WHERE id={self.db_id}')
        con.commit()
        con.close()

        ribbons_finished_prev = self.ribbons_finished
        self.ribbons_finished = ribbons_finished
        self.z_layers_current = z_layers_current
        has_progress = ribbons_finished > ribbons_finished_prev

        if CHECKING_TIFFS_ENABLED and self.imaging_status == "in_progress":
            print("self.z_layers_checked", self.z_layers_checked)
            if finished:  # only check layer 0 (last layer)
                z_start = 0
                z_stop = -1
            else:
                z_start = int((self.z_layers_checked - 1) if self.z_layers_checked is not None else self.z_layers_total)
                # z_start = int((self.z_layers_checked or self.z_layers_total) - 1)
                z_stop = int(z_layers_current)

            time.sleep(10)  # wait, in case if last image is still being saved
            bad_layer = self.check_tiffs(z_start, z_stop)
            if bad_layer is not None:
                error_flag = True
                self.z_layers_current = bad_layer

        return finished, has_progress, error_flag

    def send_message(self, msg_type):
        log.info("---------------------Sending message------------------------")
        msg_map = {
            'imaging_started': "Imaging of {} {} {} *_started_*",
            'imaging_finished': "Imaging of {} {} {} *_finished_*",
            'imaging_paused': "*WARNING: Imaging of {} {} {} paused at z-layer {}*",
            # 'imaging_resumed': "Imaging of {} {} {} *_resumed_*",
            'processing_started': "Processing of {} {} {} started",
            'processing_finished': "Imaris file built for {} {} {}. Check it out at {}",
            'broken_ims_file': "*WARNING: Broken Imaris file at {} {} {}.*",
            'stitching_error': "*WARNING: Stitching error {} {} {}. Txt file in error folder.*",
            'stitching_stuck': "*WARNING: Stitching of {} {} {} could be stuck. Check cluster.*",
            'denoising_stuck': "*WARNING: Denoising of {} {} {} could be stuck. Check CBPy.*",
            'ims_build_stuck': "*WARNING: Building of Imaris file for {} {} {} could be stuck. Check conversion tool.*",
            'broken_tiff_file': "*WARNING: Broken tiff file in {} {} {} z-layer {}*"
        }
        if msg_type in ['imaging_paused', 'broken_tiff_file']:
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name, self.z_layers_current)
        elif msg_type == 'processing_finished':
            ims_folder = str(PureWindowsPath(str(Path(self.imaris_file_path).parent).replace('/CBI_Hive', 'H:')))
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name, ims_folder)
        else:
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name)
        log.info("Message text:", msg_text)
        payload = {
            "channel": SLACK_CHANNEL_ID,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": msg_text
                    }
                }
            ]
        }
        response = requests.post(SLACK_URL, data=json.dumps(payload), headers=SLACK_HEADERS)
        return response

    def mark_no_imaging_progress(self):
        progress_stopped_at = datetime.now().strftime(DATETIME_FORMAT)
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_no_progress_time = "{progress_stopped_at}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.imaging_no_progress_time = progress_stopped_at

    def mark_paused(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_status = "paused" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.imaging_status = "paused"

    def mark_has_imaging_progress(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_no_progress_time = null WHERE id={self.db_id}')
        con.commit()
        con.close()

        self.imaging_no_progress_time = None

    def mark_resumed(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_status = "in_progress" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.imaging_status = "in_progress"

    def mark_imaging_finished(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_status = "finished" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.imaging_status = "finished"

    @property
    def ribbons_in_z_layer(self):
        # TODO: fails here if file was removed
        with open(self.path_on_fast_store, 'r') as f:
            data = f.read()
        soup = BeautifulSoup(data, "xml")
        return int(soup.find('grid_cols').text)

    @property
    def rscm_txt_file_name(self):
        return f"{str(self.db_id).zfill(5)}_{self.pi}_{self.cl_number}_{self.name}.txt"

    def start_processing(self):
        """create txt file in the RSCM queue stitch directory
        file name: {dataset_id}_{pi_name}_{cl_number}_{dataset_name}.txt
        this way the earlier datasets go in first
        """
        dat_file_path = Path(self.path_on_fast_store)
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', self.rscm_txt_file_name)
        contents = f'rootDir="{str(dat_file_path.parent)}"\nkeepComposites=True'
        with open(txt_file_path, "w") as f:
            f.write(contents)
        log.info("-----------------------Queue processing. Text file : ---------------------")
        log.info(contents)

    def clean_up_composites(self):
        if self.path_on_hive and self.imaris_file_path:
            denoised_composites = sorted(glob(os.path.join(os.path.dirname(self.imaris_file_path), 'composite_*.tif')))
            raw_composites = sorted(glob(os.path.join(self.path_on_hive, "composites_RSCM_v0.1", 'composite_*.tif')))
            log.info("---------------------Cleaning up composites--------------------")
            for f in denoised_composites + raw_composites:
                log.info("Will remove:", f)
                # os.remove(f)

    @classmethod
    def initialize_from_db(cls, record):
        pi_id = record[5]
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        pi_name = cur.execute(f'SELECT name FROM pi WHERE id="{pi_id}"').fetchone()
        con.close()
        if pi_name:
            pi_name = pi_name[0]

        cl_number_id = record[4]
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        cl_number = cur.execute(f'SELECT name FROM clnumber WHERE id="{cl_number_id}"').fetchone()
        con.close()

        if cl_number:
            cl_number = cl_number[0]
        obj = cls(
            db_id = record[0],
            name = record[1],
            path_on_fast_store = record[2],
            cl_number = cl_number,
            pi = pi_name,
            imaging_status = record[6],
            processing_status = record[7],
            path_on_hive = record[8],
            job_number = record[9],
            imaris_file_path = record[10],
            channels = record[11],
            z_layers_total = record[12],
            z_layers_current = record[13],
            ribbons_total = record[14],
            ribbons_finished = record[15],
            imaging_no_progress_time = record[16],
            processing_no_progress_time = record[17],
            z_layers_checked = record[19],
            keep_composites = record[20],
            delete_405 = record[21]
        )
        return obj

    def update_path_on_hive(self, path_on_hive):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET path_on_hive = "{path_on_hive}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.path_on_hive = path_on_hive

    def update_processing_status(self, processing_status):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET processing_status = "{processing_status}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.processing_status = processing_status

    def update_job_number(self, job_number):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET job_number = "{job_number}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.job_number = job_number

    def update_imaris_file_path(self, ims_file_path):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaris_file_path = "{ims_file_path}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.imaris_file_path = ims_file_path

    def get_processing_summary(self):
        processing_summary = {}
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        processing_summary_str = cur.execute(f'SELECT processing_summary FROM dataset WHERE id={self.db_id}').fetchone()
        con.close()
        if processing_summary_str and processing_summary_str[0] is not None:
            processing_summary = json.loads(processing_summary_str[0])
        return processing_summary

    def update_processing_summary(self, to_update):
        processing_summary = self.get_processing_summary()
        print("processing_summary before:", processing_summary)
        processing_summary.update(to_update)
        print("processing_summary after", processing_summary)
        processing_summary_str = json.dumps(processing_summary)
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f"UPDATE dataset SET processing_summary = '{processing_summary_str}' WHERE id={self.db_id}")
        con.commit()
        con.close()

    def check_being_stitched(self):
        """
        Check if current dataset's txt file is in 'processing' dir for stitching
        :return: bool
        """
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'processing', self.rscm_txt_file_name)
        return os.path.exists(txt_file_path)

    def check_stitching_progress(self):
        response = requests.get(f'{DASK_DASHBOARD}info/main/workers.html')
        soup = BeautifulSoup(response.content)
        trs = soup.select('tr')
        workers = {}
        for tr in trs[1:]:
            a = tr.find('td').find('a')
            worker_url = a.attrs['href'].replace('../', f'{DASK_DASHBOARD}info/')
            resp = requests.get(worker_url)
            soup = BeautifulSoup(resp.content)
            tables = soup.select('table')
            rows = tables[2].select("tr")
            workers[a.text] = len(rows) - 1
        processing_summary = self.get_processing_summary()
        workers_previous = processing_summary.get('stitching', {})
        has_progress = workers != workers_previous
        print("---------------------------stitching has progress", has_progress)
        if has_progress:
            self.update_processing_summary({"stitching": workers})
        return has_progress

    def check_stitching_complete(self):
        """
        Check if current dataset's txt file is in 'complete' dir
        :return: bool
        """
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'complete', self.rscm_txt_file_name)
        return os.path.exists(txt_file_path)

    def check_stitching_errored(self):
        """
        Check if current dataset's txt file is in 'error' dir
        :return: bool
        """
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'error', self.rscm_txt_file_name)
        return os.path.exists(txt_file_path)

    def mark_has_processing_progress(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET processing_no_progress_time = null WHERE id={self.db_id}')
        con.commit()
        con.close()

        self.processing_no_progress_time = None

    def mark_no_processing_progress(self):
        progress_stopped_at = datetime.now().strftime(DATETIME_FORMAT)
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET processing_no_progress_time = "{progress_stopped_at}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.processing_no_progress_time = progress_stopped_at

    def check_tiffs(self, z_start, z_stop):
        """
        z_start > z_stop, because imaging from top to bottom
        :param z_start:
        :param z_stop:
        :return:
        """
        print("--------------------checking tiff files-----------------")
        print("z start", z_start, "z stop", z_stop)
        for z in range(z_start, z_stop, -1):
            print("checking layer", z)
            layer_dir = os.path.join(
                str(Path(self.path_on_fast_store).parent),
                f'{self.name.split("_stack")[0]}_layer{str(z).zfill(3) if z < 1000 else str(z)}'
            )
            colors = glob(os.path.join(layer_dir, '[0-9]' * 3))

            for cc in colors:
                images = glob(os.path.join(cc, 'images', '*col*.tif'))
                print("Images:", len(images))

                for image in images:
                    try:
                        with tifffile.TiffFile(image) as img:
                            tag = img.pages[0]
                    except Exception:
                        return z
            self.z_layers_checked = z
            con = sqlite3.connect(DB_LOCATION)
            cur = con.cursor()
            res = cur.execute(
                f'UPDATE dataset SET z_layers_checked = {z} WHERE id={self.db_id}')
            con.commit()
            con.close()

    @property
    def composites_dir(self):
        """
        At the time of building composites
        """
        data_location = DATA_LOCATION[WHERE_PROCESSING_HAPPENS['build_composites']]
        raw_data_dir = os.path.join(data_location, self.pi, self.cl_number, self.name)
        return os.path.join(raw_data_dir, 'composites_RSCM_v0.1')

    @property
    def job_dir(self):
        """
        At the time of denoising
        """
        data_location = DATA_LOCATION[WHERE_PROCESSING_HAPPENS['denoise']]
        raw_data_dir = os.path.join(data_location, self.pi, self.cl_number, self.name)
        composites_dir = os.path.join(raw_data_dir, 'composites_RSCM_v0.1')
        job_dirs = [f for f in sorted(glob(os.path.join(composites_dir, 'job_*'))) if os.path.isdir(f)]
        return job_dirs[-1] if len(job_dirs) else None

    @property
    def imaris_file_name(self):
        return f"composites_RSCM_v0.1_job_{self.job_number}.ims"

    @property
    def full_path_to_imaris_file(self):
        """
        At the time of building ims
        """
        data_location = DATA_LOCATION[WHERE_PROCESSING_HAPPENS['build_ims']]
        folder = os.path.join(data_location, self.pi, self.cl_number, self.name, 'composites_RSCM_v0.1', f"job_{self.job_number}")
        return os.path.join(folder, self.imaris_file_name)

    def check_all_raw_composites_present(self):
        expected_composites = self.z_layers_total * self.channels
        print('expected_composites', expected_composites)
        actual_composites = len(glob(os.path.join(self.composites_dir, 'composite*.tif')))
        print('actual_composites', actual_composites)
        return expected_composites == actual_composites

    def check_all_raw_composites_same_size(self):
        files = sorted(glob(os.path.join(self.composites_dir, 'composite*.tif')))
        composite_sizes = [os.path.getsize(x) for x in files]
        return len(set(composite_sizes)) == 1

    def check_all_denoised_composites_present(self):
        expected_composites = self.z_layers_total * self.channels
        actual_composites = len(glob(os.path.join(self.job_dir, 'composite*.tif')))
        return expected_composites == actual_composites

    def check_all_denoised_composites_same_size(self):
        files = sorted(glob(os.path.join(self.job_dir, 'composite*.tif')))
        composite_sizes = [os.path.getsize(x) for x in files]
        return len(set(composite_sizes)) == 1

    def check_denoising_progress(self):
        all_denoised_composites_present = self.check_all_denoised_composites_present()
        all_denoised_composites_same_size = self.check_all_denoised_composites_same_size()
        denoising_finished = all_denoised_composites_present and all_denoised_composites_same_size
        if denoising_finished:
            return True, True
        processing_summary = self.get_processing_summary()
        denoising_summary = processing_summary.get('denoising', {})
        previous_denoised_composites = denoising_summary.get('denoised_composites', 0)
        denoised_composites = len(glob(os.path.join(self.job_dir, 'composite*.tif')))
        denoising_has_progress = denoised_composites > previous_denoised_composites
        if denoising_has_progress:
            self.update_processing_summary({"denoising": {"denoised_composites": denoised_composites}})
        return denoising_finished, denoising_has_progress

    def delete_channel_405(self):
        color = '405'
        rootDir = os.path.join(FASTSTORE_ACQUISITION_FOLDER, self.pi, self.cl_number, self.name)  # build path like this for safety reasons
        assert len(rootDir) > (len(FASTSTORE_ACQUISITION_FOLDER) + 1)  # for safety reasons
        log.info(f"Removing color 405 in folder {rootDir}")
        a = sorted(glob(os.path.join(rootDir, '**', color + '*')))
        log.info("Will remove folders:")
        log.info("\n".join(list(a)))
        # z = [shutil.rmtree(x) for x in a]  # TODO: uncomment once verified

        # update channels number, total and finished ribbons number
        self.channels -= 1
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET channels = {self.channels} WHERE id={self.db_id}')
        con.commit()
        con.close()

        self.ribbons_total = self.z_layers_total * self.channels * self.ribbons_in_z_layer
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET ribbons_total = {self.ribbons_total} WHERE id={self.db_id}')
        con.commit()
        con.close()

        self.ribbons_finished = self.z_layers_total * self.channels * self.ribbons_in_z_layer
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET ribbons_finished = {self.ribbons_finished} WHERE id={self.db_id}')
        con.commit()
        con.close()

    def check_ims_building_progress(self):
        processing_summary = self.get_processing_summary()
        previous_ims_size = processing_summary.get('building_ims', {}).get('ims_size', 0)
        current_ims_size = os.path.getsize(self.full_path_to_imaris_file)
        has_progress = current_ims_size > previous_ims_size
        if has_progress:
            value_from_db = processing_summary.get('building_ims')
            if value_from_db:
                value_from_db.update({'ims_size': current_ims_size})
                self.update_processing_summary({'building_ims': value_from_db})
            else:
                self.update_processing_summary({'building_ims': {'ims_size': current_ims_size}})
        return has_progress

    @property
    def imsqueue_file_name(self):
        return f"job_{self.job_number}.txt.imsqueue"

    def check_ims_converter_works(self):
        currently_building = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', '*.imsqueue'))
        if len(currently_building):
            currently_building = currently_building[0]
        else:
            return False
        with open(currently_building, 'r') as f:
            content = f.readlines()
            if len(content) and len(content[0].split('"')):
                ims_dir = content[0].split('"')[1]
                ims_path = os.path.join(ims_dir, f"composites_RSCM_v0.1_{ims_dir.split(os.path.sep)[-1]}")
                processing_summary = self.get_processing_summary()
                previous_ims_size = processing_summary.get('building_ims', {}).get('other_ims_size', 0)
                current_ims_size = os.path.getsize(ims_path)
                has_progress = current_ims_size != previous_ims_size  # Not just > because other file could have started building
                if has_progress:
                    value_from_db = processing_summary.get('building_ims')
                    if value_from_db:
                        value_from_db.update({'other_ims_size': current_ims_size})
                        self.update_processing_summary({'building_ims': value_from_db})
                    else:
                        self.update_processing_summary({'building_ims': {'other_ims_size': current_ims_size}})
                return has_progress
        return False

    def check_cbpy_works(self):
        currently_denoising = glob(os.path.join(CBPY_FOLDER, 'active', '*.xml*'))
        if len(currently_denoising):
            currently_building = currently_denoising[0]
        else:
            return False
        with open(currently_building, 'r') as f:
            content = f.read()
            if len(content):
                soup = BeautifulSoup(content, "xml")
                root_dir = soup.find('outFilePathUnix').text
                processing_summary = self.get_processing_summary()
                previous_denoised_composites = processing_summary.get('denoising', {}).get('other_dataset_denoised', 0)
                current_denoised_composites = len(glob(os.path.join(root_dir, "composite*.tif")))
                has_progress = current_denoised_composites != previous_denoised_composites  # Not just > because other file could have started building
                if has_progress:
                    value_from_db = processing_summary.get('denoising')
                    if value_from_db:
                        value_from_db.update({'other_dataset_denoised': current_denoised_composites})
                        self.update_processing_summary({'denoising': value_from_db})
                    else:
                        self.update_processing_summary({'denoising': {'other_dataset_denoised': current_denoised_composites}})
                return has_progress
        return False


def check_imaging():
    # Discover all vs_series.dat files in the acquisition directory
    vs_series_files = []
    for root, dirs, files in os.walk(FASTSTORE_ACQUISITION_FOLDER):
        for file in files:
            if file.endswith("vs_series.dat"):
                file_path = Path(os.path.join(root, file))
                if 'stack' in str(file_path.parent.name):
                    vs_series_files.append(str(file_path))
    print("Unique datasets found: ", len(vs_series_files))
    # TODO: files that were deleted, should also be removed from db

    for file_path in vs_series_files:
        print("Working on: ", file_path)
        is_new = check_if_new(file_path)
        if is_new:
            log.info("-----------------------New dataset--------------------------")
            dataset = Dataset.create(file_path)
            log.info(dataset.path_on_fast_store)
            dataset.send_message('imaging_started')
        else:
            dataset = read_dataset_record(file_path)
            if not dataset or dataset.imaging_status == 'finished':
                print("No dataset or Imaging status is 'finished'")
                continue
            elif dataset.imaging_status == 'in_progress':
                print("Imaging status is 'in-progress'")
                got_finished, has_progress, error_flag = dataset.check_imaging_progress()
                if error_flag:
                    dataset.mark_paused()
                    dataset.send_message('broken_tiff_file')
                    continue
                print("Imaging finished:", got_finished)
                if got_finished:
                    dataset.mark_imaging_finished()
                    dataset.send_message('imaging_finished')
                    if dataset.delete_405:
                        print("------------Deleting 405 channel")
                        dataset.delete_channel_405()
                    dataset.start_processing()
                    continue
                print("Imaging has progress:", has_progress)
                if has_progress:
                    if dataset.imaging_no_progress_time:
                        dataset.mark_has_imaging_progress()
                    continue
                else:
                    if not dataset.imaging_no_progress_time:
                        dataset.mark_no_imaging_progress()
                    else:
                        progress_stopped_at = datetime.strptime(dataset.imaging_no_progress_time, DATETIME_FORMAT)
                        if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                            dataset.mark_paused()
                            dataset.send_message('imaging_paused')
            elif dataset.imaging_status == 'paused':
                print("Imaging status is 'paused'")
                finished, has_progress, error_flag = dataset.check_imaging_progress()  # maybe imaging resumed
                if not has_progress:
                    continue
                else:
                    dataset.mark_has_imaging_progress()
                    dataset.mark_resumed()
                    # response = dataset.send_message('imaging_resumed')
                    # print(response)


def check_processing():
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT * FROM dataset WHERE processing_status="not_started" AND imaging_status="finished"'
    ).fetchall()
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        if dataset.check_being_stitched():
            dataset.update_processing_status('started')
            dataset.send_message('processing_started')

    # check if they are on the same stage or moved to the next stage
    # check if it is stuck

    # check stitching
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="started"'
    ).fetchall()
    # print("\nDataset instances where stitching started:")
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        # print("-----", dataset)
        if dataset.check_stitching_complete():
            print("File in complete dir")
            # path_on_hive = os.path.join(HIVE_ACQUISITION_FOLDER, dataset.pi, dataset.cl_number, dataset.name)
            # if os.path.exists(path_on_hive):  # copying started
            if dataset.check_all_raw_composites_present() and dataset.check_all_raw_composites_same_size():
                print("All composites present and same size")
                dataset.update_processing_status('stitched')
            else:
                print("All composites present: ", dataset.check_all_raw_composites_present())
                print("All composites same size: ", dataset.check_all_raw_composites_same_size())
        elif dataset.check_stitching_errored():
            print("File in error dir")
            dataset.update_processing_status('paused')
            dataset.send_message('stitching_error')
        elif dataset.check_being_stitched():
            print("File in processing dir")
            has_progress = dataset.check_stitching_progress()
            if has_progress:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                continue
            else:
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        dataset.send_message('stitching_stuck')
        else:
            print("File in none of ClusterStitchTest dirs")

    # check moving to hive
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="stitched"'
    ).fetchall()
    print("\nDataset instances that have been stitched:")
    # for record in records:
    #     dataset = Dataset.initialize_from_db(record)
    #     all_ribbons_present = dataset.check_all_ribbons_present()
    #     all_raw_composites_present = dataset.check_all_raw_composites_present()
    #     denoising_started = dataset.check_denoising_started()
    #     if all_ribbons_present and all_raw_composites_present and denoising_started and not os.path.exists(
    #         dataset.path_on_fast_store
    #     ):
    #         dataset.update_processing_status('moved_to_hive')
    #     else:
    #         dataset.check_moving_to_hive_progress()
    #     # TODO: check all files are there
    #     # TODO: check folder from FastStore got deleted
    #     # TODO: check denoising started
    #     # TODO: otherwise, check progress
    #     # TODO: Update db to "moved_to_hive" status
    #
    # # check denoising
    # records = cur.execute(
    #     'SELECT * FROM dataset WHERE processing_status="moved_to_hive"'
    # ).fetchall()

    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("-----", dataset)
        if dataset.job_dir:
            print("Job dir is there")
            job_number = re.findall(r"\d+", os.path.basename(dataset.job_dir))[-1]
            dataset.update_job_number(job_number)
            denoising_started = len(glob(os.path.join(dataset.job_dir, "composite*.tif"))) > 0
            print("Denoising started:", denoising_started)
            # print("Job dir", dataset.job_dir)
            if not denoising_started:
                # TODO: check the # of queued files == number of composites ?
                in_queue = len(glob(os.path.join(CBPY_FOLDER, 'queueGPU', f"job_{dataset.job_number}*"))) > 0
                print("In queue:", in_queue)
                if in_queue:
                    if dataset.processing_no_progress_time:
                        dataset.mark_has_processing_progress()
                    # continue
                else:
                    if not dataset.processing_no_progress_time:
                        dataset.mark_no_processing_progress()
                    else:
                        progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                        if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                            dataset.update_processing_status('paused')
                            dataset.send_message('denoising_stuck')
                # check that something else is being denoised and making progress
                cbpy_works = dataset.check_cbpy_works()
                print("CBPY works:", cbpy_works)
                if cbpy_works:
                    if dataset.processing_no_progress_time:
                        dataset.mark_has_processing_progress()
                    continue
                else:
                    if not dataset.processing_no_progress_time:
                        dataset.mark_no_processing_progress()
                    else:
                        progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                        if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                            dataset.update_processing_status('paused')
                            dataset.send_message('denoising_stuck')
                continue

            denoising_finished, denoising_has_progress = dataset.check_denoising_progress()
            print('denoising_finished', denoising_finished)
            print('denoising_has_progress', denoising_has_progress)
            if denoising_finished:
                dataset.update_processing_status('denoised')
                continue
            if denoising_has_progress:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                continue
            else:
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        dataset.send_message('denoising_stuck')

    # check building imaris file
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="denoised"'
    ).fetchall()
    print("Dataset records that have been denoised", records)
    print("\nDataset instances that have been denoised:")
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("-----", dataset)
        if os.path.exists(dataset.full_path_to_imaris_file):
            try:
                # try to open imaris file
                ims_file = ims(dataset.full_path_to_imaris_file)
            except Exception as e:
                log.error("ERROR opening imaris file:", e)
                dataset.send_message("broken_ims_file")
                dataset.update_processing_status('paused')
                continue
            else:
                dataset.update_processing_status('built_ims')
                # TODO: send message that ims file built?
                if not dataset.keep_composites:
                    dataset.clean_up_composites()
        elif os.path.exists(f"{dataset.full_path_to_imaris_file}.part") and os.path.exists(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', dataset.imsqueue_file_name)):
            # Building of ims file in-progress
            ims_has_progress = dataset.check_ims_building_progress()
            if ims_has_progress:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                continue
            else:
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        dataset.send_message('ims_build_stuck')
        else:
            # ims file is not being built
            in_queue = os.path.exists(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', dataset.imsqueue_file_name))
            if in_queue:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                # continue
            else:
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        dataset.send_message('ims_build_stuck')

            # check what other file is being processed, check its size
            ims_converter_works = dataset.check_ims_converter_works()
            if ims_converter_works:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                continue
            else:
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        dataset.send_message('ims_build_stuck')

    # Eventually datasets should be on hive
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="built_ims"'
    ).fetchall()
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        path_on_hive = os.path.join(HIVE_ACQUISITION_FOLDER, dataset.pi, dataset.cl_number, dataset.name)
        if os.path.exists(os.path.join(path_on_hive, 'vs_series.dat')):
            dataset.update_path_on_hive(path_on_hive)
        final_ims_file_path = os.path.join(path_on_hive, 'composites_RSCM_v0.1', f'job_{dataset.job_number}', dataset.imaris_file_name)
        if os.path.exists(final_ims_file_path):
            try:
                ims_file = ims(final_ims_file_path)
            except Exception as e:
                # probably still copying
                # TODO check size?
                log.error(e)
                continue
            else:
                # update db, send msg
                dataset.update_imaris_file_path(final_ims_file_path)
                dataset.update_processing_status('finished')
                dataset.send_message("processing_finished")


class Warning:
    def __init__(self, **kwargs):
        self.type = kwargs.get('type')
        self.active = kwargs.get('active', True)
        self.message_sent = kwargs.get('message_sent', False)
        self.db_id = kwargs.get('db_id')

    @classmethod
    def create(cls, warning_type):
        warning = cls(type=warning_type)
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'INSERT OR IGNORE INTO warning(type) VALUES("{warning_type}")')
        warning.db_id = cur.lastrowid
        con.commit()
        con.close()
        return warning

    @classmethod
    def get_from_db(cls, warning_type):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'SELECT * FROM warning WHERE type = "{warning_type}"').fetchone()
        con.close()
        if not res:
            return
        warning = cls(
            db_id=res[0],
            type=res[1],
            message_sent=res[2],
            active=res[3]
        )
        return warning

    def send_message(self):
        msg_map = {
            'low_space_hive': f":exclamation: *WARNING: Critically low space on Hive (more than {MAX_ALLOWED_STORAGE_PERCENT}% used)*",
            'low_space_faststore': f":exclamation: *WARNING: Critically low space on FastStore (more than {MAX_ALLOWED_STORAGE_PERCENT}% used)*",
            'space_hive_thr0': f":exclamation: *WARNING: Low space on Hive (more than {STORAGE_THRESHOLD_0}% used)*",
            'space_faststore_thr0': f":exclamation: *WARNING: Low space on FastStore (more than {STORAGE_THRESHOLD_0}% used)*",
            'space_hive_thr1': f":exclamation: *WARNING: Low space on Hive (more than {STORAGE_THRESHOLD_1}% used)*",
            'space_faststore_thr1': f":exclamation: *WARNING: Low space on FastStore (more than {STORAGE_THRESHOLD_1}% used)*",
        }
        msg_text = msg_map[self.type]
        payload = {
            "channel": SLACK_CHANNEL_ID,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": msg_text
                    }
                }
            ]
        }
        response = requests.post(SLACK_URL, data=json.dumps(payload), headers=SLACK_HEADERS)
        # update db
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET message_sent = 1 WHERE id={self.db_id}')
        con.commit()
        con.close()
        return response

    def mark_as_active(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET active = 1 WHERE id={self.db_id}')
        con.commit()
        con.close()

    def mark_as_inactive(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET active = 0 WHERE id={self.db_id}')
        con.commit()
        con.close()

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET message_sent = 0 WHERE id={self.db_id}')
        con.commit()
        con.close()


def check_storage():
    def check(used_percent, storage_unit):
        """
        :param used_percent:
        :param storage_unit: "hive" or "faststore"
        :return:
        """
        if used_percent >= MAX_ALLOWED_STORAGE_PERCENT:
            # if active warning exists and message sent - do nothing
            # elif active warning exists and message not sent - send warning msg
            # elif inactive warning exists - make warning active, send warning msg
            # else create new active warning, send warning msg
            warning = Warning.get_from_db(f'low_space_{storage_unit}')
            if warning and warning.active:
                if not warning.message_sent:
                    warning.send_message()
            elif warning and not warning.active:
                warning.mark_as_active()
                warning.send_message()
            else:  # record doesn't exist
                warning = Warning.create(f'low_space_{storage_unit}')
                warning.send_message()
        elif used_percent >= STORAGE_THRESHOLD_1:
            warning = Warning.get_from_db(f'space_{storage_unit}_thr1')
            if warning and warning.active:
                if not warning.message_sent:
                    warning.send_message()
            elif warning and not warning.active:
                warning.mark_as_active()
                warning.send_message()
            else:  # record doesn't exist
                warning = Warning.create(f'space_{storage_unit}_thr1')
                warning.send_message()
            # inactivate more critical warning
            warning = Warning.get_from_db(f'low_space_{storage_unit}')
            if warning and warning.active:
                warning.mark_as_inactive()
        elif used_percent >= STORAGE_THRESHOLD_0:
            warning = Warning.get_from_db(f'space_{storage_unit}_thr0')
            if warning and warning.active:
                if not warning.message_sent:
                    warning.send_message()
            elif warning and not warning.active:
                warning.mark_as_active()
                warning.send_message()
            else:  # record doesn't exist
                warning = Warning.create(f'space_{storage_unit}_thr0')
                warning.send_message()
            # inactivate more critical warning
            warning = Warning.get_from_db(f'space_{storage_unit}_thr1')
            if warning and warning.active:
                warning.mark_as_inactive()
        else:
            warning = Warning.get_from_db(f'space_{storage_unit}_thr0')
            # if active warning exists, make it inactive, make message_sent=False
            if warning and warning.active:
                warning.mark_as_inactive()
            # else do nothing

    cmd = ["df", "-h"]
    ret = subprocess.run(cmd, capture_output=True)
    output = ret.stdout.decode()
    output_rows = output.split('\n')
    beegfs_nodes = [x for x in output_rows if x.startswith('beegfs')]
    hive = [x for x in beegfs_nodes if x.endswith('Hive')][0]
    faststore = [x for x in beegfs_nodes if x.endswith('FastStore')][0]
    faststore_used_percent_str = [x for x in faststore.split() if x.endswith("%")][0]
    hive_used_percent_str = [x for x in hive.split() if x.endswith("%")][0]
    faststore_used_percent = int(faststore_used_percent_str.replace("%", ""))
    hive_used_percent = int(hive_used_percent_str.replace("%", ""))
    check(hive_used_percent, "hive")
    check(faststore_used_percent, "faststore")


def scan():
    try:
        check_storage()
        check_imaging()
        check_processing()
        # TODO db_cleanup()
    except Exception as e:
        print("\n\n!!! EXCEPTION:", e, '\n\n')

    print("========================== Waiting 30 seconds ========================")
    time.sleep(30)


if __name__ == "__main__":
    while True:
        scan()
