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
    processing_status (not_started, started, stitched, copied_to_hive, denoised, built_ims, finished)
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


FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire"
HIVE_ACQUISITION_FOLDER = "/CBI_Hive/Acquire"
DB_LOCATION = "/CBI_Hive/CBI/Iana/projects/internal/RSCM_datasets"

SLACK_URL = "https://slack.com/api/chat.postMessage"
load_dotenv()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL")
SLACK_HEADERS = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {os.getenv("SLACK_TOKEN")}'}
PROGRESS_TIMEOUT = 600  # seconds
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
RSCM_FOLDER_STITCHING = "/CBI_FastStore/clusterStitchTEST"
DASK_DASHBOARD = os.getenv("DASK_DASHBOARD")
CHROME_DRIVER_PATH = '/CBI_Hive/CBI/Iana/projects/internal/micro_status/chromedriver'
MAX_ALLOWED_STORAGE_PERCENT = 94
STORAGE_THRESHOLD_0 = 85
STORAGE_THRESHOLD_1 = 90
CHECKING_TIFFS_ENABLED = True


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
        z_layers_checked = record[19]
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
        self.channels = kwargs.get('channels')
        self.z_layers_total = kwargs.get('z_layers_total')
        self.z_layers_current = kwargs.get('z_layers_current')
        self.z_layers_checked = kwargs.get('z_layers_checked')
        self.ribbons_total = kwargs.get('ribbons_total')
        self.ribbons_finished = kwargs.get('ribbons_finished')
        self.imaging_no_progress_time = kwargs.get('imaging_no_progress_time')
        self.processing_no_progress_time = kwargs.get('processing_no_progress_time')
        self.z_layers_checked = kwargs.get('z_layers_checked')

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
            processing_no_progress_time=None
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

        if CHECKING_TIFFS_ENABLED:
            print("self.z_layers_checked", self.z_layers_checked)
            if finished:  # only check layer 0 (last layer)
                z_start = 0
                z_stop = -1
                time.sleep(10)  # wait, in case if last image is still being saved
            else:
                z_start = int((self.z_layers_checked - 1) or self.z_layers_total)
                z_stop = int(z_layers_current)

            bad_layer = self.check_tiffs(z_start, z_stop)
            if bad_layer is not None:
                error_flag = True
                self.z_layers_current = bad_layer

        return finished, has_progress, error_flag

    def send_message(self, msg_type):
        print("---------------------In send message------------------------")
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
            'broken_tiff_file': "*WARNING: Broken tiff file in {} {} {} z-layer {}*"
        }
        if msg_type in ['imaging_paused', 'broken_tiff_file']:
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name, self.z_layers_current)
        elif msg_type == 'processing_finished':
            ims_folder = str(PureWindowsPath(str(Path(self.imaris_file_path).parent).replace('/CBI_Hive', 'H:')))
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name, ims_folder)
        else:
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name)
        print("Message text", msg_text)
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
        contents = f'rootDir="{str(dat_file_path.parent)}"'
        with open(txt_file_path, "w") as f:
            f.write(contents)
        print("-----------------------Created text file : ---------------------")
        print(contents)

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
            channels = record[11],
            z_layers_total = record[12],
            z_layers_current = record[13],
            ribbons_total = record[14],
            ribbons_finished = record[15],
            imaging_no_progress_time = record[16],
            processing_no_progress_time = record[17],
            z_layers_checked = record[19]
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
            print("-----------------------New dataset--------------------------")
            dataset = Dataset.create(file_path)
            dataset.send_message('imaging_started')
            # print(response)
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
                    response = dataset.send_message('imaging_finished')
                    print(response)
                    dataset.start_processing()
                    continue
                print("Imaging has progress:", has_progress)
                if has_progress:
                    if dataset.imaging_no_progress_time:
                        dataset.mark_has_imaging_progress()
                    continue
                else:  # TODO else is redundant
                    if not dataset.imaging_no_progress_time:
                        dataset.mark_no_imaging_progress()
                    else:
                        progress_stopped_at = datetime.strptime(dataset.imaging_no_progress_time, DATETIME_FORMAT)
                        if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                            dataset.mark_paused()
                            response = dataset.send_message('imaging_paused')
                            print(response)
            elif dataset.imaging_status == 'paused':
                print("Imaging status is 'paused'")
                has_progress = dataset.check_imaging_progress()  # maybe imaging resumed
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
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        if dataset.check_stitching_complete():
            path_on_hive = os.path.join(HIVE_ACQUISITION_FOLDER, dataset.pi, dataset.cl_number, dataset.name)
            if os.path.exists(path_on_hive):  # copying started
                dataset.update_processing_status('stitched')
        elif dataset.check_stitching_errored():
            dataset.update_processing_status('paused')
            dataset.send_message('stitching_error')
        elif dataset.check_being_stitched():
            has_progress = dataset.check_stitching_progress()
            if has_progress:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                continue
            else:  # TODO else is redundant
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        dataset.send_message('stitching_stuck')

    # check after stitching
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="stitched"'
    ).fetchall()
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        path_on_hive = os.path.join(HIVE_ACQUISITION_FOLDER, dataset.pi, dataset.cl_number, dataset.name)
        # check if path on hive exists (smth already copied) -> update db record (processing status, path on hive)
        if os.path.exists(os.path.join(path_on_hive, 'vs_series.dat')):
            dataset.update_path_on_hive(path_on_hive)
            # dataset.update_processing_status('stitched')
            composites_dir = os.path.join(path_on_hive, 'composites_RSCM_v0.1')
            # check if job folder exists -> update db
            job_dir = sorted(glob(os.path.join(composites_dir, 'job_*')))
            if len(job_dir):
                job_dir = job_dir[-1]
                job_number = re.findall(r"\d+", os.path.basename(job_dir))[-1]
                dataset.update_job_number(job_number)
                # check if final ims file exists
                ims_file_path = os.path.join(job_dir, f'composites_RSCM_v0.1_job_{job_number}.ims')
                if os.path.exists(ims_file_path):
                    try:
                        # try to open imaris file
                        ims_file = ims(ims_file_path)
                    except Exception as e:
                        print("ERROR opening imaris file:", e)
                        dataset.send_message("broken_ims_file")
                        dataset.update_processing_status('paused')
                        continue
                    # update db, send msg
                    dataset.update_imaris_file_path(ims_file_path)
                    dataset.update_processing_status('finished')
                    dataset.send_message("processing_finished")
                # check if ims file .part exists
                elif os.path.exists(ims_file_path + '.part'):
                    # update db
                    # dataset.update_processing_status('denoised')
                    pass


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
    except Exception as e:
        print("\n\n!!! EXCEPTION:", e, '\n\n')

    print("========================== Waiting 30 seconds ========================")
    time.sleep(30)


if __name__ == "__main__":
    while True:
        scan()
