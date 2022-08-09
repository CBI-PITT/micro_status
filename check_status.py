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
    imaging_no_progress_time (?)
    processing_no_progress_time (?)

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

-------------------------------------
Messages:
    1) imaging started
    2) imaging paused (crashed?)
    2a) imaging resumed
    3) imaging finished
    4) processing started
    5) processing_paused (crashed?)
    6) processing finished


pip install python-dotenv
"""

import json
import os
import re
import requests
import sqlite3
from datetime import datetime
from glob import glob
from pathlib import Path

from bs4 import BeautifulSoup
from dotenv import load_dotenv

# con = sqlite3.connect('/CBI_Hive/CBI/Iana/projects/internal/RSCM_datasets')
# cur = con.cursor()


FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire"
HIVE_ACQUISITION_FOLDER = "/CBI_Hive/Acquire"
DB_LOCATION = "/CBI_Hive/CBI/Iana/projects/internal/RSCM_datasets"

SLACK_URL = "https://slack.com/api/chat.postMessage"
load_dotenv()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL")
SLACK_HEADERS = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {os.getenv("SLACK_TOKEN")}'}
PROGRESS_TIMEOUT = 600  # seconds
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"


def check_if_new(file_path):
    con = sqlite3.connect(DB_LOCATION)
    con.row_factory = lambda cursor, row: row[0]
    cur = con.cursor()
    vs_series_file_records = cur.execute('SELECT path FROM vsseriesfile').fetchall()
    con.close()
    return file_path not in vs_series_file_records


def create_dataset_record(file_path):
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    res = cur.execute(f'INSERT OR IGNORE INTO vsseriesfile(path) VALUES("{file_path}")')
    vs_series_file_id = cur.lastrowid
    con.commit()
    con.close()

    file_path = Path(file_path)
    path_parts = file_path.parts
    pi_name = path_parts[3] if path_parts[3].isalpha() else None
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

    dataset = Dataset(
        db_id = dataset_id,
        path_on_fast_store = file_path,
        pi = pi_name,
        cl_number = cl_number,
        name = dataset_name,
        imaging_status = "in_progress",
        processing_status = "not_started",
        channels = channels,
        z_layers_total = z_layers,
        ribbons_total = ribbons_total,
        z_layers_current = z_layers - 1,
        ribbons_finished = 0,
        imaging_no_progress_time = None,
        processing_no_progress_time = None
    )
    return dataset


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

    cl_number_id = record[4]
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    cl_number = cur.execute(f'SELECT name FROM clnumber WHERE id="{cl_number_id}"').fetchone()
    con.close()

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
        processing_no_progress_time = record[17]
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
        self.ribbons_total = kwargs.get('ribbons_total')
        self.ribbons_finished = kwargs.get('ribbons_finished')
        self.imaging_no_progress_time = kwargs.get('imaging_no_progress_time')
        self.processing_no_progress_time = kwargs.get('processing_no_progress_time')

    def check_imaging_progress(self):
        print("--------------------in check imaging progress")
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
        return ribbons_finished > ribbons_finished_prev


    def check_imaging_finished(self):
        # TODO: this check can be made in the above fn
        print("--------------------Checking whether imaging finished")
        file_path = Path(self.path_on_fast_store)
        ribbons_finished = 0
        subdirs = sorted(glob(os.path.join(file_path.parent, '*')), reverse=True)
        subdirs = [x for x in subdirs if os.path.isdir(x) and 'layer' in x]
        if self.z_layers_total > 1000:
            len4 = lambda x: len(re.findall(r"\d+", os.path.basename(x))[-1]) == 4
            len3 = lambda x: len(re.findall(r"\d+", os.path.basename(x))[-1]) == 3
            subdirs_0 = filter(len4, subdirs)
            subdirs_1 = filter(len3, subdirs)
            subdirs = list(subdirs_0) + list(subdirs_1)

        for subdir in subdirs:
            color_dirs = [x.path for x in os.scandir(subdir) if x.is_dir()]
            for color_dir in color_dirs:
                images_dir = os.path.join(color_dir, 'images')
                ribbons = len(os.listdir(images_dir))
                ribbons_finished += ribbons
                if ribbons < self.ribbons_in_z_layer:
                    break
        return ribbons_finished == self.ribbons_total


    def send_message(self, msg_type):
        print("---------------------In send message")
        msg_map = {
            'imaging_started': "Imaging of {} {} {} *_started_*",
            'imaging_finished': "Imaging of {} {} {} *_finished_*",
            'imaging_paused': "**WARNING:** Imaging of {} {} {} *_paused_* at z-layer {}",
            'imaging_resumed': "Imaging of {} {} {} *_resumed_*",
        }
        if msg_type == 'imaging_paused':
            msg_text = msg_map['imaging_paused'].format(self.pi, self.cl_number, self.name, self.z_layers_current)
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
        res = cur.execute(f'UPDATE dataset SET imaging_status = "paused" WHERE id={self.db_id}')
        con.commit()
        con.close()

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_no_progress_time = "{progress_stopped_at}" WHERE id={self.db_id}')
        con.commit()
        con.close()

        self.imaging_status = "paused"
        self.imaging_no_progress_time = progress_stopped_at

    def mark_has_imaging_progress(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_status = "in_progress" WHERE id={self.db_id}')
        con.commit()
        con.close()

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET imaging_no_progress_time = null WHERE id={self.db_id}')
        con.commit()
        con.close()

        self.imaging_status = "in_progress"
        self.imaging_no_progress_time = None

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
        is_new = check_if_new(file_path)
        if is_new:
            print("-----------------------New dataset--------------------------")
            dataset = create_dataset_record(file_path)
            response = dataset.send_message('imaging_started')
            print(response)
        else:
            dataset = read_dataset_record(file_path)
            if not dataset or dataset.imaging_status == 'finished':
                print("----------------No dataset or Imaging status is 'finished'")
                continue
            elif dataset.imaging_status == 'in_progress':
                print("--------------Imaging status is 'in-progress'")
                got_finished = dataset.check_imaging_finished()
                print("Imaging finished:", got_finished)
                if got_finished:
                    dataset.mark_imaging_finished()
                    response = dataset.send_message('imaging_finished')
                    print(response)
                    continue
                has_progress = dataset.check_imaging_progress()
                print("Imaging has progress:", has_progress)
                if has_progress:
                    continue
                else:
                    if not dataset.imaging_no_progress_time:
                        dataset.mark_no_imaging_progress()
                    else:
                        progress_stopped_at = datetime.strptime(dataset.imaging_no_progress_time, DATETIME_FORMAT)
                        if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                            response = dataset.send_message('imaging_paused')
                            print(response)
            elif dataset.imaging_status == 'paused':
                print("----------------Imaging status is 'paused'")
                has_progress = dataset.check_imaging_progress()  # maybe imaging resumed
                if not has_progress:
                    continue
                else:
                    dataset.mark_has_imaging_progress()
                    response = dataset.send_message('imaging_resumed')
                    print(response)


def check_processing():
    # get all datasets, with processing other than finished
    # check if they are on the same stage or moved to the next stage
    # check if it is stuck
    pass


def scan():
    check_imaging()
    check_processing()


if __name__ == "__main__":
    while True:
        scan()