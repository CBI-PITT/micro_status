import json
import logging
import os
import re
import requests
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime
from glob import glob
from pathlib import Path, PureWindowsPath

import tifffile
from bs4 import BeautifulSoup
# from dotenv import load_dotenv
from imaris_ims_file_reader import ims

from micro_status.settings import *

log = logging.getLogger(__name__)


class Dataset:
    def __init__(self, path_on_fast_store, **kwargs):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        record = cur.execute(f'SELECT * FROM dataset WHERE path_on_fast_store="{str(path_on_fast_store)}"').fetchone()
        con.close()

        pi_id = record[4]
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        pi_name = cur.execute(f'SELECT name FROM pi WHERE id="{pi_id}"').fetchone()
        con.close()
        if pi_name:
            pi_name = pi_name[0]

        cl_number_id = record[3]
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        cl_number = cur.execute(f'SELECT name FROM clnumber WHERE id="{cl_number_id}"').fetchone()
        con.close()
        if cl_number:
            cl_number = cl_number[0]

        self.db_id = record[0]
        self.name = record[1]
        self.path_on_fast_store = path_on_fast_store
        self.cl_number = cl_number
        self.pi = pi_name

        self.imaging_status = record[5]
        self.processing_status = record[6]
        # self.path_on_hive = kwargs.get('path_on_hive')
        # self.job_number = kwargs.get('job_number')
        # self.imaris_file_path = kwargs.get('imaris_file_path')
        self.channels = record[10]
        # self.z_layers_total = kwargs.get('z_layers_total')
        # self.z_layers_current = kwargs.get('z_layers_current')
        # self.z_layers_checked = kwargs.get('z_layers_checked')
        # self.ribbons_total = kwargs.get('ribbons_total')
        # self.ribbons_finished = kwargs.get('ribbons_finished')
        self.imaging_no_progress_time = record[21]
        self.processing_no_progress_time = record[22]
        # self.keep_composites = kwargs.get('keep_composites')
        # self.delete_405 = kwargs.get('delete_405')
        # self.is_brain = kwargs.get('is_brain', False)
        # self.peace_json_created = kwargs.get('peace_json_created', False)

    def __str__(self):
        return f"{self.db_id} {self.pi} {self.cl_number} {self.name}"

    @classmethod
    def create(cls, file_path):
        file_path = Path(file_path)   # TODO remove RSCM_FASTSTORE_ACQUISITION_FOLDER from the path
        path_parts = file_path.parts
        last_name_pattern = r"^[A-Za-z '-_]+$"
        pi_name = path_parts[4] if re.findall(last_name_pattern, path_parts[4]) else None  # TODO make it more general
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

        is_brain_dataset = 0
        if pi_name.lower() in BRAIN_DATA_PRODUCERS:
            is_brain_dataset = 1

        cl_number = [x for x in path_parts if 'CL' in x.upper()]
        cl_number = '00CL00' if len(cl_number) == 0 else cl_number[0]
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

        dataset_name = path_parts[-1]

        print("Path", file_path, "pi_name", pi_name, "cl_number", cl_number, "dataset_name", dataset_name)

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'''INSERT OR IGNORE INTO dataset(name, path_on_fast_store, cl_number, pi, 
        imaging_status, processing_status, created)
        VALUES("{dataset_name}", "{file_path}", "{cl_number_id}", "{pi_id}", 
        "in_progress", "not_started", "{datetime.now().strftime(DATETIME_FORMAT)}")
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
            analysis_status="not_started",
            # channels=channels,
            imaris_file_path=None,
            imaging_no_progress_time=None,
            processing_no_progress_time=None,
            # is_brain=is_brain_dataset,
            peace_json_created=None
        )
        dataset._specific_setup()
        return dataset

    def _specific_setup(self, **kwargs):
        raise NotImplementedError("Subclasses must implement this method")

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
            'processing_finished': "Processing of {} {} {} finished",
            'broken_ims_file': "*WARNING: Broken Imaris file at {} {} {}. Requeuing.*",
            'stitching_error': "*WARNING: Stitching error {} {} {}. Txt file in error folder.*",
            'stitching_stuck': "*WARNING: Stitching of {} {} {} could be stuck. Check cluster.*",
            'denoising_stuck': "*WARNING: Denoising of {} {} {} could be stuck. Check CBPy.*",
            'ims_build_stuck': "*WARNING: Building of Imaris file for {} {} {} seems to be stuck.*",
            'broken_tiff_file': "*WARNING: Broken tiff file in {} {} {} z-layer {}*",
            'built_ims': "Imaris file built for {} {} {}. Check it out at {}",
            'ignoring_demo_dataset': "Ignoring demo dataset {} {} {}",
            'requeue_ims': "Requeuing ims build task for {} {} {}",
            'peace_json_created': "Created analysis task for brain dataset {} {} {}"
        }
        if msg_type in ['imaging_paused', 'broken_tiff_file']:
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name, self.z_layers_current)
        elif msg_type == 'built_ims':
            imaris_file_path = self.full_path_to_imaris_file
            # ims_folder = str(PureWindowsPath(str(Path(imaris_file_path).parent).replace('/CBI_Hive', 'H:')))
            ims_folder = str(PureWindowsPath(str(Path(imaris_file_path).parent).replace('/h20', 'H:').replace('/CBI_FastStore', 'Z:')))
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name, ims_folder)
        else:
            msg_text = msg_map[msg_type].format(self.pi, self.cl_number, self.name)
        log.info(f"Message text: {msg_text}")
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
        if MESSAGES_ENABLED:  # doing this check here to be able to save message to logs
            response = requests.post(SLACK_URL, data=json.dumps(payload), headers=SLACK_HEADERS)
        return True

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

    @property
    def rscm_move_txt_file_name(self):
        return f"{str(self.db_id).zfill(5)}_{self.pi}_{self.cl_number}_{self.name}_move.txt"

    def start_processing(self):
        """create txt file in the RSCM queue stitch directory
        file name: {dataset_id}_{pi_name}_{cl_number}_{dataset_name}.txt
        this way the earlier datasets go in first
        """
        dat_file_path = Path(self.path_on_fast_store)
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', self.rscm_txt_file_name)
        contents = f'rootDir="{str(dat_file_path.parent)}"\nkeepComposites=True\nmoveToHive=False'
        with open(txt_file_path, "w") as f:
            f.write(contents)
        log.info("-----------------------Queue processing. Text file : ---------------------")
        log.info(contents)

    def clean_up_raw_composites(self):
        log.info("---------------------Cleaning up raw composites--------------------")
        log.info(f"composites_dir: {self.composites_dir}")
        if not self.composites_dir:
            return
        raw_composites = sorted(glob(os.path.join(self.composites_dir, 'composite_*.tif')))
        log.info(f"raw_composites: {len(raw_composites)}")
        if self.full_path_to_imaris_file.startswith('/CBI_FastStore'):
            trash_location = FASTSTORE_TRASH_LOCATION
        else:
            trash_location = HIVE_TRASH_LOCATION
        trash_folder_raw = os.path.join(trash_location, self.pi, self.cl_number, self.name, "raw_composites")
        if not os.path.exists(trash_folder_raw):
            os.makedirs(trash_folder_raw)
        for f in raw_composites:
            log.info(f"move to trash: {f}")
            trash_path = os.path.join(trash_folder_raw, os.path.basename(f))
            shutil.move(f, trash_path)

    def clean_up_denoised_composites(self):
        log.info("---------------------Cleaning up denoised composites--------------------")
        log.info(f"job_dir: {self.job_dir}")
        if not self.job_dir:
            return
        denoised_composites = sorted(glob(os.path.join(self.job_dir, 'composite_*.tif')))
        log.info(f"denoised_composites: {len(denoised_composites)}")
        if self.full_path_to_imaris_file.startswith('/CBI_FastStore'):
            trash_location = FASTSTORE_TRASH_LOCATION
        else:
            trash_location = HIVE_TRASH_LOCATION
        trash_folder_denoised = os.path.join(trash_location, self.pi, self.cl_number, self.name, "denoised_composites")
        if not os.path.exists(trash_folder_denoised):
            os.makedirs(trash_folder_denoised)
        for f in denoised_composites:
            log.info(f"move to trash: {f}")
            trash_path = os.path.join(trash_folder_denoised, os.path.basename(f))
            shutil.move(f, trash_path)

    @classmethod
    def initialize_from_db(cls, record):
        pi_id = record[4]
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        pi_name = cur.execute(f'SELECT name FROM pi WHERE id="{pi_id}"').fetchone()
        con.close()
        if pi_name:
            pi_name = pi_name[0]

        cl_number_id = record[3]
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
            delete_405 = record[21],
            is_brain=record[22],
            peace_json_created=record[23]
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
        composites_dir = os.path.join(raw_data_dir, 'composites_RSCM_v0.1')
        if os.path.exists(composites_dir):
            return composites_dir
        if data_location == RSCM_FASTSTORE_ACQUISITION_FOLDER:
            data_location = HIVE_ACQUISITION_FOLDER
        else:
            data_location = RSCM_FASTSTORE_ACQUISITION_FOLDER
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
        if len(job_dirs):
            final_job_dir = job_dirs[-1]
        else: # TODO: rewrite this terrible piece
            if data_location == RSCM_FASTSTORE_ACQUISITION_FOLDER:
                data_location = HIVE_ACQUISITION_FOLDER
            else:
                data_location = RSCM_FASTSTORE_ACQUISITION_FOLDER
            raw_data_dir = os.path.join(data_location, self.pi, self.cl_number, self.name)
            composites_dir = os.path.join(raw_data_dir, 'composites_RSCM_v0.1')
            job_dirs = [f for f in sorted(glob(os.path.join(composites_dir, 'job_*'))) if os.path.isdir(f)]
            final_job_dir = job_dirs[-1] if len(job_dirs) else None
        if final_job_dir:
            job_number = re.findall(r"\d+", os.path.basename(final_job_dir))[-1]
            self.update_job_number(job_number)
        return final_job_dir

    @property
    def imaris_file_name(self):
        return f"composites_RSCM_v0.1_job_{self.job_number}.ims"

    @property
    def full_path_to_imaris_file(self):
        """
        At the time of building ims
        """
        data_location = DATA_LOCATION[WHERE_PROCESSING_HAPPENS['build_ims']]
        composites_dir = os.path.join(data_location, self.pi, self.cl_number, self.name, 'composites_RSCM_v0.1')
        job_folder = os.path.join(composites_dir, f"job_{self.job_number}")
        if not os.path.exists(job_folder):
            # if folder doesn't exist, try to find other job folders
            job_dirs = [f for f in sorted(glob(os.path.join(composites_dir, 'job_*'))) if os.path.isdir(f)]
            job_folder = job_dirs[-1] if len(job_dirs) else composites_dir
        ims_path = os.path.join(job_folder, self.imaris_file_name)
        if os.path.exists(ims_path):
            return ims_path
        if data_location == RSCM_FASTSTORE_ACQUISITION_FOLDER:  # TODO: rewrite this terrible piece
            data_location = HIVE_ACQUISITION_FOLDER
        else:
            data_location = RSCM_FASTSTORE_ACQUISITION_FOLDER
        composites_dir = os.path.join(data_location, self.pi, self.cl_number, self.name, 'composites_RSCM_v0.1')
        job_folder = os.path.join(composites_dir, f"job_{self.job_number}")
        if not os.path.exists(job_folder):
            # if folder doesn't exist, try to find other job folders
            job_dirs = [f for f in sorted(glob(os.path.join(composites_dir, 'job_*'))) if os.path.isdir(f)]
            job_folder = job_dirs[-1] if len(job_dirs) else composites_dir
        return os.path.join(job_folder, self.imaris_file_name)

    @property
    def full_path_to_ims_part_file(self):
        data_location = DATA_LOCATION[WHERE_PROCESSING_HAPPENS['build_ims']]
        composites_dir = os.path.join(data_location, self.pi, self.cl_number, self.name, 'composites_RSCM_v0.1')
        job_folder = os.path.join(composites_dir, f"job_{self.job_number}")
        if not os.path.exists(job_folder):
            # if folder doesn't exist, try to find other job folders
            job_dirs = [f for f in sorted(glob(os.path.join(composites_dir, 'job_*'))) if os.path.isdir(f)]
            job_folder = job_dirs[-1] if len(job_dirs) else composites_dir
        ims_part_path = os.path.join(job_folder, f"{self.imaris_file_name}.part")
        if os.path.exists(ims_part_path):
            return ims_part_path
        if data_location == RSCM_FASTSTORE_ACQUISITION_FOLDER:  # TODO: rewrite this terrible piece
            data_location = HIVE_ACQUISITION_FOLDER
        else:
            data_location = RSCM_FASTSTORE_ACQUISITION_FOLDER
        composites_dir = os.path.join(data_location, self.pi, self.cl_number, self.name, 'composites_RSCM_v0.1')
        job_folder = os.path.join(composites_dir, f"job_{self.job_number}")
        if not os.path.exists(job_folder):
            # if folder doesn't exist, try to find other job folders
            job_dirs = [f for f in sorted(glob(os.path.join(composites_dir, 'job_*'))) if os.path.isdir(f)]
            job_folder = job_dirs[-1] if len(job_dirs) else composites_dir
        return os.path.join(job_folder, f"{self.imaris_file_name}.part")

    def check_all_raw_composites_present(self):
        expected_composites = self.z_layers_total * self.channels
        print('expected raw composites', expected_composites)
        actual_composites = len(glob(os.path.join(self.composites_dir, 'composite*.tif')))
        print('actual raw composites', actual_composites)
        return expected_composites == actual_composites

    def check_all_raw_composites_same_size(self):
        files = sorted(glob(os.path.join(self.composites_dir, 'composite*.tif')))
        composite_sizes = [os.path.getsize(x) for x in files]
        return len(set(composite_sizes)) == 1

    def check_all_denoised_composites_present(self):
        if not self.job_dir:
            return False
        expected_composites = self.z_layers_total * self.channels
        print('expected denoised composites', expected_composites)
        actual_composites = len(glob(os.path.join(self.job_dir, 'composite*.tif')))
        print('actual denoised composites', actual_composites)
        return expected_composites == actual_composites

    def check_all_denoised_composites_same_size(self):
        files = sorted(glob(os.path.join(self.job_dir, 'composite*.tif')))
        composite_sizes = [os.path.getsize(x) for x in files]
        return len(set(composite_sizes)) == 1

    def check_denoising_finished(self):
        all_denoised_composites_present = self.check_all_denoised_composites_present()
        all_denoised_composites_same_size = self.check_all_denoised_composites_same_size()
        return all_denoised_composites_present and all_denoised_composites_same_size

    def check_denoising_progress(self):
        processing_summary = self.get_processing_summary()
        denoising_summary = processing_summary.get('denoising', {})
        previous_denoised_composites = denoising_summary.get('denoised_composites', 0)
        denoised_composites = len(glob(os.path.join(self.job_dir, 'composite*.tif')))
        denoising_has_progress = denoised_composites > previous_denoised_composites
        if denoising_has_progress:
            self.update_processing_summary({"denoising": {"denoised_composites": denoised_composites}})
        return denoising_has_progress

    def delete_channel_405(self):
        color = '405'
        rootDir = os.path.join(RSCM_FASTSTORE_ACQUISITION_FOLDER, self.pi, self.cl_number, self.name)  # build path like this for safety reasons
        assert len(rootDir) > (len(RSCM_FASTSTORE_ACQUISITION_FOLDER) + 1)  # for safety reasons
        log.info(f"Removing color 405 in folder {rootDir}")
        a = sorted(glob(os.path.join(rootDir, '**', color + '*')))
        log.info("Will remove folders:")
        log.info("\n".join(list(a)))
        z = [shutil.rmtree(x) for x in a]

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
        print("in check_ims_building_progress")
        processing_summary = self.get_processing_summary()
        previous_ims_size = processing_summary.get('building_ims', {}).get('ims_size', 0)
        partial_ims_file = self.full_path_to_ims_part_file
        print("partial_ims_file", partial_ims_file)
        if not os.path.exists(partial_ims_file):
            print("Partial ims file doesn't exist")
            has_progress = False
            current_ims_size = 0
        else:
            print("Partial ims file exists")
            current_ims_size = os.path.getsize(partial_ims_file)
            has_progress = current_ims_size != previous_ims_size  # the file building could start over
            print("current_ims_size != previous_ims_size", has_progress)
        if has_progress:
            value_from_db = processing_summary.get('building_ims')
            if value_from_db:
                value_from_db.update({'ims_size': current_ims_size})
                self.update_processing_summary({'building_ims': value_from_db})
            else:
                self.update_processing_summary({'building_ims': {'ims_size': current_ims_size}})
        else:
            has_progress = self.in_imaris_queue and self.check_ims_converter_works()
            print("has_progress", has_progress)
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
                ims_path = os.path.join(ims_dir, f"composites_RSCM_v0.1_{ims_dir.split(os.path.sep)[-1]}.ims.part")
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

    def check_imaris_file_built(self):
        file_opens = False
        file_exists = os.path.exists(self.full_path_to_imaris_file)
        if file_exists:
            try:
                # try to open imaris file
                ims_file = ims(self.full_path_to_imaris_file)
                file_opens = True
            except:
                file_opens = False
        return file_exists and file_opens

    def guess_processing_status(self):
        status = "started"
        if self.check_all_raw_composites_present() and self.check_all_raw_composites_same_size():
            status = "stitched"
        # else:
        #     return status
        if self.check_all_denoised_composites_present() and self.check_all_denoised_composites_same_size():
            status = "denoised"
        # else:
        #     return status
        if self.check_imaris_file_built():
            status = "built_ims"
        return status

    def check_finalization_progress(self):
        pass

    def start_moving(self):
        """create txt file in the RSCM queue stitch directory
        file name: {dataset_id}_{pi_name}_{cl_number}_{dataset_name}_move.txt
        this way the earlier datasets go in first
        """
        dat_file_path = Path(self.path_on_fast_store)
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', self.rscm_move_txt_file_name)
        contents = f'rootDir="{str(dat_file_path.parent)}"\nIMS=False\ndenoise=False\nmoveOnly=True'
        with open(txt_file_path, "w") as f:
            f.write(contents)
        log.info("-----------------------Queue moving to Hive. Text file : ---------------------")
        log.info(contents)

    @property
    def in_imaris_queue(self):
        return len(glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', f"*{self.job_number}*.txt.imsqueue"))) > 0

    def requeue_ims(self):
        if self.full_path_to_imaris_file.startswith('/CBI_FastStore'):
            trash_location = FASTSTORE_TRASH_LOCATION
        else:
            trash_location = HIVE_TRASH_LOCATION
        trash_folder_ims = os.path.join(trash_location, self.pi, self.cl_number, self.name, f"ims_{datetime.now().strftime(DATETIME_FORMAT)}")
        os.makedirs(trash_folder_ims)

        # check whether it is .ims.part file or .ims file
        if os.path.exists(self.full_path_to_imaris_file):
            file_to_delete = self.full_path_to_imaris_file
        elif os.path.exists(self.full_path_to_ims_part_file):
            file_to_delete = self.full_path_to_ims_part_file

        # move broken imaris file to trash folder
        shutil.move(file_to_delete, os.path.join(trash_folder_ims, os.path.basename(file_to_delete)))

        # move .imsqueue file to queue
        if self.in_imaris_queue:
            return

        complete_imsqueue_files = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'complete', f"*{self.job_number}*.txt.imsqueue"))
        error_imsqueue_files = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'error', f"*{self.job_number}*.txt.imsqueue"))
        processing_imsqueue_files = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', f"*{self.job_number}*.txt.imsqueue"))

        if len(complete_imsqueue_files) > 0:  # file is in the 'complete' folder by mistake
            imsqueue_file_to_move = complete_imsqueue_files[0]
        elif len(error_imsqueue_files) > 0:  # file is in the error folder
            imsqueue_file_to_move = error_imsqueue_files[0]
        elif len(processing_imsqueue_files) > 0:  # file is in the processing folder
            imsqueue_file_to_move = processing_imsqueue_files[0]

        imsqueue_destination = os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', os.path.basename(imsqueue_file_to_move))
        log.info(f"Moving {imsqueue_file_to_move} to {imsqueue_destination}")
        shutil.move(imsqueue_file_to_move, imsqueue_destination)

    def create_peace_json(self):
        print("Creating PEACE JSON")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        comp_name = "deneb"
        json_path = os.path.join(PEACE_JSON_FOLDER, f'{comp_name}_settings{timestamp}.json')
        actions = ['extract_tiff_series', 'detect_cells']

        current_path = Path(self.full_path_to_imaris_file)
        for level in range(len(current_path.parents) - 1):
            current_path = current_path.parent
            presumable_out_folder = current_path / 'analysis'
            if os.path.exists(str(presumable_out_folder)):
                break
        else:
            presumable_out_folder = os.path.join(HIVE_ACQUISITION_FOLDER, self.pi, self.cl_number, 'analysis')
        print("presumable_out_folder", presumable_out_folder)
        settings = {
            'ims_file': str(self.full_path_to_imaris_file),
            'actions': actions,
            'output_folder': str(presumable_out_folder),
        }
        with open(json_path, 'w') as f:
            f.write(json.dumps(settings))
        print("Created JSON")
        json_created_time = datetime.now().strftime(DATETIME_FORMAT)
        self.peace_json_created = json_created_time
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET peace_json_created = "{json_created_time}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.send_message('peace_json_created')


class Found(BaseException):
    pass


class RSCMDataset(Dataset):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        # self.db_id = kwargs.get('db_id')
        # self.name = kwargs.get('name')
        # self.path_on_fast_store = kwargs.get('path_on_fast_store')
        # self.cl_number = kwargs.get('cl_number')
        # self.pi = kwargs.get('pi')
        # self.imaging_status = kwargs.get('imaging_status')
        # self.processing_status = kwargs.get('processing_status')
        # self.path_on_hive = kwargs.get('path_on_hive')
        self.job_number = kwargs.get('job_number')
        # self.imaris_file_path = kwargs.get('imaris_file_path')
        # self.channels = kwargs.get('channels')
        self.z_layers_total = kwargs.get('z_layers_total')
        self.z_layers_current = kwargs.get('z_layers_current')
        self.z_layers_checked = kwargs.get('z_layers_checked')
        self.ribbons_total = kwargs.get('ribbons_total')
        self.ribbons_finished = kwargs.get('ribbons_finished')
        # self.imaging_no_progress_time = kwargs.get('imaging_no_progress_time')
        # self.processing_no_progress_time = kwargs.get('processing_no_progress_time')
        self.keep_composites = kwargs.get('keep_composites')
        self.delete_405 = kwargs.get('delete_405')
        # self.is_brain = kwargs.get('is_brain')
        # self.peace_json_created = kwargs.get('peace_json_created')

    def _specific_setup(self, **kwargs):
        print("In specific setup")

        with open(os.path.join(self.path_on_fast_store, 'vs_series.dat'), 'r') as f:
            data = f.read()

        soup = BeautifulSoup(data, "xml")
        z_layers = int(soup.find('stack_slice_count').text)
        ribbons_in_z_layer = int(soup.find('grid_cols').text)

        ribbons_finished = 0
        file_path = Path(self.path_on_fast_store)
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
        ribbons_total = z_layers * channels * ribbons_in_z_layer

        # update database record
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET z_layers_total = "{z_layers}", ribbons_total = "{ribbons_total}", z_layers_current = "{z_layers - 1}", ribbons_finished = 0 WHERE id={dataset.db_id}'
        )
        con.commit()
        con.close()

        # update dataset instance
        self.z_layers_total = z_layers
        self.ribbons_total = ribbons_total
        self.z_layers_current = z_layers - 1
        self.ribbons_finished = 0


class MesoSPIMDataset(Dataset):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.tiles_total = kwargs.get('tiles_total')
        # self.z_layers = kwargs.get('z_layers')
        metadata_file = sorted(glob(os.path.join(self.path_on_fast_store, '*.btf_meta.txt')))[0]
        f = open(metadata_file, 'r')
        lines = f.readlines()
        xy = [l for l in lines if "[Pixelsize in um]" in l][0]
        xy_res = re.findall(r"\d+", xy)[0]
        self.resolution_xy = int(xy_res)
        z = [l for l in lines if "[z_stepsize]" in l][0]
        z_res = re.findall(r"\d+\.\d+", z)[0]
        self.resolution_z = int(float(z_res))

    def _specific_setup(self, **kwargs):
        def get_total_MesoSPIM_colors(settings_bin_file):
            import sys
            sys.path.append('/h20/CBI/Iana/src/mesoSPIM-control')
            sys.path.append('/h20/home/iana/.conda/envs/mesospim/lib/python3.12/site-packages')
            import pickle
            f = open(settings_bin_file, 'rb')
            acquisition_list = pickle.load(f)
            lasers = [x['laser'] for x in acquisition_list]
            total_colors = len(set(lasers))
            return total_colors

        settings_bin_file = sorted(glob(os.path.join(self.path_on_fast_store, "*.bin")))
        if len(settings_bin_file):
            settings_bin_file = settings_bin_file[0]
            self.channels = get_total_MesoSPIM_colors(settings_bin_file)
            # update database record
            con = sqlite3.connect(DB_LOCATION)
            cur = con.cursor()
            res = cur.execute(
                f'UPDATE dataset SET channels = "{self.channels}" WHERE id={self.db_id}'
            )
            con.commit()
            con.close()

    def start_processing(self):
        """
        /CBI_FastStore/cbiPythonTools/mesospim_utils/mesospim_utils/rl.py convert-ims-dir-mesospim-tiles <path_on_fast_store> --res 5 1 1
        """
        cmd = [
            '/CBI_FastStore/cbiPythonTools/mesospim_utils/mesospim_utils/rl.py',
            'convert-ims-dir-mesospim-tiles',
            self.path_on_fast_store,
            '--res',
            str(self.resolution_z),
            str(self.resolution_xy),
            str(self.resolution_xy)
        ]
        print("COMMAND TO CONVERT TO IMS", cmd)
        subprocess.run(cmd)

