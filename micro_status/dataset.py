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
        self.path_on_hive = record[7]
        self.job_number = record[8]
        self.imaris_file_path = record[9]
        self.channels = record[10]
        self.z_layers_total = record[11]
        self.z_layers_current = record[12]
        self.z_layers_checked = record[24]
        self.ribbons_total = record[13]
        self.ribbons_finished = record[14]
        self.tiles_total = record[15]
        self.tiles_finished = record[16]
        self.tiles_x = record[17]
        self.tiles_y = record[18]
        self.resolution_xy = record[19]
        self.resolution_z = record[20]
        self.imaging_no_progress_time = record[21]
        self.processing_no_progress_time = record[22]
        self.processing_summray = record[23]
        self.keep_composites = record[25]
        self.delete_405 = record[26]
        self.created = datetime.strptime(record[27], DATETIME_FORMAT)
        self.modality = record[28]
        self.is_brain = record[29]
        self.peace_json_created = record[30]
        self.imaging_summary = record[31]
        self.moved = record[32]
        self.moving = record[33]
        self.paused = record[34]

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
        raise NotImplementedError("Subclasses must implement this method")

    def update_db_field(self, field_name, field_value):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET {field_name} = "{field_value}" WHERE id={self.db_id}')
        con.commit()
        con.close()

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
            'peace_json_created': "Created analysis task for brain dataset {} {} {}",
            'moved': "Dataset {} {} {} has been moved to h20"
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
        res = cur.execute(f'UPDATE dataset SET imaging_status = "paused", paused = 1 WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.imaging_status = "paused"
        self.paused = True

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
            db_id=record[0],
            name=record[1],
            path_on_fast_store=record[2],
            cl_number=cl_number,
            pi=pi_name,
            imaging_status=record[5],
            processing_status=record[6],
            path_on_hive=record[7],
            job_number=record[8],
            imaris_file_path=record[9],
            channels=record[10],
            z_layers_total=record[11],
            z_layers_current=record[12],
            ribbons_total=record[13],
            ribbons_finished=record[14],
            tiles_total=record[15],
            tiles_finished=record[16],
            tiles_x=record[17],
            tiles_y=record[18],
            resolution_xy=record[19],
            resolution_z=record[20],
            imaging_no_progress_time=record[21],
            processing_no_progress_time=record[22],
            processing_summray=record[23],
            z_layers_checked=record[24],
            keep_composites=record[25],
            delete_405=record[26],
            created=datetime.strptime(record[27], DATETIME_FORMAT),
            modality=record[28],
            is_brain=record[29],
            peace_json_created=record[30],
            imaging_summary=record[31],
            moved=record[32],
            moving=record[33],
            paused=record[34]
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


    # @property
    # def imsqueue_file_name(self):
    #     return f"job_{self.job_number}.txt.imsqueue"

    # def check_ims_converter_works(self):
    #     currently_building = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', '*.imsqueue'))
    #     if len(currently_building):
    #         currently_building = currently_building[0]
    #     else:
    #         return False
    #     with open(currently_building, 'r') as f:
    #         content = f.readlines()
    #         if len(content) and len(content[0].split('"')):
    #             ims_dir = content[0].split('"')[1]
    #             ims_path = os.path.join(ims_dir, f"composites_RSCM_v0.1_{ims_dir.split(os.path.sep)[-1]}.ims.part")
    #             processing_summary = self.get_processing_summary()
    #             previous_ims_size = processing_summary.get('building_ims', {}).get('other_ims_size', 0)
    #             current_ims_size = os.path.getsize(ims_path)
    #             has_progress = current_ims_size != previous_ims_size  # Not just > because other file could have started building
    #             if has_progress:
    #                 value_from_db = processing_summary.get('building_ims')
    #                 if value_from_db:
    #                     value_from_db.update({'other_ims_size': current_ims_size})
    #                     self.update_processing_summary({'building_ims': value_from_db})
    #                 else:
    #                     self.update_processing_summary({'building_ims': {'other_ims_size': current_ims_size}})
    #             return has_progress
    #     return False

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


    # def check_finalization_progress(self):
    #     pass

    def start_moving(self):
        """create txt file in the RSCM queue stitch directory
        file name: {dataset_id}_{pi_name}_{cl_number}_{dataset_name}_move.txt
        this way the earlier datasets go in first
        """
        print("Starting to move")
        dat_file_path = Path(self.path_on_fast_store)
        # txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', self.rscm_move_txt_file_name)
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'tempQueue', self.rscm_move_txt_file_name)
        contents = f'rootDir="{str(dat_file_path)}"\nIMS=False\ndenoise=False\nmoveOnly=True'
        with open(txt_file_path, "w") as f:
            f.write(contents)
        log.info("-----------------------Queue moving to Hive. Text file : ---------------------")
        log.info(contents)
        self.moving = True
        self.update_db_field('moving', 1)

    # @property
    # def in_imaris_queue(self):
    #     return len(glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', f"*{self.job_number}*.txt.imsqueue"))) > 0

    # def requeue_ims(self):
    #     if self.full_path_to_imaris_file.startswith('/CBI_FastStore'):
    #         trash_location = FASTSTORE_TRASH_LOCATION
    #     else:
    #         trash_location = HIVE_TRASH_LOCATION
    #     trash_folder_ims = os.path.join(trash_location, self.pi, self.cl_number, self.name, f"ims_{datetime.now().strftime(DATETIME_FORMAT)}")
    #     os.makedirs(trash_folder_ims)
    #
    #     # check whether it is .ims.part file or .ims file
    #     if os.path.exists(self.full_path_to_imaris_file):
    #         file_to_delete = self.full_path_to_imaris_file
    #     elif os.path.exists(self.full_path_to_ims_part_file):
    #         file_to_delete = self.full_path_to_ims_part_file
    #
    #     # move broken imaris file to trash folder
    #     shutil.move(file_to_delete, os.path.join(trash_folder_ims, os.path.basename(file_to_delete)))
    #
    #     # move .imsqueue file to queue
    #     if self.in_imaris_queue:
    #         return
    #
    #     complete_imsqueue_files = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'complete', f"*{self.job_number}*.txt.imsqueue"))
    #     error_imsqueue_files = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'error', f"*{self.job_number}*.txt.imsqueue"))
    #     processing_imsqueue_files = glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', f"*{self.job_number}*.txt.imsqueue"))
    #
    #     if len(complete_imsqueue_files) > 0:  # file is in the 'complete' folder by mistake
    #         imsqueue_file_to_move = complete_imsqueue_files[0]
    #     elif len(error_imsqueue_files) > 0:  # file is in the error folder
    #         imsqueue_file_to_move = error_imsqueue_files[0]
    #     elif len(processing_imsqueue_files) > 0:  # file is in the processing folder
    #         imsqueue_file_to_move = processing_imsqueue_files[0]
    #
    #     imsqueue_destination = os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', os.path.basename(imsqueue_file_to_move))
    #     log.info(f"Moving {imsqueue_file_to_move} to {imsqueue_destination}")
    #     shutil.move(imsqueue_file_to_move, imsqueue_destination)

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

    @property
    def rscm_move_txt_file_name(self):
        return f"{str(self.db_id).zfill(5)}_{self.pi}_{self.cl_number}_{self.name}_move.txt"

    def check_if_moved(self):
        moved = os.path.exists(os.path.join(RSCM_FOLDER_STITCHING, 'complete', self.rscm_move_txt_file_name))
        if moved:
            self.update_db_field('moved', 1)
            self.moved = True
            self.update_db_field('moving', 0)
            self.moving = False
            path_on_hive = self.path_on_fast_store.replace(FASTSTORE_ACQUISITION_FOLDER, HIVE_ACQUISITION_FOLDER)
            if os.path.exists(path_on_hive):
                self.update_db_field('path_on_hive', path_on_hive)
                self.path_on_hive = path_on_hive
                self.send_message('moved')


class Found(BaseException):
    pass
