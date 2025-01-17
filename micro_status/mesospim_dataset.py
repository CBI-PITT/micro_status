import json
import logging
import os
import pickle
import re
import subprocess
import sqlite3
import sys
from datetime import datetime
from glob import glob

from .dataset import Dataset
from .settings import *

log = logging.getLogger(__name__)


class MesoSPIMDataset(Dataset):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tiles_total = None
        if os.path.exists(self.path_on_fast_store):
            self.path = self.path_on_fast_store
        elif self.path_on_hive and os.path.exists(self.path_on_hive):
            self.path = self.path_on_hive
        else:
            self.path = self.path_on_fast_store.replace('/CBI_FastStore', '/h20')
        # self.z_layers = kwargs.get('z_layers')
        metadata_file = sorted(glob(os.path.join(self.path, '*.btf_meta.txt')))[0]
        f = open(metadata_file, 'r')
        lines = f.readlines()
        xy = [l for l in lines if "[Pixelsize in um]" in l][0]
        xy_res = re.findall(r"\d+", xy)[0]
        self.resolution_xy = int(xy_res)
        z = [l for l in lines if "[z_stepsize]" in l][0]
        z_res = re.findall(r"\d+\.\d+", z)[0]
        self.resolution_z = int(float(z_res))
        self.settings_bin_file = None
        bin_files = sorted(glob(os.path.join(self.path, "*.bin")))
        if len(bin_files):
            self.settings_bin_file = bin_files[0]
            self.channels = self.get_total_MesoSPIM_colors()
            self.tiles_total = self.get_total_MesoSPIM_tiles()

    def _specific_setup(self, **kwargs):
        if self.settings_bin_file:
            # update database record
            con = sqlite3.connect(DB_LOCATION)
            cur = con.cursor()
            res = cur.execute(
                f'UPDATE dataset SET channels = "{self.channels}", modality = "mesospim" WHERE id={self.db_id}'
            )
            con.commit()
            con.close()

    def check_imaging_progress(self):
        if self.tiles_total:
            print("self.tiles_total", self.tiles_total)
            files = sorted(glob(os.path.join(self.path_on_fast_store, "*.btf")))
            tiles_imaged = len(files)
            print('tiles_imaged', tiles_imaged)
            tile_sizes = [os.path.getsize(x) for x in files]
            print("tile_sizes", tile_sizes)
            if tiles_imaged == self.tiles_total:
                if len(set(tile_sizes)) == 1:  # imaging finished
                    self.mark_imaging_finished()
                    self.send_message('imaging_finished')
                    self.start_processing()
                    self.update_processing_status('in_progress')
                    self.send_message('processing_started')
            else:
                print("Imaging still in progress")
                tiles_imaged_prev = self.tiles_finished
                print("self.tiles_finished", self.tiles_finished)
                smallest_file_size = min(tile_sizes)
                print("smallest_file_size", smallest_file_size)
                if tiles_imaged != tiles_imaged_prev:  # has progress
                    self.update_db_field('tiles_finished', tiles_imaged)
                    self.tiles_finished = tiles_imaged
                else:
                    imaging_summary = json.loads(self.imaging_summary) if self.imaging_summary else {}
                    smallest_file_size_prev = imaging_summary.get('smallest_file_size', 0)
                    print("smallest_file_size_prev", smallest_file_size_prev)
                    if smallest_file_size_prev != smallest_file_size:  # has progress
                        imaging_summary['smallest_file_size'] = smallest_file_size
                        imaging_summary_str = json.dumps(imaging_summary)
                        # self.update_db_field('imaging_summary', imaging_summary_str)
                        con = sqlite3.connect(DB_LOCATION)
                        cur = con.cursor()
                        res = cur.execute(f"UPDATE dataset SET imaging_summary = '{imaging_summary_str}' WHERE id={self.db_id}")
                        con.commit()
                        con.close()
                        self.imaging_summary = imaging_summary_str
                    else:  # has no progress
                        if self.imaging_no_progress_time:
                            progress_stopped_at = datetime.strptime(self.imaging_no_progress_time, DATETIME_FORMAT)
                            if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                                # self.mark_paused()
                                # self.send_message('imaging_paused')
                                print("CHECK IMAGING! May be paused")
                        else:
                            self.mark_no_imaging_progress()

    def get_total_MesoSPIM_tiles(self):
        # print("Counting tiles")
        if self.settings_bin_file:
            sys.path.append('/h20/CBI/Iana/src/mesoSPIM-control')
            sys.path.append('/h20/home/iana/.conda/envs/mesospim/lib/python3.12/site-packages')
            f = open(self.settings_bin_file, 'rb')
            acquisition_list = pickle.load(f)
            total_btf_files = len(acquisition_list)
            return total_btf_files

    def get_total_MesoSPIM_colors(self):
        # print("Counting color channels")
        if self.settings_bin_file:
            sys.path.append('/h20/CBI/Iana/src/mesoSPIM-control')
            sys.path.append('/h20/home/iana/.conda/envs/mesospim/lib/python3.12/site-packages')
            f = open(self.settings_bin_file, 'rb')
            acquisition_list = pickle.load(f)
            lasers = [x['laser'] for x in acquisition_list]
            total_colors = len(set(lasers))
            return total_colors

    def start_processing(self):
        """
        /CBI_FastStore/cbiPythonTools/mesospim_utils/mesospim_utils/rl.py convert-ims-dir-mesospim-tiles <path_on_fast_store> --res 5 1 1
        """
        cmd = [
            '/CBI_FastStore/cbiPythonTools/mesospim_utils/mesospim_utils/rl.py',
            'convert-ims-dir-mesospim-tiles',
            self.path,
            '--res',
            str(self.resolution_z),
            str(self.resolution_xy),
            str(self.resolution_xy)
        ]
        print("COMMAND TO CONVERT TO IMS", cmd)
        subprocess.run(cmd)
