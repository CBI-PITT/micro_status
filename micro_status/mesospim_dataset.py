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

log = logging.getLogger(__name__)


class MesoSPIMDataset(Dataset):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tiles_total = None
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
        self.settings_bin_file = None
        bin_files = sorted(glob(os.path.join(self.path_on_fast_store, "*.bin")))
        if len(bin_files):
            self.settings_bin_file = bin_files[0]

    def _specific_setup(self, **kwargs):
        def get_total_MesoSPIM_colors(settings_bin_file):
            sys.path.append('/h20/CBI/Iana/src/mesoSPIM-control')
            sys.path.append('/h20/home/iana/.conda/envs/mesospim/lib/python3.12/site-packages')
            f = open(settings_bin_file, 'rb')
            acquisition_list = pickle.load(f)
            lasers = [x['laser'] for x in acquisition_list]
            total_colors = len(set(lasers))
            return total_colors

        if self.settings_bin_file:
            self.channels = get_total_MesoSPIM_colors(settings_bin_file)
            self.tiles_total = self.get_total_MesoSPIM_tiles()
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
            files = sorted(glob(os.path.join(file_path, "*.btf")))
            tiles_imaged = len(files)
            tile_sizes = [os.path.getsize(x) for x in files]
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
                smallest_file_size = min(tile_sizes)
                if tiles_imaged != tiles_imaged_prev:  # has progress
                    self.update_db_field('tiles_finished', tiles_imaged)
                    self.tiles_finished = tiles_imaged
                else:
                    imaging_summary = json.loads(self.imaging_summary) if self.imaging_summary else {}
                    smallest_file_size_prev = imaging_summary.get('smallest_file_size', 0)
                    if smallest_file_size_prev != smallest_file_size:  # has progress
                        imaging_summary['smallest_file_size'] = smallest_file_size
                        imaging_summary_str = json.dumps(imaging_summary)
                        self.update_db_field('imaging_summary', imaging_summary_str)
                        self.imaging_summary = imaging_summary_str
                    else:  # has no progress
                        if self.imaging_no_progress_time:
                            progress_stopped_at = datetime.strptime(self.imaging_no_progress_time, DATETIME_FORMAT)
                            if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                                dataset.mark_paused()
                                dataset.send_message('imaging_paused')
                        else:
                            self.mark_no_imaging_progress()

    def get_total_MesoSPIM_tiles(self):
        if self.settings_bin_file:
            sys.path.append('/h20/CBI/Iana/src/mesoSPIM-control')
            sys.path.append('/h20/home/iana/.conda/envs/mesospim/lib/python3.12/site-packages')
            f = open(self.settings_bin_file, 'rb')
            acquisition_list = pickle.load(f)
            total_btf_files = len(acquisition_list)
            return total_btf_files

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
