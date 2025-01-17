import logging
import os
import re
import sqlite3
import time
from glob import glob
from pathlib import Path

from bs4 import BeautifulSoup

from .dataset import Dataset
from .settings import *

log = logging.getLogger(__name__)


class RSCMDataset(Dataset):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _specific_setup(self, **kwargs):
        print("In specific setup")

        with open(os.path.join(self.path_on_fast_store, 'vs_series.dat'), 'r') as f:
            data = f.read()

        soup = BeautifulSoup(data, "xml")
        z_layers = int(soup.find('stack_slice_count').text)
        ribbons_in_z_layer = int(soup.find('grid_cols').text)

        ribbons_finished = 0
        file_path = Path(self.path_on_fast_store)
        subdirs = os.scandir(file_path)
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
        # current_z_layer = re.findall(r"\d+", subdir.name)[-1]
        ribbons_total = z_layers * channels * ribbons_in_z_layer

        # update database record
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET z_layers_total = "{z_layers}", ribbons_total = "{ribbons_total}", z_layers_current = "{z_layers - 1}", ribbons_finished = 0, modality = "rscm" WHERE id={self.db_id}'
        )
        con.commit()
        con.close()

        # update dataset instance
        self.z_layers_total = z_layers
        self.ribbons_total = ribbons_total
        self.z_layers_current = z_layers - 1
        self.ribbons_finished = 0

    def check_imaging_progress(self):
        error_flag = False
        file_path = Path(self.path_on_fast_store)
        ribbons_finished = 0  # TODO: optimize, start with current z layer, not mrom 0
        subdirs = sorted(glob(os.path.join(file_path, '*')), reverse=True)
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

    def build_imaris_file(self):
        import subprocess
        print(" !!!!!!!!!!!!!!!!!!!! Starting imaris build command !!!!!!!!!!!!!!!!!!!")
        cmd = [
            '/h20/home/lab/miniconda3/envs/make_ims/bin/python',
            '/h20/home/lab/scripts/makeIMS_slurm_wine.py',
            self.job_dir,
            'true'
        ]
        print(cmd)
        subprocess.run(cmd)

    @property
    def ribbons_in_z_layer(self):
        # TODO: fails here if file was removed
        with open(os.path.join(self.path_on_fast_store, 'vs_series.dat'), 'r') as f:
            data = f.read()
        soup = BeautifulSoup(data, "xml")
        return int(soup.find('grid_cols').text)


class Found(BaseException):
    pass
