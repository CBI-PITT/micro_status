import logging
import os
import re
import shutil
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

    def start_processing(self):
        """create txt file in the RSCM queue stitch directory
        file name: {dataset_id}_{pi_name}_{cl_number}_{dataset_name}.txt
        this way the earlier datasets go in first
        """
        file_path = Path(self.path_on_fast_store)
        txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', self.rscm_txt_file_name)
        # txt_file_path = os.path.join(RSCM_FOLDER_STITCHING, 'tempQueue', self.rscm_txt_file_name)
        contents = f'rootDir="{str(file_path)}"\nkeepComposites=True\nmoveToHive=False'
        with open(txt_file_path, "w") as f:
            f.write(contents)
        log.info("-----------------------Queue processing. Text file : ---------------------")
        log.info(contents)

    def build_imaris_file(self):
        import subprocess
        print("Starting imaris build command")
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

    @property
    def rscm_txt_file_name(self):
        return f"{str(self.db_id).zfill(5)}_{self.pi}_{self.cl_number}_{self.name}.txt"

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

    def update_job_number(self, job_number):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE dataset SET job_number = "{job_number}" WHERE id={self.db_id}')
        con.commit()
        con.close()
        self.job_number = job_number

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
        # else:
        #     has_progress = self.in_imaris_queue and self.check_ims_converter_works()
        #     print("has_progress", has_progress)
        return has_progress

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
            status = "finished"
        return status




class Found(BaseException):
    pass
