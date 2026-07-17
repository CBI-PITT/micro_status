import json
import logging
import os
import re
import subprocess
import sqlite3
from datetime import datetime
from glob import glob

from .dataset import Dataset
from .settings import *
from .utils import backup_zarr_v2_metadata_to_zip

log = logging.getLogger(__name__)


class MesoSPIMDataset(Dataset):
    file_type = "btf"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.refractive_index = None
        if os.path.exists(self.path_on_fast_store):
            self.path = self.path_on_fast_store
        elif self.path_on_hive and os.path.exists(self.path_on_hive):
            self.path = self.path_on_hive
        else:
            self.path = self.path_on_fast_store.replace('/CBI_FastStore', '/h20')

        metadata_files = self.metadata_files
        if metadata_files:
            with open(metadata_files[0], 'r') as f:
                lines = f.readlines()

            xy = [l for l in lines if "[Pixelsize in um]" in l]
            if xy:
                xy_res = re.findall(r"\d+(?:\.\d+)?", xy[0])[0]
                self.resolution_xy = int(float(xy_res))

            z = [l for l in lines if "[z_stepsize]" in l]
            if z:
                z_res = re.findall(r"-?\d+(?:\.\d+)?", z[0])[0]
                self.resolution_z = int(float(z_res))

            ri = [l for l in lines if "[ETL CFG File]" in l]
            if ri:
                ri_value = re.findall(r"_?RI_?([0-9]*\.[0-9]+)_?", ri[0])
                if ri_value:
                    self.refractive_index = float(ri_value[0])

        channels = self.get_total_MesoSPIM_colors_from_file_list()
        if channels:
            self.channels = channels

        tiles_total = self.get_total_MesoSPIM_tiles()
        if tiles_total:
            self.tiles_total = tiles_total

    def _specific_setup(self, **kwargs):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(
            f'UPDATE dataset SET modality = "mesospim" WHERE id={self.db_id}'
        )
        con.commit()
        con.close()

        if self.channels or self.tiles_total:
            # update database record
            con = sqlite3.connect(DB_LOCATION)
            cur = con.cursor()
            res = cur.execute(
                f'UPDATE dataset SET channels = "{self.channels}", tiles_total = "{self.tiles_total}" WHERE id={self.db_id}'
            )
            con.commit()
            con.close()
            # log.info(f"Channels: {self.channels}, Tiles: {self.tiles_total}")
        # log.info(f"Refractive index {self.refractive_index}")
        # log.info(f"Resolution (XY) {self.resolution_xy}, resolution (Z) {self.resolution_z}")

    def check_imaging_progress(self):
        if self.tiles_total:
            files = self.tile_files
            tiles_imaged = len(files)
            tile_sizes = [os.path.getsize(x) for x in files]
            if tiles_imaged >= self.tiles_total:  # all tiles are there
                if len(set(tile_sizes)) == 1:  # all tiles are the same size -> imaging finished
                    self.mark_imaging_finished()
                    log.info(f"Updated imaging status to finished for {self.path_on_fast_store}")
                    self.send_message('imaging_finished')
                    self.start_processing()
                    self.update_processing_status('in_progress')
                    log.info(f"Updated processing status to in_progress for {self.path_on_fast_store}")
                    self.send_message('processing_started')
                    self.stitch_maxips()
            else:  # not all tiles are there
                tiles_imaged_prev = self.tiles_finished
                smallest_file_size = min(tile_sizes)
                if tiles_imaged != tiles_imaged_prev:  # number of tiles has changed -> has progress
                    self.update_db_field('tiles_finished', tiles_imaged)
                    self.tiles_finished = tiles_imaged
                    self.mark_imaging_resumed()
                    log.info(f"Updated imaging status to in_progress for {self.path_on_fast_store}")
                else:  # number of tiles hasn't changed
                    imaging_summary = json.loads(self.imaging_summary) if self.imaging_summary else {}
                    smallest_file_size_prev = imaging_summary.get('smallest_file_size', 0)
                    if smallest_file_size_prev != smallest_file_size:  # smallest file has changed -> has progress
                        imaging_summary['smallest_file_size'] = smallest_file_size
                        imaging_summary_str = json.dumps(imaging_summary)
                        # self.update_db_field('imaging_summary', imaging_summary_str)
                        con = sqlite3.connect(DB_LOCATION)
                        cur = con.cursor()
                        res = cur.execute(f"UPDATE dataset SET imaging_summary = '{imaging_summary_str}' WHERE id={self.db_id}")
                        con.commit()
                        con.close()
                        self.imaging_summary = imaging_summary_str
                        self.mark_imaging_resumed()
                        log.info(f"Updated imaging status to in_progress for {self.path_on_fast_store}")
                    else:  # has no progress
                        if self.imaging_no_progress_time:  # already had no progress during the last check
                            progress_stopped_at = datetime.strptime(self.imaging_no_progress_time, DATETIME_FORMAT)
                            if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                                if self.imaging_status != "needs_attention":
                                    self.mark_imaging_paused()
                                    log.info(f"Updated imaging status to paused for {self.path_on_fast_store}")
                                    self.send_message('imaging_paused')
                        else:
                            self.mark_no_imaging_progress()

    @property
    def tile_name_pattern(self):
        return r'_Tile(\d+)_Ch([0-9]+[a-zA-Z]?)_'

    @property
    def tile_files(self):
        if not os.path.exists(self.path_on_fast_store):
            return []

        tile_entries = []
        for entry_name in os.listdir(self.path_on_fast_store):
            entry_path = os.path.join(self.path_on_fast_store, entry_name)
            if not re.search(self.tile_name_pattern, entry_name):
                continue
            if self.file_type == "btf" and os.path.isfile(entry_path) and entry_name.endswith('.btf'):
                tile_entries.append(entry_path)
        return sorted(tile_entries)

    @property
    def metadata_files(self):
        metadata_patterns = [
            os.path.join(self.path_on_fast_store, '*_meta.txt'),
            os.path.join(os.path.dirname(self.path_on_fast_store), f'{self.name}_*_meta.txt'),
        ]

        metadata_files = []
        for pattern in metadata_patterns:
            metadata_files.extend(glob(pattern))

        metadata_files = [x for x in metadata_files if re.search(self.tile_name_pattern, os.path.basename(x))]
        return sorted(set(metadata_files))

    def get_total_MesoSPIM_tiles(self):
        return len(self.tile_files) or None

    def get_total_MesoSPIM_colors_from_file_list(self):
        channel_matches = []
        for tile_file in self.tile_files:
            match = re.search(self.tile_name_pattern, os.path.basename(tile_file))
            if match:
                channel_matches.append(match.group(2))
        return len(set(channel_matches)) or None

    def start_processing(self):
        """
        /CBI_FastStore/cbiPythonTools/mesospim_utils/mesospim_utils/rl.py convert-ims-dir-mesospim-tiles <path_on_fast_store> --res 5 1 1
        """
        # cmd = [
        #     '/CBI_FastStore/cbiPythonTools/mesospim_utils/mesospim_utils/rl.py',
        #     'convert-ims-dir-mesospim-tiles',
        #     self.path,
        #     '--res',
        #     str(self.resolution_z),
        #     str(self.resolution_xy),
        #     str(self.resolution_xy)
        # ]
        log.info(f"Queuing processing for {self.path_on_fast_store}")
        # cmd = [
        #     '/h20/home/lab/miniconda3/envs/mesospim_utils/bin/python',
        #     '/h20/home/lab/src/mesospim_utils/mesospim_utils/automated.py',
        #     'automated-method-slurm',
        #     self.path if ' ' not in self.path else f'"{self.path}"'
        # ]
        cmd = [
            '/h20/home/lab/miniconda3/envs/mesospim_dev/bin/python',
            '/h20/home/lab/src/mesospim_utils_v0.1/mesospim_utils/automated.py',
            # '/h20/home/lab/src/mesospim_utils_zarr/mesospim_utils/mesospim_utils/automated.py',
            'automated-method-slurm',
            self.path if ' ' not in self.path else f'"{self.path}"',
            '--final-file-type', 'ims'
        ]
        subprocess.run(cmd)

    def check_tile_ims_files(self):
        from imaris_ims_file_reader import ims
        all_good = True
        ims_files = sorted(glob(os.path.join(self.path_on_fast_store, 'ims_files', '*Tile*_Ch*_Sh*.ims')))
        for ims_file in ims_files:
            try:
                # try to open imaris file
                ims_file_obj = ims(ims_file)
            except Exception as e:
                log.error(f"ERROR opening imaris file: {e}")
                self.send_message("broken_ims_file")
                all_good = False
        return all_good

    def check_auto_stitch(self):
        if self.refractive_index:
            imaris_folder = os.path.join(self.path_on_fast_store, 'decon', 'ims_files')
        else:
            imaris_folder = os.path.join(self.path_on_fast_store, 'ims_files')
        json_files = glob(os.path.join(imaris_folder, '*.json'))
        if len(json_files):
            stitching_json_file_name = os.path.basename(json_files[0])
            stitching_json_file_error = os.path.join(MESOSPIM_AUTO_STITCH_FOLDER, 'error', stitching_json_file_name)
            if os.path.exists(stitching_json_file_error):
                processing_summary = self.get_processing_summary()
                value_from_db = processing_summary.get('stitching', {})
                if value_from_db:
                    already_in_error_folder = value_from_db.get('already_in_error_folder')
                    if not already_in_error_folder:
                        self.send_message('stitching_error')
                        value_from_db.update({'stitching': {'already_in_error_folder': True}})
                        self.update_processing_summary(value_from_db)
                else:
                    self.send_message('stitching_error')
                    value_from_db.update({'stitching': {'already_in_error_folder': True}})
                    self.update_processing_summary(value_from_db)
            else:
                processing_summary = self.get_processing_summary()
                value_from_db = processing_summary.get('stitching')
                if value_from_db:
                    already_in_error_folder = value_from_db.get('already_in_error_folder')
                    if already_in_error_folder:
                        value_from_db.update({'stitching': {'already_in_error_folder': False}})
                        self.update_processing_summary(value_from_db)

    @property
    def full_path_to_imaris_file(self):
        ims_file = None
        if self.refractive_index:
            imaris_folder = os.path.join(self.path_on_fast_store, 'decon', 'ims_files')
        else:
            imaris_folder = os.path.join(self.path_on_fast_store, 'ims_files')
        candidates = glob(os.path.join(imaris_folder, "*.ontage.ims"))
        if len(candidates):
            ims_file = candidates[0]
        return ims_file

    @property
    def grid_size(self):
        rows = None
        columns = None

        metadata_files = self.metadata_files
        if len(metadata_files):
            first_channel = re.findall(r"_Ch([0-9]+[a-zA-Z]?)_", os.path.basename(metadata_files[0]))
            if len(first_channel):
                first_channel = first_channel[0]
                first_channel_metadata_files = [x for x in metadata_files if f'_Ch{first_channel}_' in os.path.basename(x)]
                x_positions = []
                y_positions = []
                for file in first_channel_metadata_files:
                    with open(file, 'r') as f:
                        lines = f.readlines()
                    x = [l for l in lines if "[x_pos]" in l]
                    y = [l for l in lines if "[y_pos]" in l]
                    if not x or not y:
                        continue
                    x_pos = re.findall(r"-?\d+(?:\.\d+)?", x[0])[0]
                    y_pos = re.findall(r"-?\d+(?:\.\d+)?", y[0])[0]
                    x_positions.append(float(x_pos))
                    y_positions.append(float(y_pos))
                # Extract unique x_pos and y_pos values
                x_positions = set(x_positions)
                y_positions = set(y_positions)
                # Calculate the grid size
                rows = len(y_positions)
                columns = len(x_positions)
        return rows, columns

    def stitch_maxips(self):
        from pathlib import Path
        rows, cols = self.grid_size
        script_folder = Path(__file__).parent
        cmd = [
            '/h20/home/lab/miniconda3/envs/peace/bin/python',
            str(script_folder / 'validate_tiles.py'),
            f'{self.path}/MAX_*.ome.zarr.tif' if self.file_type == "ome.zarr" else f'{self.path}/MAX_*.btf.tiff',
            '--rows', str(rows),
            '--cols', str(cols),
            '--order', 'col-major',
            '--stage-direction-y', '1',
            '--stage-direction-x', '1',
            '--outdir', f'{self.path}/stitched_maxips'
        ]
        import subprocess
        subprocess.run(cmd)




class MesoSPIMZarrDataset(MesoSPIMDataset):
    file_type = "ome.zarr"

    def _specific_setup(self, **kwargs):
        super()._specific_setup(**kwargs)

    def check_imaging_progress(self):
        xml_file_pattern = "*.ome.zarr.xml"
        xml_files = sorted(glob(os.path.join(self.path_on_fast_store, xml_file_pattern)))
        if len(xml_files):
            self.mark_imaging_finished()
            log.info(f"Updated imaging status to finished for {self.path_on_fast_store}")
            self.send_message('imaging_finished')
            backup_zarr_v2_metadata_to_zip(xml_files[0].replace('.xml', ''))
            self.start_processing()
            self.update_processing_status('in_progress')
            log.info(f"Updated processing status to in_progress for {self.path_on_fast_store}")
            self.send_message('processing_started')
            self.stitch_maxips()

    def start_processing(self):
        log.info(f"Queuing processing for {self.path_on_fast_store}")
        cmd = [
            '/h20/home/lab/miniconda3/envs/mesospim_dev/bin/python',
            '/h20/home/lab/src/mesospim_utils_v0.1/mesospim_utils/automated.py',
            # '/h20/home/lab/src/mesospim_utils_zarr/mesospim_utils/mesospim_utils/automated.py',
            'automated-method-slurm',
            self.path if ' ' not in self.path else f'"{self.path}"',
            '--final-file-type', 'ims'
        ]
        subprocess.run(cmd)

    def clean_up_before_moving(self):
        if self.refractive_index:  # decon will be done
            decon_folder = os.path.join(self.path_on_fast_store, 'decon')
            if os.path.exists(decon_folder):
                h5_files = glob(os.path.join(decon_folder, "*.h5"))
                ome_zarr_dirs = glob(os.path.join(decon_folder, "*.ome.zarr"))
                if len(h5_files) or len(ome_zarr_dirs):
                    import shutil
                    for f in h5_files:
                        trash_path = self.target_cleanup_path_in_trash(f)
                        if not trash_path:
                            continue
                        os.makedirs(os.path.dirname(trash_path), exist_ok=True)
                        shutil.move(f, trash_path)
                        self.touch_trash_marker(trash_path)
                    for f in ome_zarr_dirs:
                        trash_path = self.target_cleanup_path_in_trash(f)
                        if not trash_path:
                            continue
                        os.makedirs(os.path.dirname(trash_path), exist_ok=True)
                        shutil.move(f, trash_path)
                        self.touch_trash_marker(trash_path)

    @property
    def final_imaris_search_dir(self):
        if self.refractive_index:
            return os.path.join(self.path_on_fast_store, 'decon')
        return self.path_on_fast_store

    @property
    def full_path_to_imaris_file(self):
        candidates = sorted(glob(os.path.join(self.final_imaris_search_dir, '*.ims')))
        if len(candidates):
            return candidates[0]
        return None

    @property
    def tile_files(self):
        if not os.path.exists(self.path_on_fast_store):
            return []

        tile_entries = []
        for entry_name in os.listdir(self.path_on_fast_store):
            entry_path = os.path.join(self.path_on_fast_store, entry_name)
            if self.file_type == "ome.zarr" and os.path.isdir(entry_path) and entry_name.endswith('.ome.zarr'):
                for subentry_name in os.listdir(entry_path):
                    subentry_path = os.path.join(entry_path, subentry_name)
                    if not re.search(self.tile_name_pattern, subentry_name):
                        continue
                    if os.path.isdir(subentry_path) and subentry_name.endswith('.ome.zarr'):
                        tile_entries.append(subentry_path)
        return sorted(tile_entries)
