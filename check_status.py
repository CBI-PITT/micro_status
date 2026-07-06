"""
Database schema:

Dataset:
    id
    name
    cl_number - ForeignKey to CLNumber
    pi - ForeignKey to PI
    imaging_status (in_progress, paused, finished)
    processing_status (not_started, started, stitched, moved_to_hive, denoised, built_ims, finished)
    path_on_fast_store
    path_on_hive
    imaris_file_path
    channels
    imaging_no_progress_time
    processing_no_progress_time

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
    3) imaging finished
    4) processing started
    5) processing_paused (crashed?)
    6) processing finished

Other warnings:
    1) Low space on Hive
    2) Low space on FastStore


pip install python-dotenv

ROADMAP:
    - track moving to hive
    - more informative processing statuses
    - respond to messages in threads

"""

import json
import logging
import os
import re
import requests
import shutil
import subprocess
import sqlite3
import time
import traceback
from datetime import datetime, timedelta
from glob import glob
from pathlib import Path, PureWindowsPath

import tifffile
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from imaris_ims_file_reader import ims

from micro_status.dataset import Dataset
from micro_status.mesospim_dataset import MesoSPIMDataset, MesoSPIMZarrDataset
from micro_status.rscm_dataset import RSCMDataset
from micro_status.settings import *  # TODO replace this with normal import
from micro_status.warning import Warning
from micro_status.utils import can_be_moved


# console_handler = logging.StreamHandler()
LOG_FILE_NAME_PATTERN = "/CBI_FastStore/Iana/bot_logs/{}_{}.txt"
file_handler = logging.FileHandler(
    LOG_FILE_NAME_PATTERN.format(
        os.uname().nodename,
        datetime.now().strftime(DATETIME_FORMAT)
    )
)
logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s',
    handlers=[file_handler]
    # handlers=[console_handler, file_handler]
)
log = logging.getLogger(__name__)


def check_if_new(file_path):
    """
    Check that vs_series file with given path is not in the database.
    """
    con = sqlite3.connect(DB_LOCATION)
    con.row_factory = lambda cursor, row: row[0]
    cur = con.cursor()
    records = cur.execute('SELECT path_on_fast_store FROM dataset').fetchall()
    con.close()
    return file_path not in records


def check_RSCM_imaging():
    print("\n ================ Checking RSCM Imaging ==============\n")
    # Discover all vs_series.dat files in the acquisition directory
    datasets = []
    max_depth = 4
    for root, dirs, files in os.walk(RSCM_FASTSTORE_ACQUISITION_FOLDER):
        depth = root[len(RSCM_FASTSTORE_ACQUISITION_FOLDER):].count(os.sep)
        if depth >= max_depth:
            dirs[:] = []  # stop descending further
        for file in files:
            if file.endswith("vs_series.dat"):
                file_path = Path(os.path.join(root, file))
                file_path = file_path.parent
                if 'stack' in str(file_path.name):
                    datasets.append(str(file_path))
    print("\tUnique RSCM datasets found: ", len(datasets))

    for file_path in datasets:
        print("\t\t- ", file_path)
        is_new = check_if_new(file_path)
        new_dataset_marker_json = os.path.join(file_path, NEW_DATASET_MARKER_FILENAME)
        if is_new:
            log.info(f"New RSCM dataset at {file_path}")
            # check whether it's an existing dataset that got renamed
            if os.path.exists(new_dataset_marker_json):
                # read .microstatus.json file in the root folder of the dataset
                old_path_data = json.load(open(new_dataset_marker_json, 'r'))
                old_path = old_path_data['path']
                dataset = RSCMDataset(old_path)
                dataset.update_db_field('path_on_fast_store', file_path)
                log.info(f"Updated path on FastStore for renamed dataset from {old_path} to {file_path}")
                dataset.path_on_fast_store = file_path
                path_data = {"path": file_path}
                json.dump(path_data, open(new_dataset_marker_json, "w"))
            else:
                dataset = RSCMDataset.create(file_path)
                # create .microstatus.json file in the root folder of the dataset
                path_data = {"path": file_path}
                json.dump(path_data, open(new_dataset_marker_json, "w"))
                if "demo" in str(dataset.name).lower() or "test" in str(dataset.path_on_fast_store).lower():
                    # demo dataset
                    log.info(f"Ignoring demo dataset {dataset}")
                    print(f"\t\t\tIgnoring demo dataset {dataset}")
                    dataset.send_message('ignoring_demo_dataset')
                    dataset.mark_imaging_finished()
                    dataset.update_processing_status('finished')
                    dataset.update_db_field("moved", 1)
                    continue
                dataset.send_message('imaging_started')
        else:
            dataset = RSCMDataset(file_path)
            print("\t\t\t", dataset.imaging_status)
            # Create .microstatus.json file in the root folder of the dataset if it doesn't exist
            # This is for old datasets. Can be removed after all old datasets get moved to h20
            if not os.path.exists(new_dataset_marker_json):
                path_data = {"path": file_path}
                json.dump(path_data, open(new_dataset_marker_json, "w"))

            if dataset.imaging_status == 'in_progress':
                # print("Imaging status is 'in-progress'")
                got_finished, has_progress, error_flag = dataset.check_imaging_progress()
                if error_flag:
                    dataset.mark_imaging_paused()
                    log.info(f"Updated imaging status to paused for {file_path}")
                    dataset.send_message('broken_tiff_file')
                    continue
                if got_finished:
                    dataset.mark_imaging_finished()
                    log.info(f"Updated imaging status to finished for {file_path}")
                    dataset.send_message('imaging_finished')
                    if dataset.delete_405:
                        log.info(f"Deleting 405 channel for {dataset}")
                        dataset.delete_channel_405()
                    if '_cont_' not in dataset.name.lower():
                        dataset.start_processing()
                    continue
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
                            dataset.mark_imaging_paused()
                            log.info(f"Updated imaging status to paused for {file_path}")
                            dataset.send_message('imaging_paused')
            elif dataset.imaging_status == 'needs_attention':
                finished, has_progress, error_flag = dataset.check_imaging_progress()  # maybe imaging resumed
                if not has_progress:
                    continue
                else:
                    # dataset.mark_has_imaging_progress()
                    dataset.mark_imaging_resumed()
                    log.info(f"Updated imaging status to in_progress for {file_path}")


def check_mesoSPIM_imaging():
    print("\n ================ Checking MesoSPIM imaging ===============\n")
    # Discover all metadata files in the acquisition directory
    datasets = set()
    zarr_datasets = set()
    max_depth = 4
    for root, dirs, files in os.walk(MESOSPIM_FASTSTORE_ACQUISITION_FOLDER):
        depth = root[len(MESOSPIM_FASTSTORE_ACQUISITION_FOLDER):].count(os.sep)
        if depth >= max_depth:
            dirs[:] = []  # stop descending further
        for file in files:
            if file.endswith(".btf_meta.txt"):
                file_path = Path(os.path.join(root, file))
                file_path = file_path.parent
                datasets.add(str(file_path))
            elif file.endswith(".ome.zarr_meta.txt"):
                file_path = Path(os.path.join(root, file))
                file_path = file_path.parent
                zarr_datasets.add(str(file_path))
    print("\tUnique MesoSPIM btf datasets found: ", len(datasets))
    print("\tUnique MesoSPIM zarr datasets found: ", len(zarr_datasets))

    for file_path in list(datasets):
        try:
            print("\t\t- ", file_path)
            is_new = check_if_new(file_path)
            new_dataset_marker_json = os.path.join(file_path, NEW_DATASET_MARKER_FILENAME)
            if is_new:
                log.info(f"New mesoSPIM dataset at {file_path}")
                if os.path.exists(new_dataset_marker_json):
                    # # read .microstatus.json file in the root folder of the dataset
                    old_path_data = json.load(open(new_dataset_marker_json, 'r'))
                    old_path = old_path_data['path']
                    dataset = MesoSPIMDataset(old_path)
                    dataset.update_db_field('path_on_fast_store', file_path)
                    log.info(f"Updated path on FastStore for renamed dataset from {old_path} to {file_path}")
                    dataset.path_on_fast_store = file_path
                    path_data = {"path": file_path}
                    json.dump(path_data, open(new_dataset_marker_json, "w"))
                else:
                    dataset = MesoSPIMDataset.create(file_path)
                    # create .microstatus.json file in the root folder of the dataset
                    path_data = {"path": file_path}
                    json.dump(path_data, open(new_dataset_marker_json, "w"))
                    if "demo" in str(dataset.name).lower() or "test" in str(dataset.path_on_fast_store).lower():
                        # demo dataset
                        log.info(f"Ignoring demo dataset {dataset}")
                        print(f"Ignoring demo dataset {dataset}")
                        dataset.send_message('ignoring_demo_dataset')
                        dataset.mark_imaging_finished()
                        dataset.update_processing_status('finished')
                        dataset.update_db_field("moved", 1)
                        continue
                    dataset.send_message('imaging_started')

            dataset = MesoSPIMDataset(file_path)
            print("\t\t\t", dataset.imaging_status)
            if not os.path.exists(new_dataset_marker_json):
                path_data = {"path": file_path}
                json.dump(path_data, open(new_dataset_marker_json, "w"))
            # check whether imaging finished or paused
            if dataset.imaging_status == 'in_progress':
                dataset.check_imaging_progress()
            # elif dataset.imaging_status == "finished" and not dataset.moved and dataset.moving:
            #     dataset.check_if_moved()
            # elif dataset.imaging_status == "finished" and not dataset.moved and not dataset.moving:
            #     dataset.start_moving()  # TODO
            # elif dataset.imaging_status == "finished" and dataset.moved and dataset.path_on_hive is not None and dataset.processing_status == 'not_started':
            elif dataset.imaging_status == "finished" and dataset.processing_status == "not_started":
                dataset.start_processing()
                dataset.update_processing_status('in_progress')
                log.info(f"Updated processing status to in_progress for {file_path}")
                dataset.send_message('processing_started')
            elif dataset.imaging_status == "needs_attention":
                dataset.check_imaging_progress()
        except Exception:
            print(traceback.format_exc())
            continue


    for file_path in list(zarr_datasets):
        try:
            print("\t\t- ", file_path)
            is_new = check_if_new(file_path)
            new_dataset_marker_json = os.path.join(file_path, NEW_DATASET_MARKER_FILENAME)
            if is_new:
                log.info(f"New mesoSPIM dataset at {file_path}")
                if os.path.exists(new_dataset_marker_json):
                    print("\t\t\t>>>>>>>>>>>> renamed dataset >>>>>>>>>>>>")
                    old_path_data = json.load(open(new_dataset_marker_json, 'r'))
                    old_path = old_path_data['path']
                    dataset = MesoSPIMZarrDataset(old_path)
                    dataset.update_db_field('path_on_fast_store', file_path)
                    log.info(f"Updated path on FastStore for renamed dataset from {old_path} to {file_path}")
                    dataset.path_on_fast_store = file_path
                    path_data = {"path": file_path}
                    json.dump(path_data, open(new_dataset_marker_json, "w"))
                else:
                    dataset = MesoSPIMZarrDataset.create(file_path)
                    # create .microstatus.json file in the root folder of the dataset
                    path_data = {"path": file_path}
                    json.dump(path_data, open(new_dataset_marker_json, "w"))
                    if "demo" in dataset.name:
                        # demo dataset
                        log.info(f"Ignoring demo dataset {dataset}")
                        print(f"Ignoring demo dataset {dataset}")
                        dataset.send_message('ignoring_demo_dataset')
                        dataset.mark_imaging_finished()
                        dataset.update_processing_status('finished')
                        dataset.update_db_field("moved", 1)
                        continue
                    dataset.send_message('imaging_started')

            if not is_new:
                dataset = MesoSPIMZarrDataset(file_path)
            print("\t\t\t", dataset.imaging_status)
            if not os.path.exists(new_dataset_marker_json):
                path_data = {"path": file_path}
                json.dump(path_data, open(new_dataset_marker_json, "w"))
            if dataset.imaging_status == 'in_progress':
                dataset.check_imaging_progress()
        except Exception:
            print(traceback.format_exc())
            continue
def list_jobs(user):
    """List active SLURM jobs for a specific user."""
    result = subprocess.run(["squeue", "-u", user], capture_output=True, text=True)
    jobs = []
    for line in result.stdout.splitlines()[1:]:  # Skip the header line
        parts = line.split()
        if parts:
            jobs.append(parts[0])  # First column is the job ID
    return jobs


def kill_jobs_by_name(user, job_name):
    """Kill SLURM jobs with a specific name."""
    # List jobs matching the name
    result = subprocess.run(["squeue", "-u", user, "-n", job_name], capture_output=True, text=True)
    for line in result.stdout.splitlines()[1:]:  # Skip the header line
        parts = line.split()
        if parts:
            job_id = parts[0]
            subprocess.run(["scancel", job_id])


def list_and_kill_jobs(user, job_name=None):
    """List and kill SLURM jobs for a user, optionally filtered by name."""
    # Build squeue command
    squeue_cmd = ["squeue", "-u", user]
    if job_name:
        squeue_cmd.extend(["-n", job_name])

    # List jobs
    result = subprocess.run(squeue_cmd, capture_output=True, text=True)
    log.info(result.stdout)
    for line in result.stdout.splitlines()[1:]:  # Skip the header line
        parts = line.split()
        if parts:
            job_id = parts[0]
            print(f"Killing job {job_id}")
            subprocess.run(["scancel", job_id])


def job_in_queue(user, job_name):
    squeue_cmd = ["squeue", "-u", user]
    if job_name:
        squeue_cmd.extend(["-n", job_name])

    # List jobs
    result = subprocess.run(squeue_cmd, capture_output=True, text=True)
    # log.info(result.stdout)
    job_ids = []
    for line in result.stdout.splitlines()[1:]:  # Skip the header line
        parts = line.split()
        if parts:
            job_id = parts[0]
            job_ids.append(job_id)
    return len(job_ids) > 0


def check_RSCM_processing():
    print("\n ================ Checking RSCM processing ===============")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="not_started" AND imaging_status="finished" AND modality="rscm"'
    ).fetchall()

    if records:  # there's something to be stitched
        if not job_in_queue('lab', 'DASK_SCHED') or not job_in_queue('lab', 'DASK_WORKER'): # or not job_in_queue('lab', 'RSCM_Listen'):
            log.info("!!!!!!!!!!!!!!!!! Launching RSCM cluster !!!!!!!!!!!!!!!!!!")
            script_name = '/h20/home/lab/scripts/run_rscm_cluster.sh'
            result = subprocess.run([script_name], text=True, capture_output=True)
            log.info(result.stdout)

    for dataset_path in records:
        print("\t\t-", dataset_path[0])
        dataset = RSCMDataset(dataset_path[0])
        if dataset.check_being_stitched():
            dataset.update_processing_status('started')
            log.info(f"Updated processing status to started for {dataset_path[0]}")
            dataset.send_message('processing_started')

    # =========================  check stitching  ============================
    print("\n\t===== check stitching")

    records_not_started = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="not_started" AND imaging_status="finished" AND modality="rscm"'
    ).fetchall()
    records_started = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE modality="rscm" AND (processing_status="started" OR processing_status="in_progress")'
    ).fetchall()
    if not records_not_started and not records_started:  # nothing is being stitched. dask cluster can be stopped
        records_moving = cur.execute('SELECT path_on_fast_store FROM dataset WHERE processing_status="finished" AND moving=1 AND moved=0').fetchall()
        if not records_moving and job_in_queue('lab', 'DASK_SCHED'):
            if not len(glob("/CBI_FastStore/clusterStitchTEST/processing/*.txt")) and not len(glob("/CBI_FastStore/clusterStitchTEST/queueStitch/*.txt")) and not len(glob("/CBI_FastStore/clusterStitchTEST/tempQueue/*.txt")):
                log.info("!!!!!!!!!!!!!!!!! Stopping RSCM cluster !!!!!!!!!!!!!!!!!!")
                list_and_kill_jobs('lab', "DASK_SCHED")  # TODO check that nothing is being moved
                list_and_kill_jobs('lab', "DASK_WORKER")
                list_and_kill_jobs('lab', "RSCM_Listen")

    for dataset_path in records_started:
        print("\t\t-", dataset_path[0])
        dataset = RSCMDataset(dataset_path[0])
        if dataset.check_stitching_complete():
            # print("File in complete dir")
            # path_on_hive = os.path.join(HIVE_ACQUISITION_FOLDER, dataset.pi, dataset.cl_number, dataset.name)
            # if os.path.exists(path_on_hive):  # copying started
            if dataset.check_all_raw_composites_present() and dataset.check_all_raw_composites_same_size():
                # print("All composites present and same size")
                dataset.update_processing_status('stitched')
                log.info(f"Updated processing status to stitched for {dataset_path[0]}")
            # else:
                # print("All composites present: ", dataset.check_all_raw_composites_present())
                # print("All composites same size: ", dataset.check_all_raw_composites_same_size())
        elif dataset.check_stitching_errored():
            # File in error dir
            dataset.update_processing_status('needs_attention')
            dataset.update_db_field('paused', '1')
            log.info(f"Updated processing status to needs_attention for {dataset_path[0]}")
            dataset.send_message('stitching_error')
        elif dataset.check_being_stitched():
            # File in processing dir
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
                        dataset.update_processing_status('needs_attention')
                        dataset.update_db_field('paused', '1')
                        log.info(f"Updated processing status to needs_attention for {dataset_path[0]}")
                        dataset.send_message('stitching_stuck')

        if dataset.job_dir:
            if dataset.check_imaris_file_built():
                dataset.update_db_field('processing_status', 'finished')
                log.info(f"Updated processing status to finished for {dataset_path[0]}")
                dataset.send_message('built_ims')
                if not dataset.keep_composites:
                    dataset.clean_up_denoised_composites()
                dataset.start_moving()

    # ====================  check denoising =====================
    print("\n\t=====  check denoising")

    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE processing_status="stitched" AND modality="rscm" and paused=0'
    ).fetchall()
    if records:  # there's something to be denoised
        if not job_in_queue('lab', 'CBPy'):
            log.info("!!!!!!!!!!!!!!!!! Launching CBPY !!!!!!!!!!!!!!!!!!")
            script_name = '/h20/home/lab/scripts/run_cbpy.sh'
            result = subprocess.run(["sbatch", script_name], text=True, capture_output=True)
    else:
        if job_in_queue('lab', 'CBPy'):
            if not len(glob("/CBI_FastStore/clusterPy/active/*.xml")) and not len(glob("/CBI_FastStore/clusterPy/queueGPU/*.xml")):
                log.info("!!!!!!!!!!!!!!!!! Stopping CBPY !!!!!!!!!!!!!!!!!!")
                # list_and_kill_jobs('lab', "CBPy")

    for dataset_path in records:
        print("\t\t-", dataset_path[0])
        dataset = RSCMDataset(dataset_path[0])
        if dataset.job_dir:
            # print("Job dir is there")
            job_number = re.findall(r"\d+", os.path.basename(dataset.job_dir))[-1]
            dataset.update_job_number(job_number)
            denoising_started = len(glob(os.path.join(dataset.job_dir, "composite*.tif"))) > 0
            # print("Denoising started:", denoising_started)
            if not denoising_started:
                # TODO: check the # of queued files == number of composites ?
                in_queue = len(glob(os.path.join(CBPY_FOLDER, 'queueGPU', f"job_{dataset.job_number}*"))) > 0
                # print("In queue:", in_queue)
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
                            dataset.update_processing_status('needs_attention')
                            dataset.update_db_field('paused', 1)
                            log.info(f"Updated processing status to needs_attention for {dataset_path[0]}")
                            dataset.send_message('denoising_stuck')
                # check that something else is being denoised and making progress
                cbpy_works = dataset.check_cbpy_works()
                # print("CBPY works:", cbpy_works)
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
                            dataset.update_processing_status('needs_attention')
                            log.info(f"Updated processing status to needs_attention for {dataset_path[0]}")
                            dataset.send_message('denoising_stuck')
                continue

            denoising_finished = dataset.check_denoising_finished()
            # print('denoising_finished', denoising_finished)
            if denoising_finished:
                dataset.update_processing_status('denoised')
                log.info(f"Updated processing status to denoised for {dataset_path[0]}")
                dataset.clean_up_raw_composites()
                dataset.build_imaris_file()
                continue
            denoising_has_progress = dataset.check_denoising_progress()
            # print('denoising_has_progress', denoising_has_progress)
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
                        dataset.update_processing_status('needs_attention')
                        log.info(f"Updated processing status to needs_attention for {dataset_path[0]}")
                        dataset.send_message('denoising_stuck')

    # ===================== check building imaris file ========================
    print("\n\t=====  check building imaris file")

    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE processing_status="denoised" AND modality="rscm" and paused=0'
    ).fetchall()
    for dataset_path in records:
        print("\t\t-", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
        if os.path.exists(dataset.full_path_to_imaris_file):
            # print("Imaris file exists")
            try:
                # try to open imaris file
                ims_file = ims(dataset.full_path_to_imaris_file)
            except Exception as e:
                log.error(f"ERROR opening imaris file: {e}")
                dataset.send_message("broken_ims_file")
                dataset.update_db_field('processing_status', 'needs_attention')
                dataset.update_db_field('paused', '1')
                # dataset.requeue_ims()

                # update ims_size=0 in processing_summary
                processing_summary = dataset.get_processing_summary()
                value_from_db = processing_summary.get('building_ims')
                if value_from_db:
                    value_from_db.update({'ims_size': 0})
                    dataset.update_processing_summary({'building_ims': value_from_db})
                continue
            else:
                dataset.update_db_field('processing_status', 'finished')
                dataset.send_message('built_ims')
                if not dataset.keep_composites:
                    dataset.clean_up_denoised_composites()
                dataset.start_moving()
        elif os.path.exists(dataset.full_path_to_ims_part_file):
            # Building of ims file in-progress
            # print("Building Imaris file in-progress")
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
                        dataset.update_db_field('paused', 1)
                        # dataset.send_message('ims_build_stuck')
                        #dataset.requeue_ims()
                        #dataset.send_message('requeue_ims')
        else:
            # ims file is not being built
            print("Imaris file is not being built")
            # in_queue = os.path.exists(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', dataset.imsqueue_file_name))
            # if dataset.in_imaris_queue:
            #     if dataset.processing_no_progress_time:
            #         dataset.mark_has_processing_progress()
            #     # continue
            # else:
            #     if not dataset.processing_no_progress_time:
            #         dataset.mark_no_processing_progress()
            #     else:
            #         progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
            #         if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
            #             dataset.update_processing_status('paused')
            #             # dataset.send_message('ims_build_stuck')

            # # check what other file is being processed, check its size
            # ims_converter_works = dataset.check_ims_converter_works()
            # if ims_converter_works:
            #     if dataset.processing_no_progress_time:
            #         dataset.mark_has_processing_progress()
            #     continue
            # else:
            #     if not dataset.processing_no_progress_time:
            #         dataset.mark_no_processing_progress()
            #     else:
            #         progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
            #         if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
            #             dataset.update_processing_status('paused')
            #             # dataset.send_message('ims_build_stuck')

    # Eventually datasets should be on hive
    # ===================== check moving ========================
    print("\n\t=====  check moving")

    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE modality = "rscm" AND processing_status="finished" AND moved=0'
    ).fetchall()
    if records and can_be_moved():  # there's something to be stitched
        if not job_in_queue('lab', 'DASK_SCHED') or not job_in_queue('lab', 'DASK_WORKER'): # or not job_in_queue('lab', 'RSCM_Listen'):
            log.info("!!!!!!!!!!!!!!!!! Launching RSCM cluster !!!!!!!!!!!!!!!!!!")
            script_name = '/h20/home/lab/scripts/run_rscm_cluster.sh'
            result = subprocess.run([script_name], text=True, capture_output=True)
            log.info(result.stdout)
            # listen_script = "/h20/home/lab/scripts/run_RSCM_stitch_listen.sh"
            # result = subprocess.run(['sbatch', listen_script], text=True, capture_output=True)
            # log.info(result.stdout)

    # print("\nDatasets that should be moved:")
    for dataset_path in records:
        print("\t\t-", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
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
                # dataset.update_processing_status('finished')
                dataset.send_message("processing_finished")

    # ==================== Handle 'paused' processing status ==================
    print("\n\t=====  check paused datasets")
    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE modality="rscm" and imaging_status="finished" and paused=1'
    ).fetchall()
    for dataset_path in records:
        print("\t\t-", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
        guessed_processing_status = dataset.guess_processing_status()
        # print("guessed_processing_status:", guessed_processing_status)
        progress_methods_map = {
            "started": dataset.check_stitching_progress,
            "stitched": dataset.check_denoising_progress,
            "denoised": dataset.check_ims_building_progress,
            # "built_ims": dataset.check_finalization_progress
        }
        has_progress = progress_methods_map[guessed_processing_status]()
        # print("has progress", has_progress)
        if has_progress:
            dataset.mark_has_processing_progress()
            dataset.update_processing_status(guessed_processing_status)
        # print("guessed_processing_status", guessed_processing_status)
        # print("dataset.job_dir", dataset.job_dir)
        # print("os.path.exists(dataset.job_dir)", os.path.exists(dataset.job_dir))
        if guessed_processing_status == "finished" and dataset.job_dir and dataset.job_dir.startswith('/CBI_FastStore') and os.path.exists(dataset.job_dir):
            dataset.start_moving()


def check_moving():
    print("\n ================ Checking moving ===============")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="finished" AND moved=0 AND paused=0'
    ).fetchall()
    for dataset_path in records:
        print("\t-", dataset_path[0])
        if dataset_path[0].startswith(MESOSPIM_FASTSTORE_ACQUISITION_FOLDER):
            try:
                dataset = MesoSPIMDataset(dataset_path[0])
            except:
                print("\t\tWARNING: Invalid dataset at", dataset_path[0])
                continue
        elif dataset_path[0].startswith(RSCM_FASTSTORE_ACQUISITION_FOLDER):
            dataset = RSCMDataset(dataset_path[0])
        else:
            continue

        if dataset.moving and not dataset.moved:
            dataset.check_if_moved()

        if dataset.moved:
            continue

        if not os.path.exists(dataset.path_on_fast_store):
            verified_path_on_hive = None
            if dataset.path_on_hive and os.path.exists(dataset.path_on_hive):
                verified_path_on_hive = dataset.path_on_hive
            elif os.path.exists(dataset.target_path_on_hive):
                verified_path_on_hive = dataset.target_path_on_hive

            if verified_path_on_hive:
                if dataset.path_on_hive != verified_path_on_hive:
                    dataset.update_path_on_hive(verified_path_on_hive)
                dataset.update_db_field('moved', 1)
                dataset.moved = True
                dataset.update_db_field('moving', 0)
                dataset.moving = False
                dataset.update_db_field('paused', 0)
                dataset.paused = False
                log.info(f"Marked dataset moved after confirming Hive destination for missing FastStore source: {dataset.path_on_fast_store} -> {verified_path_on_hive}")
            else:
                dataset.update_db_field('moving', 0)
                dataset.moving = False
                dataset.mark_processing_paused()
                log.warning(f"Paused dataset missing from FastStore Acquire with no confirmed Hive destination: {dataset.path_on_fast_store}")
            continue

        if not dataset.moving:
            dataset.start_moving()
        # elif dataset.moved and dataset.path_on_hive is not None and dataset.processing_status == 'not_started':
        #     dataset.start_processing()
        #     dataset.update_processing_status('in_progress')
        #     dataset.send_message('processing_started')


def check_mesoSPIM_processing():
    print("\n ================ Checking MesoSPIM processing ===============\n")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    # datasets where either decon or imaris conversion has started
    # if refractive_index is given, check whether decon finished or in progress. If finished, update status to "decon_done"
    # if no refractive index, check whether imaris files are fully converted
    # check if Montage file present (just in case)
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE modality = "mesospim" AND processing_status="in_progress" AND paused=0'
    ).fetchall()
    for dataset_path in records:
        print('\t-', dataset_path[0])
        try:
            if len(glob(os.path.join(dataset_path[0], "*.ome.zarr.xml"))):
                try:
                    dataset = MesoSPIMZarrDataset(dataset_path[0])
                except:
                    print(traceback.format_exc())
                    continue
            else:
                try:
                    dataset = MesoSPIMDataset(dataset_path[0])
                except:
                    print("\t\tWARNING: Invalid dataset at", dataset_path[0])
                    continue
            total_tile_files = dataset.get_total_MesoSPIM_tiles()
            channels = dataset.get_total_MesoSPIM_colors_from_file_list()
            if total_tile_files and channels:
                if dataset.refractive_index:  # decon will be done
                    decon_folder = os.path.join(dataset.path_on_fast_store, 'decon')
                    imaris_folder = os.path.join(decon_folder, 'ims_files')
                    if type(dataset) == MesoSPIMDataset and os.path.exists(decon_folder):
                        decon_tif_files = os.listdir(decon_folder)
                        decon_tif_files = [x for x in decon_tif_files if x.endswith('.tif') and not x.startswith('psf_')]
                        if len(decon_tif_files) == total_tile_files:
                            # check all tiff files are the same size
                            tif_sizes = [os.path.getsize(os.path.join(decon_folder, x)) for x in decon_tif_files]
                            if len(set(tif_sizes)) == 1:
                                # decon finished
                                dataset.update_processing_status("decon_done")
                                log.info(f"Changed processing status to decon_done for {dataset}")
                else:  # no decon
                    imaris_folder = os.path.join(dataset.path_on_fast_store, 'ims_files')
                if type(dataset) == MesoSPIMZarrDataset:
                    final_ims_file = dataset.full_path_to_imaris_file
                    if final_ims_file:
                        try:
                            ims_file = ims(final_ims_file)
                        except:
                            print("\t\tbuilding montage")
                        try:
                            dataset.update_imaris_file_path(final_ims_file)
                            dataset.update_processing_status('finished')
                            log.info(f"Changed processing status to finished for {dataset}")
                            dataset.send_message('processing_finished')
                            dataset.clean_up_before_moving()
                            dataset.start_moving()
                        except:
                            print(traceback.format_exc())
                    raise Found

                ims_files = sorted(glob(os.path.join(imaris_folder, '*Tile*_Ch*_Sh*.ims')))
                total_ims_files = len(ims_files)
                if total_ims_files == int(total_tile_files / channels):
                    all_ims_files_open = dataset.check_tile_ims_files()
                    if all_ims_files_open:
                        dataset.update_processing_status("ims_converted")
                        log.info(f"Changed processing status to ims_converted for {dataset}")
                        montage_files = glob(os.path.join(imaris_folder, '*ontage.ims'))
                        if len(montage_files) > 0:
                            try:
                                ims_file = ims(montage_files[0])
                                dataset.update_processing_status('finished')
                                log.info(f"Changed processing status to finished for {dataset}")
                                dataset.send_message('built_ims')
                                dataset.start_moving()
                            except:
                                pass
                        else:
                            print("\t\tstitching")
                            dataset.check_auto_stitch()
                    else:
                        print("\t\tfound broken ims files")
                else:
                    print("\t\tstarted")
            else:
                dataset.mark_processing_paused()
                dataset.update_processing_status('needs_attention')
        except:
            print(traceback.format_exc())
            continue

    # check datasets where decon is done
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE modality = "mesospim" AND processing_status="decon_done" and paused=0'
    ).fetchall()
    for dataset_path in records:
        print('\t-', dataset_path[0])
        try:
            dataset = MesoSPIMDataset(dataset_path[0])
        except:
            print("\t\tWARNING: Invalid dataset at", dataset_path[0])
            continue
        print("\t\tdecon done")
        decon_folder = os.path.join(dataset.path_on_fast_store, 'decon')
        imaris_folder = os.path.join(decon_folder, 'ims_files')
        ims_files = sorted(glob(os.path.join(imaris_folder, '*Tile*_Ch*_Sh*.ims')))
        total_ims_files = len(ims_files)
        channels = dataset.get_total_MesoSPIM_colors_from_file_list()
        total_tile_files = dataset.get_total_MesoSPIM_tiles()
        if not total_tile_files or not channels:
            continue
        if total_ims_files == int(total_tile_files / channels):
            all_ims_files_open = dataset.check_tile_ims_files()
            if all_ims_files_open:
                dataset.update_processing_status("ims_converted")
                log.info(f"Changed processing status to ims_converted for {dataset}")
                montage_files = glob(os.path.join(imaris_folder, '*ontage.ims'))
                if len(montage_files) > 0:
                    try:
                        ims_file = ims(montage_files[0])
                        dataset.update_processing_status('finished')
                        log.info(f"Changed processing status to finished for {dataset}")
                        dataset.send_message('processing_finished')
                        dataset.start_moving()
                    except:
                        print("\t\tbuilding montage")

    # check datasets where all tiles are converted to ims
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE modality = "mesospim" AND processing_status="ims_converted" AND paused=0'
    ).fetchall()
    for dataset_path in records:
        print('\t-', dataset_path[0])
        try:
            dataset = MesoSPIMDataset(dataset_path[0])
        except:
            print("\t\tWARNING: Invalid dataset at", dataset_path[0])
            continue
        print("\t\tims converted")
        if dataset.refractive_index:  # decon
            decon_folder = os.path.join(dataset.path_on_fast_store, 'decon')
            imaris_folder = os.path.join(decon_folder, 'ims_files')
        else:  # no decon
            imaris_folder = os.path.join(dataset.path_on_fast_store, 'ims_files')
        montage_files = glob(os.path.join(imaris_folder, '*ontage.ims'))
        if len(montage_files) > 0:
            try:
                ims_file = ims(montage_files[0])
                dataset.update_processing_status('finished')
                log.info(f"Changed processing status to finished for {dataset}")
                dataset.send_message('processing_finished')
                dataset.start_moving()
            except:
                print("\t\tbuilding montage")


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
                if storage_unit == "faststore":
                    purge_faststore_trash()
            else:  # record doesn't exist
                warning = Warning.create(f'low_space_{storage_unit}')
                warning.send_message()
                if storage_unit == "faststore":
                    purge_faststore_trash()
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
    hive = [x for x in beegfs_nodes if x.endswith('h20')][0]
    faststore = [x for x in beegfs_nodes if x.endswith('FastStore')][0]
    faststore_used_percent_str = [x for x in faststore.split() if x.endswith("%")][0]
    hive_used_percent_str = [x for x in hive.split() if x.endswith("%")][0]
    faststore_used_percent = int(faststore_used_percent_str.replace("%", ""))
    hive_used_percent = int(hive_used_percent_str.replace("%", ""))
    check(hive_used_percent, "hive")
    check(faststore_used_percent, "faststore")


def prune_empty_dirs(root_path):
    for current_root, dirnames, filenames in os.walk(root_path, topdown=False):
        if dirnames or filenames:
            continue
        if current_root == root_path:
            continue
        os.rmdir(current_root)


def purge_faststore_trash():
    trash_root = FASTSTORE_TRASH_LOCATION
    if not os.path.exists(trash_root):
        log.info(f"FastStore trash folder does not exist for emergency purge: {trash_root}")
        return

    log.warning(f"Emergency purging FastStore trash at {trash_root}")
    for entry in os.scandir(trash_root):
        entry_path = entry.path
        if not entry_path.startswith(f"{FASTSTORE_TRASH_LOCATION}{os.sep}"):
            log.warning(f"Skipping emergency purge for unexpected path: {entry_path}")
            continue

        if entry.is_dir(follow_symlinks=False):
            shutil.rmtree(entry_path)
            log.info(f"Removed folder: {entry_path}")
        else:
            os.remove(entry_path)
            # log.info(f"Removed file: {entry_path}")


def cleanup_faststore_trash_root(trash_root):
    if not os.path.exists(trash_root):
        log.info(f"FastStore trash folder does not exist: {trash_root}")
        return

    log.info(f"Cleaning FastStore trash at {trash_root}")
    cutoff_ts = time.time() - (TRASH_RETENTION_DAYS * 24 * 60 * 60)
    marker_pattern = f"*{TRASH_TIMESTAMP_MARKER_SUFFIX}"

    for marker_path in glob(os.path.join(trash_root, "**", marker_pattern), recursive=True):
        trash_entry_path = marker_path[:-len(TRASH_TIMESTAMP_MARKER_SUFFIX)]
        if not trash_entry_path.startswith(FASTSTORE_TRASH_LOCATION):
            continue
        if not os.path.exists(trash_entry_path):
            try:
                os.remove(marker_path)
            except FileNotFoundError:
                pass
            continue

        try:
            marker_mtime = os.path.getmtime(marker_path)
        except FileNotFoundError:
            continue
        if marker_mtime > cutoff_ts:
            continue

        if os.path.isdir(trash_entry_path):
            shutil.rmtree(trash_entry_path)
            log.info(f"Removed folder: {trash_entry_path}")
        else:
            os.remove(trash_entry_path)

        try:
            os.remove(marker_path)
        except FileNotFoundError:
            pass

    # prune_empty_dirs(trash_root)


def cleanup_faststore_trash():
    current_day = datetime.today().strftime("%Y-%m-%d")
    marker_path = FASTSTORE_TRASH_CLEANUP_MARKER

    if os.path.exists(marker_path):
        with open(marker_path, 'r') as f:
            if f.read().strip() == current_day:
                return

    for trash_root in [RSCM_FASTSTORE_TRASH_FOLDER, MESOSPIM_FASTSTORE_TRASH_FOLDER]:
        cleanup_faststore_trash_root(trash_root)

    os.makedirs(os.path.dirname(marker_path), exist_ok=True)
    with open(marker_path, 'w') as f:
        f.write(current_day)


def move_stale_faststore_datasets():
    current_day = datetime.today().strftime("%Y-%m-%d")
    marker_path = STALE_ACQUIRE_MOVE_MARKER

    if os.path.exists(marker_path):
        with open(marker_path, 'r') as f:
            if f.read().strip() == current_day:
                return

    cutoff = datetime.now() - timedelta(days=STALE_ACQUIRE_MOVE_AGE_DAYS)
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        'SELECT path_on_fast_store, created FROM dataset WHERE moved=0 AND path_on_fast_store LIKE ?',
        (f'{FASTSTORE_ACQUISITION_FOLDER}%',)
    ).fetchall()
    con.close()

    for dataset_path, created_str in records:
        if not created_str:
            continue

        try:
            created = datetime.strptime(created_str, DATETIME_FORMAT)
        except ValueError:
            log.warning(f"Skipping stale move check for dataset with invalid created date: {dataset_path} ({created_str})")
            continue

        if created > cutoff:
            continue

        log.info(f"Dataset older than {STALE_ACQUIRE_MOVE_AGE_DAYS} days still on FastStore Acquire: {dataset_path}")

        if dataset_path.startswith(MESOSPIM_FASTSTORE_ACQUISITION_FOLDER):
            try:
                dataset = MesoSPIMDataset(dataset_path)
            except Exception:
                log.warning(f"Skipping invalid MesoSPIM dataset during stale move check: {dataset_path}")
                continue
        elif dataset_path.startswith(RSCM_FASTSTORE_ACQUISITION_FOLDER):
            try:
                dataset = RSCMDataset(dataset_path)
            except Exception:
                log.warning(f"Skipping invalid RSCM dataset during stale move check: {dataset_path}")
                continue
        else:
            continue

        if dataset.moving and not dataset.moved:
            dataset.check_if_moved()
        elif not dataset.moving and not dataset.moved:
            dataset.start_moving()

    os.makedirs(os.path.dirname(marker_path), exist_ok=True)
    with open(marker_path, 'w') as f:
        f.write(current_day)


def check_analysis():
    """
    If the finished dataset is a brain, send it for analysis by PEACE pipeline
    """
    print("Checking analysis...")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="finished" AND is_brain=1'
    ).fetchall()
    for dataset_path in records:
        dataset = Dataset(dataset_path[0])
        print("Brain dataset", dataset)
        if not dataset.peace_json_created:
            dataset.create_peace_json()


def db_backup():
    timestamp = datetime.now().strftime("%Y-%m-%d")
    backup_file_name = os.path.join(DB_BACKUPS_DIR, os.path.basename(DB_LOCATION).replace('.db', f'-{timestamp}.db'))
    if not os.path.exists(backup_file_name):
        import shutil
        shutil.copyfile(DB_LOCATION, backup_file_name)

def summary_message():
    def get_status_summary():
        conn = sqlite3.connect(DB_LOCATION)
        cursor = conn.cursor()

        # Datasets currently being imaged
        cursor.execute("SELECT path_on_fast_store FROM dataset WHERE imaging_status = 'in_progress'")
        imaging = [row[0] for row in cursor.fetchall()]

        # Datasets currently being processed
        cursor.execute("SELECT path_on_fast_store FROM dataset WHERE processing_status = 'in_progress'")
        processing = [row[0] for row in cursor.fetchall()]

        # Datasets that need attention
        cursor.execute("SELECT path_on_fast_store FROM dataset WHERE paused = 1")
        paused = [row[0] for row in cursor.fetchall()]

        conn.close()
        return imaging, processing, paused

    def format_message(imaging, processing, paused):
        today = datetime.now().strftime('%Y-%m-%d')
        message = f"*📊 Daily Dataset Status – {today}*\n"

        if imaging:
            message += "\n🔬 *Imaging in progress:*\n" + "\n".join([f"• {ds}" for ds in imaging])
        else:
            message += "\n🔬 *Imaging in progress:* None"

        if processing:
            message += "\n\n🧮 *Processing in progress:*\n" + "\n".join([f"• {ds}" for ds in processing])
        else:
            message += "\n\n🧮 *Processing in progress:* None"

        if paused:
            message += "\n\n🚨 *Needs attention:*\n" + "\n".join([f"• {ds}" for ds in paused])
        else:
            message += "\n\n✅ *No datasets require attention.*"

        return message

    def post_to_slack(msg_text, channel, header):
        print(msg_text)
        payload = {
            "channel": channel,
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
            response = requests.post(SLACK_URL, data=json.dumps(payload), headers=header)

    import pytz
    # Get the current time in UTC
    current_time_utc = datetime.now(pytz.utc)
    # Get the local timezone
    local_timezone = pytz.timezone('America/New_York')
    # Convert UTC time to local time
    local_time = current_time_utc.astimezone(local_timezone)
    if local_time.hour >= 9:
        # update Warning table with active=1
        conn = sqlite3.connect(DB_LOCATION)
        cursor = conn.cursor()
        row_id = cursor.execute("SELECT id FROM warning WHERE type = 'daily_summary'").fetchone()
        res = cursor.execute(f'UPDATE warning SET active = 1 WHERE id={row_id[0]}')
        conn.commit()
        conn.close()
        # query Warning table
        conn = sqlite3.connect(DB_LOCATION)
        cursor = conn.cursor()
        row = cursor.execute("SELECT message_sent, active FROM warning WHERE type = 'daily_summary'").fetchone()
        conn.close()
        # if daily_summary has message_sent=1: do nothing
        message_sent = int(row[0])
        active = int(row[1])
        # if daily_summary has message_sent=0 and active=1: send message
        if message_sent == 0 and active == 1:
            imaging, processing, paused = get_status_summary()
            msg = format_message(imaging, processing, paused)
            post_to_slack(msg, SLACK_CHANNEL_ID, SLACK_HEADERS)

            # update Warning table with message_sent=1
            conn = sqlite3.connect(DB_LOCATION)
            cursor = conn.cursor()
            res = cursor.execute(f'UPDATE warning SET message_sent = 1 WHERE id={row_id[0]}')
            conn.commit()
            conn.close()
    else:
        # update Warning table with message_sent=0 and active=0
        conn = sqlite3.connect(DB_LOCATION)
        cursor = conn.cursor()
        row_id = cursor.execute("SELECT id FROM warning WHERE type = 'daily_summary'").fetchone()
        res = cursor.execute(f'UPDATE warning SET active = 0, message_sent = 0 WHERE id={row_id[0]}')
        conn.commit()
        conn.close()


class Found(BaseException):
    pass


def scan():
    try:
        check_storage()
        cleanup_faststore_trash()
        move_stale_faststore_datasets()
        check_RSCM_imaging()
        check_mesoSPIM_imaging()
        check_RSCM_processing()
        check_mesoSPIM_processing()
        check_moving()
        db_backup()
        # check_analysis()
        summary_message()
    except Exception as e:
        log.error(f"\nEXCEPTION: {e}\n")
        print(traceback.format_exc())

    print("========================== Waiting 30 seconds ========================")
    time.sleep(30)


def scan_debug():
    check_storage()
    cleanup_faststore_trash()
    move_stale_faststore_datasets()
    check_RSCM_imaging()
    check_mesoSPIM_imaging()
    check_RSCM_processing()
    check_mesoSPIM_processing()
    check_moving()
    db_backup()
    # check_analysis()
    summary_message()
    time.sleep(10)


if __name__ == "__main__":
    while True:
        scan()
        # scan_debug()
        # print("========================== Waiting 59 seconds ========================")
        # time.sleep(59)
