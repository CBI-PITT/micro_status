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
from datetime import datetime
from glob import glob
from pathlib import Path, PureWindowsPath

import tifffile
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from imaris_ims_file_reader import ims

from micro_status.dataset import Dataset
from micro_status.mesospim_dataset import MesoSPIMDataset
from micro_status.rscm_dataset import RSCMDataset
from micro_status.settings import *  # TODO replace this with normal import
from micro_status.warning import Warning
from micro_status.utils import can_be_moved


console_handler = logging.StreamHandler()
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
    handlers=[console_handler, file_handler]
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


# def read_dataset_record(file_path):
#     con = sqlite3.connect(DB_LOCATION)
#     cur = con.cursor()
#     record = cur.execute(f'SELECT * FROM dataset WHERE path_on_fast_store="{str(file_path)}"').fetchone()
#     con.close()
#
#     if not record:
#         print("WARNING: broken/partial dataset", file_path)
#         return
#
#     # print("Record", record)
#
#     pi_id = record[4]
#     con = sqlite3.connect(DB_LOCATION)
#     cur = con.cursor()
#     pi_name = cur.execute(f'SELECT name FROM pi WHERE id="{pi_id}"').fetchone()
#     con.close()
#     if pi_name:
#         pi_name = pi_name[0]
#
#     cl_number_id = record[3]
#     con = sqlite3.connect(DB_LOCATION)
#     cur = con.cursor()
#     cl_number = cur.execute(f'SELECT name FROM clnumber WHERE id="{cl_number_id}"').fetchone()
#     con.close()
#     if cl_number:
#         cl_number = cl_number[0]
#
#     dataset = Dataset(
#         db_id = record[0],
#         name = record[1],
#         path_on_fast_store = record[2],
#         cl_number = cl_number,
#         pi = pi_name,
#         imaging_status = record[5],
#         processing_status = record[6],
#
#         channels = record[10],
#         # z_layers_total = record[11],
#         # z_layers_current = record[12],
#         # ribbons_total = record[13],
#         # ribbons_finished = record[14],
#         imaging_no_progress_time = record[21],
#         processing_no_progress_time = record[22],
#         # z_layers_checked = record[19],
#         # keep_composites = record[20],
#         # delete_405 = record[21],
#         # is_brain=record[22],
#         # peace_json_created=record[23]
#     )
#     return dataset


def check_RSCM_imaging():
    # Discover all vs_series.dat files in the acquisition directory
    datasets = []
    for root, dirs, files in os.walk(RSCM_FASTSTORE_ACQUISITION_FOLDER):
        for file in files:
            if file.endswith("vs_series.dat"):
                file_path = Path(os.path.join(root, file))
                file_path = file_path.parent
                if 'stack' in str(file_path.name):
                    datasets.append(str(file_path))
    print("Unique datasets found: ", len(datasets))

    for file_path in datasets:
        print("Working on: ", file_path)
        is_new = check_if_new(file_path)
        if is_new:
            log.info("-----------------------New dataset--------------------------")
            dataset = RSCMDataset.create(file_path)
            if "demo" in dataset.name.lower():
                # demo dataset
                log.info(f"Ignoring demo dataset {dataset}")
                print(f"Ignoring demo dataset {dataset}")
                dataset.send_message('ignoring_demo_dataset')
                dataset.mark_imaging_finished()
                dataset.update_processing_status('finished')
                dataset.update_db_field("moved", 1)
                continue
            dataset.send_message('imaging_started')
        else:
            dataset = RSCMDataset(file_path)
            if dataset.imaging_status == 'in_progress':
                # print("Imaging status is 'in-progress'")
                got_finished, has_progress, error_flag = dataset.check_imaging_progress()
                if error_flag:
                    dataset.mark_paused()
                    dataset.send_message('broken_tiff_file')
                    continue
                # print("Imaging finished:", got_finished)
                if got_finished:
                    dataset.mark_imaging_finished()
                    dataset.send_message('imaging_finished')
                    if dataset.delete_405:
                        print("------------Deleting 405 channel")
                        dataset.delete_channel_405()
                    if '_cont_' not in dataset.name.lower():
                        dataset.start_processing()
                    continue
                # print("Imaging has progress:", has_progress)
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
                            dataset.mark_paused()
                            dataset.send_message('imaging_paused')
            elif dataset.imaging_status == 'paused':
                print("Imaging status is 'paused'")
                finished, has_progress, error_flag = dataset.check_imaging_progress()  # maybe imaging resumed
                if not has_progress:
                    continue
                else:
                    dataset.mark_has_imaging_progress()
                    dataset.mark_resumed()
                    # response = dataset.send_message('imaging_resumed')
                    # print(response)


def check_mesoSPIM_imaging():
    print("Checking MesoSPIM imaging")
    # Discover all metadata files in the acquisition directory
    datasets = set()
    for root, dirs, files in os.walk(MESOSPIM_FASTSTORE_ACQUISITION_FOLDER):
        for file in files:
            if file.endswith(".btf_meta.txt"):
                file_path = Path(os.path.join(root, file))
                file_path = file_path.parent
                datasets.add(str(file_path))
    print("Unique datasets found: ", len(datasets))
    print(*datasets, sep="\n")

    for file_path in list(datasets):
        print("\nWorking on: ", file_path)
        is_new = check_if_new(file_path)
        if is_new:
            log.info("-----------------------New mesoSPIM dataset--------------------------")
            dataset = MesoSPIMDataset.create(file_path)
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
        dataset = MesoSPIMDataset(file_path)
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
            dataset.send_message('processing_started')
        elif dataset.imaging_status == "paused":
            pass  # TODO check status again


def get_total_MesoSPIM_tiles(settings_bin_file):
    import sys
    sys.path.append('/h20/CBI/Iana/src/mesoSPIM-control')
    sys.path.append('/h20/home/iana/.conda/envs/mesospim/lib/python3.12/site-packages')
    import pickle
    f = open(settings_bin_file, 'rb')
    acquisition_list = pickle.load(f)
    total_btf_files = len(acquisition_list)
    return total_btf_files


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
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="not_started" AND imaging_status="finished" AND modality="rscm"'
    ).fetchall()
    print(">>>>>>>>>>>>>>>>>>>>> Records for stitching 1", records)

    if records:  # there's something to be stitched
        if not job_in_queue('lab', 'DASK_SCHED') or not job_in_queue('lab', 'DASK_WORKER'): # or not job_in_queue('lab', 'RSCM_Listen'):
            log.info("!!!!!!!!!!!!!!!!! Launching RSCM cluster !!!!!!!!!!!!!!!!!!")
            script_name = '/h20/home/lab/scripts/run_rscm_cluster.sh'
            result = subprocess.run([script_name], text=True, capture_output=True)
            log.info(result.stdout)
            # listen_script = "/h20/home/lab/scripts/run_RSCM_stitch_listen.sh"
            # result = subprocess.run(['sbatch', listen_script], text=True, capture_output=True)
            # log.info(result.stdout)
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    for dataset_path in records:
        print('dataset_path', dataset_path[0])
        dataset = RSCMDataset(dataset_path[0])
        if dataset.check_being_stitched():
            dataset.update_processing_status('started')
            dataset.send_message('processing_started')

    # =========================  check stitching  ============================
    print("=========================  check stitching  ============================")

    records_not_started = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="not_started" AND imaging_status="finished" AND modality="rscm"'
    ).fetchall()
    records_started = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE processing_status="started" AND modality="rscm"'
    ).fetchall()
    if not records_not_started and not records_started:  # nothing is being stitched. dask cluster can be stopped
        records_moving = cur.execute('SELECT path_on_fast_store FROM dataset WHERE modality = "rscm" AND processing_status="finished" AND moving=1 AND moved=0').fetchall()
        if not records_moving:
            log.info("!!!!!!!!!!!!!!!!! Stopping RSCM cluster !!!!!!!!!!!!!!!!!!")
            list_and_kill_jobs('lab', "DASK_SCHED")  # TODO check that nothing is being moved
            list_and_kill_jobs('lab', "DASK_WORKER")
            list_and_kill_jobs('lab', "RSCM_Listen")
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    print("\nDataset instances where stitching started:")
    for dataset_path in records_started:
        print("-----", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
        if dataset.check_stitching_complete():
            print("File in complete dir")
            # path_on_hive = os.path.join(HIVE_ACQUISITION_FOLDER, dataset.pi, dataset.cl_number, dataset.name)
            # if os.path.exists(path_on_hive):  # copying started
            if dataset.check_all_raw_composites_present() and dataset.check_all_raw_composites_same_size():
                print("All composites present and same size")
                dataset.update_processing_status('stitched')
            else:
                print("All composites present: ", dataset.check_all_raw_composites_present())
                print("All composites same size: ", dataset.check_all_raw_composites_same_size())
        elif dataset.check_stitching_errored():
            print("File in error dir")
            # dataset.update_processing_status('paused')
            dataset.update_db_field('paused', '1')
            dataset.send_message('stitching_error')
        elif dataset.check_being_stitched():
            print("File in processing dir")
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
                        # dataset.update_processing_status('paused')
                        dataset.update_db_field('paused', '1')
                        dataset.send_message('stitching_stuck')
        else:
            print("File in none of ClusterStitchTest dirs")

    # ====================  check denoising =====================
    print("====================  check denoising =====================")

    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE processing_status="stitched" AND modality="rscm" and paused=0'
    ).fetchall()
    if records:  # there's something to be denoised
        if not job_in_queue('lab', 'CBPy'):
            log.info("!!!!!!!!!!!!!!!!! Launching CBPY !!!!!!!!!!!!!!!!!!")
            script_name = '/h20/home/lab/scripts/run_cbpy.sh'
            result = subprocess.run(["sbatch", script_name], text=True, capture_output=True)
            log.info(result.stdout)
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    else:
        log.info("!!!!!!!!!!!!!!!!! Stopping CBPY !!!!!!!!!!!!!!!!!!")
        list_and_kill_jobs('lab', "CBPy")  # TODO check that nothing is being processed
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    print("\nDatasets that have been STITCHED:")
    for dataset_path in records:
        print("-----", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
        if dataset.job_dir:
            print("Job dir is there")
            job_number = re.findall(r"\d+", os.path.basename(dataset.job_dir))[-1]
            dataset.update_job_number(job_number)
            denoising_started = len(glob(os.path.join(dataset.job_dir, "composite*.tif"))) > 0
            print("Denoising started:", denoising_started)
            if not denoising_started:
                # TODO: check the # of queued files == number of composites ?
                in_queue = len(glob(os.path.join(CBPY_FOLDER, 'queueGPU', f"job_{dataset.job_number}*"))) > 0
                print("In queue:", in_queue)
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
                            # dataset.update_processing_status('paused')
                            dataset.update_db_field('paused', 1)
                            dataset.send_message('denoising_stuck')
                # check that something else is being denoised and making progress
                cbpy_works = dataset.check_cbpy_works()
                print("CBPY works:", cbpy_works)
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
                            dataset.update_processing_status('paused')
                            dataset.send_message('denoising_stuck')
                continue

            denoising_finished = dataset.check_denoising_finished()
            print('denoising_finished', denoising_finished)
            if denoising_finished:
                dataset.update_processing_status('denoised')
                dataset.clean_up_raw_composites()
                dataset.build_imaris_file()
                # dataset.start_moving()
                continue
            denoising_has_progress = dataset.check_denoising_progress()
            print('denoising_has_progress', denoising_has_progress)
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
                        dataset.update_processing_status('paused')
                        dataset.send_message('denoising_stuck')

    # ===================== check building imaris file ========================
    print("===================== check building imaris file ========================")

    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE processing_status="denoised" AND modality="rscm" and paused=0'
    ).fetchall()
    print("\nDatasets that have been DENOISED:")
    for dataset_path in records:
        print("-----", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
        if os.path.exists(dataset.full_path_to_imaris_file):
            print("Imaris file exists")
            try:
                # try to open imaris file
                ims_file = ims(dataset.full_path_to_imaris_file)
            except Exception as e:
                log.error(f"ERROR opening imaris file: {e}")
                dataset.send_message("broken_ims_file")
                # dataset.update_db_field('processing_status', 'paused')
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
            print("Building Imaris file in-progress")
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
    print("===================== check moving ========================")

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
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    print("\nDatasets that should be moved:")
    for dataset_path in records:
        print("-----", dataset_path)
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
    print("===================== check paused datasets ========================")
    records = cur.execute(
        'SELECT path_on_fast_store FROM dataset WHERE modality="rscm" and paused=1'
    ).fetchall()
    print("\nDatasets that are in paused status:")
    for dataset_path in records:
        print("-----", dataset_path)
        dataset = RSCMDataset(dataset_path[0])
        guessed_processing_status = dataset.guess_processing_status()
        print("guessed_processing_status:", guessed_processing_status)
        progress_methods_map = {
            "started": dataset.check_stitching_progress,
            "stitched": dataset.check_denoising_progress,
            "denoised": dataset.check_ims_building_progress,
            # "built_ims": dataset.check_finalization_progress
        }
        has_progress = progress_methods_map[guessed_processing_status]()
        print("has progress", has_progress)
        if has_progress:
            dataset.mark_has_processing_progress()
            dataset.update_processing_status(guessed_processing_status)
        print("guessed_processing_status", guessed_processing_status)
        print("dataset.job_dir", dataset.job_dir)
        # print("os.path.exists(dataset.job_dir)", os.path.exists(dataset.job_dir))
        if guessed_processing_status == "finished" and dataset.job_dir and dataset.job_dir.startswith('/CBI_FastStore') and os.path.exists(dataset.job_dir):
            dataset.start_moving()


def check_moving():
    print("Checking MesoSPIM moving")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="finished" AND moved=0'
    ).fetchall()
    for dataset_path in records:
        print('dataset_path', dataset_path[0])
        if dataset_path[0].startswith(MESOSPIM_FASTSTORE_ACQUISITION_FOLDER):
            try:
                dataset = MesoSPIMDataset(dataset_path[0])
            except:
                print("WARNING: Invalid dataset at", dataset_path[0])
                continue
        elif dataset_path[0].startswith(RSCM_FASTSTORE_ACQUISITION_FOLDER):
            dataset = RSCMDataset(dataset_path[0])
        else:
            continue
        if dataset.moving and not dataset.moved:
            dataset.check_if_moved()
        elif not dataset.moved and not dataset.moving:
            dataset.start_moving()
        # elif dataset.moved and dataset.path_on_hive is not None and dataset.processing_status == 'not_started':
        #     dataset.start_processing()
        #     dataset.update_processing_status('in_progress')
        #     dataset.send_message('processing_started')


def check_mesoSPIM_processing():
    print("Checking MesoSPIM processing")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE modality = "mesospim" AND processing_status="in_progress"'
    ).fetchall()
    for dataset_path in records:
        print('dataset_path', dataset_path[0])
        try:
            dataset = MesoSPIMDataset(dataset_path[0])
        except:
            print("WARNING: Invalid dataset at", dataset_path[0])
            continue
        settings_bin_file = sorted(glob(os.path.join(dataset.path_on_fast_store, "*.bin")))
        if len(settings_bin_file):
            settings_bin_file = settings_bin_file[0]
            total_btf_files = get_total_MesoSPIM_tiles(settings_bin_file)
            print(">>>>>>>>>>>>>>>>dataset.refractive_index", dataset.refractive_index)
            if dataset.refractive_index:
                imaris_folder = os.path.join(dataset.path_on_fast_store, 'decon', 'ims_files')
            else:
                imaris_folder = os.path.join(dataset.path_on_fast_store, 'ims_files')
            ims_files = sorted(glob(os.path.join(imaris_folder, '*Tile*_Ch*_Sh*.ims')))
            total_ims_files = len(ims_files)
            channels = dataset.get_total_MesoSPIM_colors_from_file_list()
            if total_ims_files == int(total_btf_files / channels):
                all_ims_files_open = dataset.check_tile_ims_files()
                if all_ims_files_open:
                    montage_files = glob(os.path.join(imaris_folder, '*ontage.ims'))
                    if len(montage_files) > 0:
                        try:
                            ims_file = ims(montage_files[0])
                            dataset.update_processing_status('finished')
                            dataset.send_message('processing_finished')
                            dataset.start_moving()
                        except:
                            print("Still building montage")
                    else:
                        print("Still stitching")
                        dataset.check_auto_stitch()
                else:
                    print("Found broken ims files")
            else:
                print("processing still in progress")
    # TODO message if processing paused


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
    hive = [x for x in beegfs_nodes if x.endswith('h20')][0]
    faststore = [x for x in beegfs_nodes if x.endswith('FastStore')][0]
    faststore_used_percent_str = [x for x in faststore.split() if x.endswith("%")][0]
    hive_used_percent_str = [x for x in hive.split() if x.endswith("%")][0]
    faststore_used_percent = int(faststore_used_percent_str.replace("%", ""))
    hive_used_percent = int(hive_used_percent_str.replace("%", ""))
    check(hive_used_percent, "hive")
    check(faststore_used_percent, "faststore")


def move_files():
    print("Can data be moved now? ", can_be_moved())
    if can_be_moved():
        # move all files from tempQueue to QueueStitch
        files_in_temp_queue = sorted(glob(os.path.join(RSCM_FOLDER_STITCHING, 'tempQueue', '*move.txt')))
        for file in files_in_temp_queue:
            path_in_queue = os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', os.path.basename(file))
            print("moving", file, "to", path_in_queue)
            shutil.move(file, path_in_queue)
    else:
        # move all files from QueueStitch to tempQueue
        files_in_queue = sorted(glob(os.path.join(RSCM_FOLDER_STITCHING, 'queueStitch', '*move.txt')))
        for file in files_in_queue:
            path_in_temp_queue = os.path.join(RSCM_FOLDER_STITCHING, 'tempQueue', os.path.basename(file))
            shutil.move(file, path_in_temp_queue)


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
    print("local_time.hour", local_time.hour)
    if local_time.hour >= 9:
        # update Warning table with active=1
        conn = sqlite3.connect(DB_LOCATION)
        cursor = conn.cursor()
        row_id = cursor.execute("SELECT id FROM warning WHERE type = 'daily_summary'").fetchone()
        print(">>>>>>>>>>>>>>>>>>>>row", row_id)
        res = cursor.execute(f'UPDATE warning SET active = 1 WHERE id={row_id[0]}')
        conn.commit()
        conn.close()
        # query Warning table
        conn = sqlite3.connect(DB_LOCATION)
        cursor = conn.cursor()
        row = cursor.execute("SELECT message_sent, active FROM warning WHERE type = 'daily_summary'").fetchone()
        print(">>>>>>>>>>>>>>>>>>>>row (all)", row)
        conn.close()
        # if daily_summary has message_sent=1: do nothing
        message_sent = int(row[0])
        active = int(row[1])
        print("message_sent", message_sent)
        print("active", active)
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


def scan():
    try:
        check_storage()
        check_RSCM_imaging()
        check_mesoSPIM_imaging()
        check_RSCM_processing()
        check_mesoSPIM_processing()
        move_files()
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
    check_RSCM_imaging()
    check_mesoSPIM_imaging()
    check_RSCM_processing()
    check_mesoSPIM_processing()
    move_files()
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
