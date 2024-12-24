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
    - extract tiff series after ims file is on hive
    - do alignment, spot counting etc?
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
            if "demo" in dataset.name:
                # demo dataset
                log.info(f"Ignoring demo dataset {dataset}")
                print(f"Ignoring demo dataset {dataset}")
                dataset.send_message('ignoring_demo_dataset')
                dataset.mark_imaging_finished()
                dataset.update_processing_status('finished')
                continue
            dataset.send_message('imaging_started')
        else:
            dataset = RSCMDataset(file_path)
            if dataset.imaging_status == 'in_progress':
                print("Imaging status is 'in-progress'")
                got_finished, has_progress, error_flag = dataset.check_imaging_progress()
                if error_flag:
                    dataset.mark_paused()
                    dataset.send_message('broken_tiff_file')
                    continue
                print("Imaging finished:", got_finished)
                if got_finished:
                    dataset.mark_imaging_finished()
                    dataset.send_message('imaging_finished')
                    if dataset.delete_405:
                        print("------------Deleting 405 channel")
                        dataset.delete_channel_405()
                    if '_cont_' not in dataset.name:
                        dataset.start_processing()
                    continue
                print("Imaging has progress:", has_progress)
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
        print("Working on: ", file_path)
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
                continue
            dataset.send_message('imaging_started')
        dataset = MesoSPIMDataset(file_path)
        # check whether imaging finished or paused
        if dataset.imaging_status == 'in_progress':
            dataset.check_imaging_progress()
        elif dataset.imaging_status == "finished" and dataset.processing_status == 'not_started':
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


def check_RSCM_processing():
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT * FROM dataset WHERE processing_status="not_started" AND imaging_status="finished"'
    ).fetchall()
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        if dataset.check_being_stitched():
            dataset.update_processing_status('started')
            dataset.send_message('processing_started')

    # check if they are on the same stage or moved to the next stage
    # check if it is stuck

    # =========================  check stitching  ============================

    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="started"'
    ).fetchall()
    print("\nDataset instances where stitching started:")
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("-----", dataset)
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
            dataset.update_processing_status('paused')
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
                        dataset.update_processing_status('paused')
                        dataset.send_message('stitching_stuck')
        else:
            print("File in none of ClusterStitchTest dirs")

    # ====================  check denoising =====================

    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="stitched"'
    ).fetchall()
    print("\nDataset instances that have been stitched:")
    # for record in records:
    #     dataset = Dataset.initialize_from_db(record)
    #     all_ribbons_present = dataset.check_all_ribbons_present()
    #     all_raw_composites_present = dataset.check_all_raw_composites_present()
    #     denoising_started = dataset.check_denoising_started()
    #     if all_ribbons_present and all_raw_composites_present and denoising_started and not os.path.exists(
    #         dataset.path_on_fast_store
    #     ):
    #         dataset.update_processing_status('moved_to_hive')
    #     else:
    #         dataset.check_moving_to_hive_progress()
    #     # TODO: check all files are there
    #     # TODO: check folder from FastStore got deleted
    #     # TODO: check denoising started
    #     # TODO: otherwise, check progress
    #     # TODO: Update db to "moved_to_hive" status
    #
    # # check denoising
    # records = cur.execute(
    #     'SELECT * FROM dataset WHERE processing_status="moved_to_hive"'
    # ).fetchall()

    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("-----", dataset)
        if dataset.job_dir:
            print("Job dir is there")
            job_number = re.findall(r"\d+", os.path.basename(dataset.job_dir))[-1]
            dataset.update_job_number(job_number)
            denoising_started = len(glob(os.path.join(dataset.job_dir, "composite*.tif"))) > 0
            print("Denoising started:", denoising_started)
            # print("Job dir", dataset.job_dir)
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
                            dataset.update_processing_status('paused')
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
            if denoising_finished and can_be_moved():
                dataset.update_processing_status('denoised')
                dataset.clean_up_raw_composites()
                dataset.start_moving()
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
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="denoised"'
    ).fetchall()
    print("\nDataset instances that have been denoised:")
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("-----", dataset)
        if os.path.exists(dataset.full_path_to_imaris_file):
            print("Imaris file exists")
            try:
                # try to open imaris file
                ims_file = ims(dataset.full_path_to_imaris_file)
            except Exception as e:
                log.error(f"ERROR opening imaris file: {e}")
                dataset.send_message("broken_ims_file")
                dataset.update_processing_status('paused')
                dataset.requeue_ims()

                # update ims_size=0 in processing_summary
                processing_summary = dataset.get_processing_summary()
                value_from_db = processing_summary.get('building_ims')
                if value_from_db:
                    value_from_db.update({'ims_size': 0})
                    dataset.update_processing_summary({'building_ims': value_from_db})
                continue
            else:
                dataset.update_processing_status('built_ims')
                dataset.send_message('built_ims')
                if not dataset.keep_composites:
                    dataset.clean_up_denoised_composites()
                # dataset.start_moving()
        # elif os.path.exists(dataset.full_path_to_ims_part_file) and os.path.exists(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', dataset.imsqueue_file_name)):
        elif os.path.exists(dataset.full_path_to_ims_part_file) and len(glob(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'processing', f"*{dataset.job_number}*.txt.imsqueue"))):
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
                        dataset.update_processing_status('paused')
                        # dataset.send_message('ims_build_stuck')
                        #dataset.requeue_ims()
                        #dataset.send_message('requeue_ims')
        else:
            # ims file is not being built
            print("Imaris file is not being built")
            # in_queue = os.path.exists(os.path.join(RSCM_FOLDER_BUILDING_IMS, 'queueIMS', dataset.imsqueue_file_name))
            if dataset.in_imaris_queue:
                if dataset.processing_no_progress_time:
                    dataset.mark_has_processing_progress()
                # continue
            else:
                if not dataset.processing_no_progress_time:
                    dataset.mark_no_processing_progress()
                else:
                    progress_stopped_at = datetime.strptime(dataset.processing_no_progress_time, DATETIME_FORMAT)
                    if (datetime.now() - progress_stopped_at).total_seconds() > PROGRESS_TIMEOUT:
                        dataset.update_processing_status('paused')
                        # dataset.send_message('ims_build_stuck')

            # check what other file is being processed, check its size
            ims_converter_works = dataset.check_ims_converter_works()
            if ims_converter_works:
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
                        # dataset.send_message('ims_build_stuck')

    # Eventually datasets should be on hive
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="built_ims"'
    ).fetchall()
    for record in records:
        dataset = Dataset.initialize_from_db(record)
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
                dataset.update_processing_status('finished')
                dataset.send_message("processing_finished")

    # ==================== Handle 'paused' processing status ==================
    records = cur.execute(
        'SELECT * FROM dataset WHERE processing_status="paused"'
    ).fetchall()
    print("\nDatasets that are in paused status:")
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("-----", dataset)
        # TODO: see what stage processing is in
        guessed_processing_status = dataset.guess_processing_status()
        print("guessed_processing_status:", guessed_processing_status)
        # TODO: see if there's any progress at this stage
        progress_methods_map = {
            "started": dataset.check_stitching_progress,
            "stitched": dataset.check_denoising_progress,
            "denoised": dataset.check_ims_building_progress,
            "built_ims": dataset.check_finalization_progress
        }
        has_progress = progress_methods_map[guessed_processing_status]()
        print("has progress", has_progress)
        if has_progress:
            dataset.mark_has_processing_progress()
            dataset.update_processing_status(guessed_processing_status)
        print("guessed_processing_status", guessed_processing_status)
        print("dataset.job_dir", dataset.job_dir)
        print("os.path.exists(dataset.job_dir)", os.path.exists(dataset.job_dir))
        if guessed_processing_status == "denoised" and dataset.job_dir.startswith('/CBI_FastStore') and os.path.exists(dataset.job_dir):
            dataset.start_moving()


def check_mesoSPIM_processing():
    print("Checking MesoSPIM processing")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT path_on_fast_store FROM dataset WHERE processing_status="in_progress"'
    ).fetchall()
    for dataset_path in records:
        print('dataset_path', dataset_path[0])
        dataset = MesoSPIMDataset(dataset_path[0])
        settings_bin_file = sorted(glob(os.path.join(dataset.path_on_fast_store, "*.bin")))
        if len(settings_bin_file):
            settings_bin_file = settings_bin_file[0]
            total_btf_files = get_total_MesoSPIM_tiles(settings_bin_file)
            total_ims_files = len(glob(os.path.join(dataset.path_on_fast_store, 'ims_files', '*.ims')))
            if total_ims_files == int(total_btf_files / dataset.channels):
                dataset.update_processing_status('finished')
                dataset.send_message('processing_finished')
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


def check_analysis():
    """
    If the finished dataset is a brain, send it for analysis by PEACE pipeline
    """
    print("Checking analysis...")
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    records = cur.execute(
        f'SELECT * FROM dataset WHERE processing_status="finished" AND is_brain=1'
    ).fetchall()
    for record in records:
        dataset = Dataset.initialize_from_db(record)
        print("Brain dataset", dataset)
        if not dataset.peace_json_created:
            dataset.create_peace_json()


def scan():
    try:
        check_storage()
        check_RSCM_imaging()
        check_mesoSPIM_imaging()
        check_RSCM_processing()
        check_mesoSPIM_processing()
        # TODO db_backup()
        check_analysis()
    except Exception as e:
        log.error(f"\nEXCEPTION: {e}\n")
        print(traceback.format_exc())

    print("========================== Waiting 30 seconds ========================")
    time.sleep(30)


def scan_debug():
    check_storage()
    # check_RSCM_imaging()
    check_mesoSPIM_imaging()
    # check_RSCM_processing()
    check_mesoSPIM_processing()
    # TODO db_backup()
    # check_analysis()
    time.sleep(30)


if __name__ == "__main__":
    while True:
        scan_debug()
