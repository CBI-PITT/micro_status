"""
Database schema:

Dataset:
    id
    name
    imaging_status (in-progress, paused, finished)
    processing_status (not_started, started, stitched, copied_to_hive, denoised, built_ims, finished)
    cl_number - ForeignKey to SerialNumber
    path_on_fast_store
    path_on_hive
    vs_series_file - OneToOne to VSSeriesFile
    job_number
    imaris_file_path
    z_layers_total
    z_laers_finished
    ribbons_total
    ribbons_finished
    imaging_no_progress_time (?)
    processing_no_progress_time (?)

VSSeriesFile:
    id
    path

PI:
    id
    name
    public_folfer_name

CLNumber:
    id
    pi = ForeignKey to PI

-------------------------------------
Messages:
    1) imaging started
    2) imaging paused (crashed?)
    2a) imaging resumed
    3) imaging finished
    4) processing started
    5) processing_paused (crashed?)
    6) processing finished


pip install python-dotenv
"""

from datetime import datetime
import json
import os
import requests

from dotenv import load_dotenv


FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire"
HIVE_ACQUISITION_FOLDER = "/CBI_Hive/Acquire"
DB_LOCATION = ""

SLACK_URL = "https://slack.com/api/chat.postMessage"
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL")
load_dotenv()
SLACK_HEADERS = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {os.getenv("SLACK_TOKEN")}'}


def check_if_new(file_path):
    pass


def create_dataset_record(file_path):
    pass


def read_dataset_record(file_path):
    pass


class Dataset:
    def __init__(self, **kwargs):
        self.imaging_no_progress_time = kwargs.get('imaging_no_progress_time')

    def check_imaging_progress(self):
        pass

    def check_imaging_finished(self):
        pass

    def send_message(self, msg_type):
        msg_text = "Test msg using *_requests_*"
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
        response = requests.post(SLACK_URL, data=json.dumps(payload), headers=SLACK_HEADERS)
        return response

    def mark_no_imaging_progress(self):
        pass

    def mark_has_imaging_progress(self):
        pass

    def mark_imaging_finished(self):
        pass


def check_imaging():
    # Discover all vs_series.dat files in the acquisition directory
    vs_series_files = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith("vs_series.dat"):
                vs_series_files.append(os.path.join(root, file))

    for file_path in vs_series_files:
        is_new = check_if_new(file_path)
        if is_new:
            dataset = create_dataset_record(file_path)
            response = dataset.send_message('imaging_started')
        else:
            dataset = read_dataset_record(file_path)
            if dataset.imaging_status == 'finished':
                continue
            elif dataset.imaging_status == 'in-progress':
                got_finished = dataset.check_imaging_finished()
                if got_finished:
                    dataset.mark_imaging_finished()
                    response = dataset.send_message('imaging_finished')
                    continue
                has_progress = dataset.check_imaging_progress()
                if has_progress:
                    continue
                else:
                    if not dataset.imaging_no_progress_time:
                        dataset.mark_no_imaging_progress()
                    else:
                        if datetime.now() - dataset.imaging_no_progress_time > 300:  # no progress for 5 minutes
                            response = dataset.send_message('imaging_paused')
            elif dataset.imaging_status == 'paused':
                has_progress = dataset.check_imaging_progress()  # maybe imaging resumed
                if not has_progress:
                    continue
                else:
                    dataset.mark_has_imaging_progress()
                    response = dataset.send_message('imaging_resumed')


def check_processing():
    # get all datasets, with processing other than finished
    # check if they are on the same stage or moved to the next stage
    # check if it is stuck
    pass


def scan():
    check_imaging()
    check_processing()


if __name__ == "__main__":
    while True:
        scan()