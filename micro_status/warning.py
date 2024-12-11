import json
import logging
import os
import requests
import sqlite3

from micro_status.settings import *

log = logging.getLogger(__name__)


class Warning:
    def __init__(self, **kwargs):
        self.type = kwargs.get('type')
        self.active = kwargs.get('active', True)
        self.message_sent = kwargs.get('message_sent', False)
        self.db_id = kwargs.get('db_id')

    @classmethod
    def create(cls, warning_type):
        warning = cls(type=warning_type)
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'INSERT OR IGNORE INTO warning(type) VALUES("{warning_type}")')
        warning.db_id = cur.lastrowid
        con.commit()
        con.close()
        return warning

    @classmethod
    def get_from_db(cls, warning_type):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'SELECT * FROM warning WHERE type = "{warning_type}"').fetchone()
        con.close()
        if not res:
            return
        warning = cls(
            db_id=res[0],
            type=res[1],
            message_sent=res[2],
            active=res[3]
        )
        return warning

    def send_message(self):
        msg_map = {
            'low_space_hive': f":exclamation: *WARNING: Critically low space on Hive (more than {MAX_ALLOWED_STORAGE_PERCENT}% used)*",
            'low_space_h20': f":exclamation: *WARNING: Critically low space on h20 (more than {MAX_ALLOWED_STORAGE_PERCENT}% used)*",
            'low_space_faststore': f":exclamation: *WARNING: Critically low space on FastStore (more than {MAX_ALLOWED_STORAGE_PERCENT}% used)*",
            'space_hive_thr0': f":exclamation: *WARNING: Low space on Hive (more than {STORAGE_THRESHOLD_0}% used)*",
            'space_h20_thr0': f":exclamation: *WARNING: Low space on h20 (more than {STORAGE_THRESHOLD_0}% used)*",
            'space_faststore_thr0': f":exclamation: *WARNING: Low space on FastStore (more than {STORAGE_THRESHOLD_0}% used)*",
            'space_hive_thr1': f":exclamation: *WARNING: Low space on Hive (more than {STORAGE_THRESHOLD_1}% used)*",
            'space_h20_thr1': f":exclamation: *WARNING: Low space on h20 (more than {STORAGE_THRESHOLD_1}% used)*",
            'space_faststore_thr1': f":exclamation: *WARNING: Low space on FastStore (more than {STORAGE_THRESHOLD_1}% used)*",
        }
        msg_text = msg_map[self.type]
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
        # update db
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET message_sent = 1 WHERE id={self.db_id}')
        con.commit()
        con.close()
        return response

    def mark_as_active(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET active = 1 WHERE id={self.db_id}')
        con.commit()
        con.close()

    def mark_as_inactive(self):
        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET active = 0 WHERE id={self.db_id}')
        con.commit()
        con.close()

        con = sqlite3.connect(DB_LOCATION)
        cur = con.cursor()
        res = cur.execute(f'UPDATE warning SET message_sent = 0 WHERE id={self.db_id}')
        con.commit()
        con.close()