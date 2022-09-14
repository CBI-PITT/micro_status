import os
import re
import sqlite3
from glob import glob
from pathlib import Path

from bs4 import BeautifulSoup

"""

CREATE TABLE `clnumber` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT NOT NULL UNIQUE,
	`pi`	INTEGER,
	FOREIGN KEY(`pi`) REFERENCES pi(id) ON DELETE SET NULL
)

CREATE TABLE "pi" (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT NOT NULL,
	`public_folder_name`	TEXT
)

CREATE TABLE "vsseriesfile" (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`path`	TEXT NOT NULL UNIQUE
)

CREATE TABLE `dataset` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`name`	TEXT,
	`path_on_fast_store`	TEXT,
	`vs_series_file`	INTEGER,
	`cl_number`	INTEGER,
	`pi`	INTEGER,
	`imaging_status`	TEXT NOT NULL DEFAULT 'in_progress',
	`processing_status`	TEXT NOT NULL DEFAULT 'not_started',
	`path_on_hive`	TEXT,
	`job_number`	TEXT,
	`imaris_file_path`	TEXT,
	`channels`	INTEGER NOT NULL DEFAULT 1,
	`z_layers_total`	INTEGER NOT NULL,
	`z_layers_current`	INTEGER,
	`ribbons_total`	INTEGER NOT NULL,
	`ribbons_finished`	INTEGER,
	`imaging_no_progress_time`	TEXT,
	`processing_no_progress_time`	TEXT,
	FOREIGN KEY(`vs_series_file`) REFERENCES vsseriesfile(id) ON DELETE SET NULL,
	FOREIGN KEY(`cl_number`) REFERENCES clnumber(id) ON DELETE SET NULL,
	FOREIGN KEY(`pi`) REFERENCES pi(id) ON DELETE SET NULL
);
CREATE TABLE `warning` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`type`	TEXT NOT NULL,
	`message_sent`	INTEGER DEFAULT 0,
	`active`	INTEGER DEFAULT 1
);
"""


FASTSTORE_ACQUISITION_FOLDER = "/CBI_FastStore/Acquire"
DB_FILE = '/CBI_Hive/CBI/Iana/projects/internal/RSCM_datasets'

# Discover all vs_series.dat files in the acquisition directory
print("Looking for vs_series files...")
vs_series_files = []
for root, dirs, files in os.walk(FASTSTORE_ACQUISITION_FOLDER):
    for file in files:
        if file.endswith("vs_series.dat"):
            file_path = Path(os.path.join(root, file))
            if 'stack' in str(file_path.parent.name):
                vs_series_files.append(str(file_path))

print("Found files:", len(vs_series_files))

for file_path in vs_series_files:
    print("Processing", file_path)
    try:
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        res = cur.execute(f'SELECT id FROM vsseriesfile WHERE path = "{file_path}"')
        vs_series_file_id = res.fetchone()
        con.close()

        if vs_series_file_id:
            vs_series_file_id = vs_series_file_id[0]
        else:
            con = sqlite3.connect(DB_FILE)
            cur = con.cursor()
            res = cur.execute(f'INSERT OR IGNORE INTO vsseriesfile(path) VALUES("{file_path}")')
            vs_series_file_id = cur.lastrowid
            con.commit()
            con.close()

        file_path = Path(file_path)
        path_parts = file_path.parts
        pi_name = path_parts[3] if path_parts[3].isalpha() else None
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        res = cur.execute(f'SELECT id FROM pi WHERE name = "{pi_name}"')
        pi_id = res.fetchone()
        con.close()

        if pi_id:
            pi_id = pi_id[0]
        else:
            con = sqlite3.connect(DB_FILE)
            cur = con.cursor()
            res = cur.execute(f'INSERT OR IGNORE INTO pi(name) VALUES("{pi_name}")')
            pi_id = cur.lastrowid
            con.commit()
            con.close()

        cl_number = [x for x in path_parts if 'CL' in x.upper()]
        cl_number = None if len(cl_number) == 0 else cl_number[0]
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        res = cur.execute(f'SELECT id FROM clnumber WHERE name = "{cl_number}"')
        cl_number_id = res.fetchone()
        con.close()

        if cl_number_id:
            cl_number_id = cl_number_id[0]
        else:
            con = sqlite3.connect(DB_FILE)
            cur = con.cursor()
            res = cur.execute(f'INSERT OR IGNORE INTO clnumber(name, pi) VALUES("{cl_number}", {pi_id})')
            cl_number_id = cur.lastrowid
            con.commit()
            con.close()

        dataset_name = file_path.parent.name if "stack" in file_path.parent.name else None

        print("Path", file_path, "pi_name", pi_name, "cl_number", cl_number, "dataset_name", dataset_name)

        with open(file_path, 'r') as f:
            data = f.read()

        soup = BeautifulSoup(data, "xml")
        z_layers = int(soup.find('stack_slice_count').text)
        ribbons_in_z_layer = int(soup.find('grid_cols').text)
        # layer_dirs = [x.path for x in os.scandir(file_path.parent) if x.is_dir()]
        # for layer in range(int(z_layers) -1, 0, -1):

        ribbons_finished = 0
        subdirs = os.scandir(file_path.parent)
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
        current_z_layer = re.findall(r"\d+", subdir.name)[-1]
        imaging_status = "finished" if current_z_layer == z_layers else "in_progress"
        ribbons_total = z_layers * channels * ribbons_in_z_layer

        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        res = cur.execute(
            f'''INSERT OR IGNORE INTO dataset(name, path_on_fast_store, vs_series_file, cl_number, pi, 
    imaging_status, processing_status, channels, z_layers_total, z_layers_current, ribbons_total, ribbons_finished) 
    VALUES("{dataset_name}", "{file_path}", "{vs_series_file_id}", "{cl_number_id}", "{pi_id}", 
    "in_progress", "not_started", "{channels}", "{z_layers}", "{current_z_layer}", "{ribbons_total}", "{ribbons_finished}")
    '''
        )
        con.commit()
        con.close()
    except Exception as e:
        print("WARNING: ", e)
