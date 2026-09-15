import sqlite3

# Specify the path and name of the database file
db_file = "/CBI_FastStore/Iana/RSCM_MesoSPIM_datasets.db"  # File will be created in the current working directory

# Connect to the database (this creates the file if it doesn't exist)
connection = sqlite3.connect(db_file)

# try:
#     # Create a cursor object to execute SQL commands
#     cursor = connection.cursor()
#
#     # Define the SQL command to create the table
#     create_table_query = """
# CREATE TABLE IF NOT EXISTS "pi" (
# id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
# name TEXT NOT NULL,
# public_folder_name TEXT
# )
#     """
#
#     # Execute the SQL command
#     cursor.execute(create_table_query)
#     print("Table 'pi' created successfully.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection to the database
#     connection.close()
#
#
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Define the SQL command to create the new table
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS `clnumber` (
#         `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
#         `name` TEXT NOT NULL UNIQUE,
#         `pi` INTEGER,
#         FOREIGN KEY(`pi`) REFERENCES pi(id) ON DELETE SET NULL
#     )
#     """
#
#     # Execute the SQL command
#     cursor.execute(create_table_query)
#     print("Table 'clnumber' added successfully.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()
#
#
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Enable foreign key support
#     cursor.execute("PRAGMA foreign_keys = ON;")
#
#     # Define the SQL command to create the `dataset` table
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS "dataset" (
#         `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
#         `name` TEXT,
#         `path_on_fast_store` TEXT,
#         `cl_number` INTEGER,
#         `pi` INTEGER,
#         `imaging_status` TEXT NOT NULL DEFAULT 'in_progress',
#         `processing_status` TEXT NOT NULL DEFAULT 'not_started',
#         `path_on_hive` TEXT,
#         `job_number` TEXT,
#         `imaris_file_path` TEXT,
#         `channels` INTEGER NOT NULL DEFAULT 1,
#         `z_layers_total` INTEGER,
#         `z_layers_current` INTEGER,
#         `ribbons_total` INTEGER,
#         `ribbons_finished` INTEGER,
#         `tiles_total` INTEGER,
#         `tiles_finished` INTEGER,
#         `tiles_x` INTEGER,
#         `tiles_y` INTEGER,
#         `resolution_xy` TEXT,
#         `resolution_z` TEXT,
#         `imaging_no_progress_time` TEXT,
#         `processing_no_progress_time` TEXT,
#         `processing_summary` TEXT,
#         `z_layers_checked` INTEGER,
#         `keep_composites` INTEGER DEFAULT 0,
#         `delete_405` INTEGER DEFAULT 0,
#         `created` TEXT DEFAULT NULL,
#         `modality` TEXT DEFAULT NULL,
#         `is_brain` INTEGER DEFAULT 0,
#         `peace_json_created` INTEGER DEFAULT 0,
#         `imaging_summary` TEXT,
#         `moved` INTEGER DEFAULT 0,
#         `moving` INTEGER DEFAULT 0,
#         `paused` INTEGER DEFAULT 0,
#         FOREIGN KEY(`cl_number`) REFERENCES clnumber (id) ON DELETE SET NULL,
#         FOREIGN KEY(`pi`) REFERENCES pi (id) ON DELETE SET NULL
#     )
#     """
#
#     # Execute the SQL command
#     cursor.execute(create_table_query)
#     print("Table 'dataset' added successfully.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()
#
#
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Define the SQL command to create the `warning` table
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS `warning` (
#         `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
#         `type` TEXT NOT NULL,
#         `message_sent` INTEGER DEFAULT 0,
#         `active` INTEGER DEFAULT 1
#     )
#     """
#
#     # Execute the SQL command
#     cursor.execute(create_table_query)
#     print("Table 'warning' added successfully.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()



# ### Copy table
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Enable foreign key support
#     cursor.execute("PRAGMA foreign_keys = ON;")
#
#     # Define the SQL command to create the `dataset` table
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS "dataset_copy" (
#         `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
#         `name` TEXT,
#         `path_on_fast_store` TEXT,
#         `cl_number` INTEGER,
#         `pi` INTEGER,
#         `imaging_status` TEXT NOT NULL DEFAULT 'in_progress',
#         `processing_status` TEXT NOT NULL DEFAULT 'not_started',
#         `path_on_hive` TEXT,
#         `job_number` TEXT,
#         `imaris_file_path` TEXT,
#         `channels` INTEGER NOT NULL DEFAULT 1,
#         `z_layers_total` INTEGER,
#         `z_layers_current` INTEGER,
#         `ribbons_total` INTEGER,
#         `ribbons_finished` INTEGER,
#         `tiles_total` INTEGER,
#         `tiles_finished` INTEGER,
#         `tiles_x` INTEGER,
#         `tiles_y` INTEGER,
#         `resolution_xy` TEXT,
#         `resolution_z` TEXT,
#         `imaging_no_progress_time` TEXT,
#         `processing_no_progress_time` TEXT,
#         `processing_summary` TEXT,
#         `z_layers_checked` INTEGER,
#         `keep_composites` INTEGER DEFAULT 0,
#         `delete_405` INTEGER DEFAULT 0,
#         `created` TEXT DEFAULT NULL,
#         `modality` TEXT DEFAULT NULL,
#         `is_brain` INTEGER DEFAULT 0,
#         `peace_json_created` INTEGER DEFAULT 0,
#         `imaging_summary` TEXT,
#         `moved` INTEGER DEFAULT 0,
#         `moving` INTEGER DEFAULT 0,
#         `paused` INTEGER DEFAULT 0,
#         FOREIGN KEY(`cl_number`) REFERENCES clnumber (id) ON DELETE SET NULL,
#         FOREIGN KEY(`pi`) REFERENCES pi (id) ON DELETE SET NULL
#     )
#     """
#
#     # Execute the SQL command
#     cursor.execute(create_table_query)
#     print("New table 'dataset_copy' added successfully.")
#
#     copy_query = """
#     INSERT INTO dataset_copy (name, path_on_fast_store, cl_number, pi, imaging_status, processing_status, channels, created)
#     SELECT name, path_on_fast_store, cl_number, pi, imaging_status, processing_status, channels, created  FROM dataset;
#     """
#     cursor.execute(copy_query)
#     print("Data copied successfully.")
#
#     delete_query = """
#     DROP TABLE dataset;
#     """
#     cursor.execute(delete_query)
#     print("Old table 'dataset' deleted successfully.")
#
#     rename_query = """
#     ALTER TABLE dataset_copy RENAME TO dataset;
#     """
#     cursor.execute(rename_query)
#     print("Table 'dataset_copy' renamed to 'dataset'")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()


# ### Add scheduler posting columns (run once, then re-comment)
# ### ALTER TABLE dataset ADD COLUMN imaging_start TEXT
# ### ALTER TABLE dataset ADD COLUMN imaging_end TEXT
# ### ALTER TABLE dataset ADD COLUMN scheduler_posted INTEGER DEFAULT 0
# ### ALTER TABLE dataset ADD COLUMN scheduler_record_id TEXT
# ### ALTER TABLE dataset ADD COLUMN scheduler_notified INTEGER DEFAULT 0
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Define the SQL commands to add the columns
#     add_column_queries = """
#     ALTER TABLE dataset ADD COLUMN imaging_start TEXT;
#     ALTER TABLE dataset ADD COLUMN imaging_end TEXT;
#     ALTER TABLE dataset ADD COLUMN scheduler_posted INTEGER DEFAULT 0;
#     ALTER TABLE dataset ADD COLUMN scheduler_record_id TEXT;
#     ALTER TABLE dataset ADD COLUMN scheduler_notified INTEGER DEFAULT 0;
#     """
#     # Execute the SQL commands
#     cursor.executescript(add_column_queries)
#     print("Columns 'imaging_start', 'imaging_end', 'scheduler_posted', 'scheduler_record_id', 'scheduler_notified' added to the 'dataset' table.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()


# ### Add instrument_id column (run once, then re-comment)
# ### ALTER TABLE dataset ADD COLUMN instrument_id TEXT
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Define the SQL command to add the column
#     add_column_query = """
#     ALTER TABLE dataset
#     ADD COLUMN instrument_id TEXT
#     """
#     # Execute the SQL command
#     cursor.execute(add_column_query)
#     print("Column 'instrument_id' added successfully to the 'dataset' table.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()


# ### Add columns to table
# try:
#     # Create a cursor object
#     cursor = connection.cursor()
#
#     # Define the SQL command to add the column
#     add_column_query = """
#     ALTER TABLE dataset
#     ADD COLUMN is_brain INTEGER DEFAULT 0
#     """
#     # Execute the SQL command
#     cursor.execute(add_column_query)
#     print("Column 'is_brain' added successfully to the 'dataset' table.")
#
#     # Define the SQL command to add the column
#     add_column_query = """
#     ALTER TABLE dataset
#     ADD COLUMN peace_json_created INTEGER DEFAULT 0
#     """
#     # Execute the SQL command
#     cursor.execute(add_column_query)
#     print("Column 'peace_json_created' added successfully to the 'dataset' table.")
#
#     # Define the SQL command to add the column
#     add_column_query = """
#     ALTER TABLE dataset
#     ADD COLUMN imaging_summary TEXT
#     """
#     # Execute the SQL command
#     cursor.execute(add_column_query)
#     print("Column 'imaging_summary' added successfully to the 'dataset' table.")
#
# except sqlite3.Error as e:
#     print(f"An error occurred: {e}")
# finally:
#     # Close the connection
#     connection.close()
