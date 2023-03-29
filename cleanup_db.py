# conda activate microstatus

# Removes datasets with names containing "demo"

import sqlite3

from micro_status.settings import DB_LOCATION


query = "SELECT COUNT(*) FROM dataset"
con = sqlite3.connect(DB_LOCATION)
cur = con.cursor()
res = cur.execute(query)
count = res.fetchone()
con.close()
print("Total datasets before", count)

query = "DELETE FROM dataset WHERE name LIKE '%demo%';"
con = sqlite3.connect(DB_LOCATION)
cur = con.cursor()
cur.execute(query)
con.commit()
con.close()
print("Deleted demo datasets")

query = "SELECT COUNT(*) FROM dataset"
con = sqlite3.connect(DB_LOCATION)
cur = con.cursor()
res = cur.execute(query)
count = res.fetchone()
con.close()
print("Total datasets after", count)
