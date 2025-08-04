### Run on slogin
- ssh lab@slogin.cbiserver.pitt.edu <br/>
- conda activate microstatus <br/>
- cd /h20/CBI/Iana/src/micro_status <br/>
- python check_status.py <br/>

### Setup from scratch (new instance)
- Clone the repo <br/>
- Copy .env file from a running instance to the folder with the code - it has the API key for slack <br/>
- Create a new database with python create_db.py (or use an existing one) <br/>
- Run python check_status.py <br/>



The database is at /CBI_FastStore/Iana/RSCM_MesoSPIM_datasets.db <br/>
Backups of the database are created every night and stored at /CBI_FastStore/Iana/db_backups <br/>
Logs are at /CBI_FastStore/Iana/bot_logs <br/>


The values in the database tables can also be modified through a web interface (see micro_status_flask repo)
