## Data Warehouse and ETL with Redshift

### Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis. In this project, we create a database schema and ETL pipeline for this analysis. We defined fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL. At end, we tested our database and ETL pipeline by running queries given by the analytics team from Sparkify and compare the results with their expected results.

### database schema design and ETL pipeline
##### *Fact Table*
1. songplays - records in log data associated with song plays i.e. records with page NextSong
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### *Dimension Tables*
2. users - users in the app
  - user_id, first_name, last_name, gender, level
3. songs - songs in music database
  - song_id, title, artist_id, year, duration
4. artists - artists in music database
  - artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

### About the scripts
- **sql_queries.py**: contains all your sql queries, and is imported into the first three two below.
- **create_tables.py**: drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
- **etl.py**: reads and processes files from song_data and log_data and loads them into your tables. This is filled out based on the work in the ETL notebook.
- **test.ipynb**: displays the first few rows of each table to check the database.
- **Sparkify_Redshift_ETL.ipynb**: ETL processes including Redshift cluster deployment.
- **dwh.cfg**: DWH Params

**To run the script,** open a termial and run the following commands in order if you have Redshift cluster available. (Modify the dwh.cfg file before running ) It will initial the databases and create the ETL pipeline. 

    python create_tables.py 
    python etl.py
**If you don't have Redshift cluster launched**. Just go through the Sparkify_Redshift_ETL.ipynb. 

Run ***test.ipynb*** to see if the data has beed loaded successfully. <br>
