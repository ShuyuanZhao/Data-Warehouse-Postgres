import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','arn')

LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
log_jsonpath = config.get('S3','log_jsonpath')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_tb"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_tb"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_tb
(artist varchar(255),
auth varchar(20),
firstName varchar(255),
gender varchar(1),
itemInSession integer,
lastName varchar(255),
length numeric(20,5),
level varchar(10),
location varchar(500),
method varchar(10),
page varchar(20),
registration bigint,
sessionId integer,
song varchar(255),
status integer,
ts bigint,
userAgent varchar(500),
userId integer);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_tb
(artist_id varchar(20),
artist_latitude  numeric(20,5),
artist_location varchar(255),
artist_longitudes  numeric(20,5),
artist_name varchar(255),
duration numeric(20,5),
num_songs integer,
song_id varchar(20),
title varchar(255),
year integer);
""")

songplay_table_create = ("""
CREATE TABLE songplays
(songplay_id integer identity(0, 1) PRIMARY KEY,
start_time timestamp NOT NULL REFERENCES time (start_time) sortkey,
user_id integer NOT NULL REFERENCES users (user_id),
song_id varchar(20) NOT NULL REFERENCES songs (song_id) distkey,
artist_id varchar(20) NOT NULL REFERENCES artists (artist_id),
session_id varchar(20),
location varchar(255),
level varchar(20),
user_agent varchar(200));
""")

user_table_create = ("""
CREATE TABLE users
(user_id integer PRIMARY KEY,
first_name varchar(255),
last_name varchar(255),
gender varchar(1),
level varchar(10));
""")

song_table_create = ("""
CREATE TABLE songs
(song_id varchar(20) PRIMARY KEY,
title varchar(255),
artist_id varchar(50),
year integer,
duration decimal(20,10));
""")

artist_table_create = ("""
CREATE TABLE artists
(artist_id varchar(20) PRIMARY KEY,
name varchar(255),
location varchar(200),
latitude  decimal(20,10),
longitude decimal(20,10));
""")

time_table_create = ("""
CREATE TABLE time
(start_time timestamp PRIMARY KEY sortkey,
HOUR smallint,
DAY smallint,
WEEK smallint,
MONTH smallint,
YEAR smallint,
weekday smallint);
""")

# STAGING TABLES
staging_events_copy = ("""
copy staging_events_tb from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json {};
""").format(LOG_DATA, DWH_ROLE_ARN,log_jsonpath)

staging_songs_copy = ("""
copy staging_songs_tb from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES
songplay_table_insert = ("""
insert into songplays (start_time,user_id,song_id,
artist_id,session_id,location,level,user_agent)
(select TIMESTAMP 'epoch' + a.ts/1000 *INTERVAL '1 second' as start_time,
a.userId, b.song_id, c.artist_id, a.sessionId, a.location, a.level, a.userAgent
from staging_events_tb a
join songs b on a.song = b.title
join artists c on a.artist = c.name
where a.page = 'NextSong'
and a.userId is not null
and a.ts is not null);
""")

user_table_insert = ("""
insert into users
(select userId, firstName, lastName, gender, level
from staging_events_tb where userId is not null
and page='NextSong'
group by 1,2,3,4,5);
""")

song_table_insert = ("""
insert into songs
(select song_id, title, artist_id, year, duration
from staging_songs_tb where song_id is not null
group by 1,2,3,4,5
);
""")

artist_table_insert = ("""
insert into artists
(select artist_id, artist_name, artist_location,
artist_latitude, artist_longitudes
from staging_songs_tb where artist_id is not null
group by 1,2,3,4,5);
""")

time_table_insert = ("""
insert into time
(select start_time,
extract(hour from start_time) as hour,
extract(day from start_time) as day,
extract(week from start_time) as week,
extract(month from start_time) as month,
extract(year from start_time) as year,
extract(weekday from start_time) as weekday
from songplays
)
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create,]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy,staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert,time_table_insert]
