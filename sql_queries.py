import configparser 

#config
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')                   
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
ARN = config.get('IAM_ROLE','ARN')
USER_LOC = config.get('LOC','REGION')


#Drop table queries
staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"


#create table queries
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR NULL,
auth VARCHAR NULL,
firstName VARCHAR NULL,
gender VARCHAR NULL,
itemInSession VARCHAR NULL,
lastName VARCHAR NULL,
length VARCHAR NULL,
level VARCHAR NULL,
location VARCHAR NULL,
method VARCHAR NULL,
page VARCHAR NULL,
registration VARCHAR NULL,
sessionId INTEGER NOT NULL SORTKEY DISTKEY,
song VARCHAR NULL,
status INTEGER NULL,
ts BIGINT NOT NULL,
userAgent VARCHAR NULL,
userId INTEGER NULL)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
num_songs int,
artist_id varchar NOT NULL,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name text,
song_id varchar NOT NULL,
title text,
duration numeric, 
year int)
""")

songplay_table_create = ("""
create table if not exists songplay(
songplay_id int identity primary key,
start_time timestamp not null,
userId int not null ,
level varchar not null,
song_id varchar NOT NULL,
artist_id varchar NOT NULL,
session_id int not null,
location text not null,
user_agent text not null)
""")

user_table_create = ("""
create table if not exists users(
userId int primary key,
firstName text not null,
lastName text not null,
gender text not null,
level text not null)
""")

song_table_create = ("""
create table if not exists songs(
song_id varchar primary key  NOT NULL,
title text not null,
artist_id varchar not null ,
year int not null, 
duration numeric not null)
""")

artist_table_create = ("""
create table if not exists artists(
artist_id varchar primary key not null,
artist_name text not null,
artist_location varchar,
artist_latitude numeric,
artist_longitude numeric)
""")

time_table_create = ("""
create table if not exists time(
start_time timestamp primary key sortkey, 
hour int not null, 
day int not null, 
week int not null, 
month int not null, 
year int not null, 
weekday int not null)
""")

#Staging tables
staging_events_copy = (""" COPY staging_events FROM {} iam_role {} region {} FORMAT AS JSON {} timeformat 'epochmillisecs';
""").format(LOG_DATA, ARN, USER_LOC, LOG_JSONPATH)

staging_songs_copy = (""" COPY staging_songs FROM {} iam_role {} region {} FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN, USER_LOC)

#Insert tables
songplay_table_insert = (""" 
insert into songplay(start_time, userId, level, song_id, artist_id, session_id, location, user_agent)
select distinct TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS start_time, se.userId, se.level, ss.song_id,ss.artist_id, se.sessionId, se.location, se.userAgent
from staging_events se
inner join staging_songs ss
on se.song = ss.title
where se.page = 'NextSong' 
""")

user_table_insert = (""" 
insert into users (userId, firstName, lastName, gender, level)
select distinct se.userId, se.firstName, se.lastName, se.gender, se.level
from staging_events se
where se.page = 'NextSong' 
""")

song_table_insert = (""" 
insert into songs (song_id, title, artist_id, year, duration)
select distinct ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration 
from staging_songs ss
""")

artist_table_insert = (""" 
insert into artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
select distinct ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
from staging_songs ss
where ss.artist_id IS NOT NULL
""")

time_table_insert = (""" 
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' AS start_time, EXTRACT(hour FROM start_time) AS hour,EXTRACT(day FROM start_time) AS day, EXTRACT(week FROM start_time) AS week, EXTRACT(month FROM start_time) AS month, EXTRACT(year FROM start_time) AS year, EXTRACT(week FROM start_time) AS weekday
FROM staging_events AS se
where se.page = 'NextSong' 
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



  




