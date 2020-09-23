import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_in_session int,
        last_name varchar,
        length real,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration double precision,
        session_id integer,
        song varchar,
        status smallint,
        ts bigint,
        user_agent varchar,
        user_id integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs integer,
        artist_id varchar,
        artist_latitude real,
        artist_longitude real,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration real,
        year integer
    );
""")

songplay_table_create = ("""

    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id integer NOT NULL,
        level varchar,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        session_id int,
        location varchar,
        user_agent text
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    ); 
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar,
        year int,
        duration real
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude real,
        longitude real
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM 's3://udacity-dend/log_data/'
    CREDENTIALS 'aws_iam_role={}'
    JSON 's3://udacity-dend/log_json_path.json'
    REGION 'us-west-2';
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data/'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(*config['IAM_ROLE'].values())

# INSERT TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT timestamp 'epoch' + (e.ts/1000)*interval '1 second' AS start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.session_id,
            e.location,
            e.user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s
    ON e.song = s.title AND e.length = s.duration AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT user_id,
            first_name,
            last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM users)
          AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = (""" 
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    WITH events_time AS 
        (SELECT (timestamp 'epoch' + (ts/1000)*interval '1 second') AS start_time
         FROM staging_events
         WHERE page = 'NextSong')
    SELECT  DISTINCT start_time,
            EXTRACT(hour FROM start_time)     AS hour,
            EXTRACT(day FROM start_time)      AS day,
            EXTRACT(week FROM start_time)     AS week,
            EXTRACT(month FROM start_time)    AS month,
            EXTRACT(year FROM start_time)     AS year,
            EXTRACT(weekday FROM start_time)  AS weekday
    FROM events_time
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
