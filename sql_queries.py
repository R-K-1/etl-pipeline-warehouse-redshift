import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN    = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_JSONPATH  = config.get('S3', 'SONG_JSONPATH')
LOG_DATA        = config.get('S3', 'LOG_DATA')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id BIGINT IDENTITY(0,1) NOT NULL,
        artist VARCHAR(100),
        auth VARCHAR(20) NOT NULL,
        firstName VARCHAR(100),
        gender VARCHAR(1),
        itemInSession SMALLINT NOT NULL,
        lastName VARCHAR(100),
        length DECIMAL(5) NOT NULL,
        level VARCHAR(20) NOT NULL,
        location VARCHAR(100),
        method VARCHAR(10) NOT NULL,
        page VARCHAR(100) NOT NULL,
        registration DECIMAL(1),
        sessionId INT NOT NULL SORTKEY DISTKEY,
        song VARCHAR(100),
        status SMALLINT,
        ts BIGINT NOT NULL,
        userAgent VARCHAR(200),
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR(20),
        num_songs SMALLINT,
        title VARCHAR(100) NOT NULL,
        artist_name VARCHAR(100) NOT NULL,
        artist_latitutde DECIMAL(5),
        year SMALLINT,
        duration DECIMAL(5) NOT NULL,
        artist_id VARCHAR(20) NOT NULL SORTKEY DISTKEY,
        artist_longitude DECIMAL(5),
        artist_location VARCHAR(100)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) SORTKEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR(10) NOT NULL,
        song_id  VARCHAR(20),
        artist_id VARCHAR(20),
        session_id INT NOT NULL DISTKEY,
        location VARCHAR(100),
        user_agent VARCHAR(200)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL SORTKEY,
        first_name  VARCHAR(50),
        last_name  VARCHAR(50),
        gender  VARCHAR(1),
        level VARCHAR(10) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id  VARCHAR(20) NOT NULL,
        title VARCHAR(100) NOT NULL SORTKEY,
        artist_id VARCHAR(20) NOT NULL,
        year SMALLINT,
        duration DECIMAL(5) NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(20) NOT NULL SORTKEY,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(100),
        latitude DECIMAL(5),
        longitude DECIMAL(5)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id BIGINT IDENTITY(0,1) SORTKEY,
        start_time TIMESTAMP NOT NULL,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role ''{}'
    format as json {}
    STATUPDATE ON
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    aws_iam_role '{}'
    json 'auto'
    STATUPDATE ON
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays 
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT 
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
    se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events AS se
    JOIN staging_songs AS ss
    ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
    se.userId, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
    ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT 
    ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time 
    (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour, EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week, EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year, EXTRACT(week FROM start_time) AS weekday
    FROM    staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
