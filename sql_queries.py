import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN    = config.get('IAM_ROLE', 'ARN')
LOG_JSON_PATH    = config.get('S3', 'LOG_JSONPATH')
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
        artist VARCHAR(500),
        auth VARCHAR(50),
        firstName VARCHAR(500),
        gender VARCHAR(1),
        itemInSession SMALLINT,
        lastName VARCHAR(500),
        length DECIMAL(9, 5),
        level VARCHAR(50),
        location VARCHAR(500),
        method VARCHAR(50),
        page VARCHAR(100),
        registration DECIMAL(16, 1),
        sessionId INT NOT NULL SORTKEY DISTKEY,
        song VARCHAR(500),
        status SMALLINT,
        ts BIGINT NOT NULL,
        userAgent VARCHAR(500),
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR(50) NOT NULL,
        num_songs SMALLINT,
        title VARCHAR(500),
        artist_name VARCHAR(500),
        artist_latitude DECIMAL(10, 5),
        year SMALLINT,
        duration DECIMAL(9, 5) NOT NULL,
        artist_id VARCHAR(50) NOT NULL SORTKEY DISTKEY,
        artist_longitude DECIMAL(10, 5),
        artist_location VARCHAR(500)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) SORTKEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR(50) NOT NULL,
        song_id  VARCHAR(50),
        artist_id VARCHAR(50),
        session_id INT NOT NULL DISTKEY,
        location VARCHAR(500),
        user_agent VARCHAR(500)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL SORTKEY,
        first_name  VARCHAR(500),
        last_name  VARCHAR(500),
        gender  VARCHAR(1),
        level VARCHAR(50)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id  VARCHAR(50) NOT NULL,
        title VARCHAR(500) NOT NULL SORTKEY,
        artist_id VARCHAR(50) NOT NULL,
        year SMALLINT,
        duration DECIMAL(9, 5) NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(50) NOT NULL SORTKEY,
        name VARCHAR(500),
        location VARCHAR(500),
        latitude DECIMAL(10, 5),
        longitude DECIMAL(10, 5)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id BIGINT IDENTITY(0,1) NOT NULL SORTKEY,
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
    iam_role {}
    format as json {}
    STATUPDATE ON
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {}
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