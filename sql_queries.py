import configparser

# Connect directly to database using terminal
# $ psql -d "dwh" -p "5439" -U "test_user"
#   -h "dwhcluster.cuajxqgz3x85.us-west-2.redshift.amazonaws.com"

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
    DROP TABLE IF EXISTS staging_events;
"""
staging_songs_table_drop = """
    DROP TABLE IF EXISTS staging_songs;
"""
songplay_table_drop = """
    DROP TABLE IF EXISTS songplay;
"""

user_table_drop = """
    DROP TABLE IF EXISTS users;
"""

song_table_drop = """
    DROP TABLE IF EXISTS songs;
"""

artist_table_drop = """
    DROP TABLE IF EXISTS artists;
"""

time_table_drop = """
    DROP TABLE IF EXISTS time;
"""

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        event_id int identity(0, 1),
        artist_name varchar(max),
        auth varchar(max),
        first_name varchar(max),
        gender varchar(1),
        item_in_session int,
        last_name varchar(max),
        song_length double precision,
        level varchar(max),
        location varchar(max),
        method varchar(max),
        page varchar(max),
        registration varchar(max),
        session_id bigint,
        song_title varchar(max),
        status int,
        ts varchar(max),
        user_agent text,
        user_id varchar(max),
        PRIMARY KEY (event_id)
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        song_id text,
        num_songs int,
        title text,
        artist_name varchar(max),
        artist_latitude numeric,
        year int,
        duration numeric,
        artist_id text,
        artist_longitude numeric,
        artist_location text,
        PRIMARY KEY (song_id)
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id identity(0, 1),
        start_time timestamp references time(start_time),
        user_id varchar(max) references users(user_id),
        level varchar(max),
        song_id varchar(max) references songs(song_id),
        artist_id varchar(max) references artists(artist_id),
        session_id bigint,
        location varchar(max),
        user_agent text,
        PRIMARY KEY (songplay_id)
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id varchar(max),
        first_name varchar(max),
        last_name varchar(max),
        gender varchar(1),
        level varchar(max),
        PRIMARY KEY (user_id)
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar(max),
        title varchar(max),
        artist_id varchar(max) NOT NULL,
        year int,
        duration double precision,
        PRIMARY KEY (song_id)
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar(max),
        name varchar(max),
        location varchar(max),
        latitude double precision,
        longitude double precision,
        PRIMARY KEY (artist_id)
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        PRIMARY KEY (start_time)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {} json '{}' truncatecolumns;
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN')
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {} json 'auto' truncatecolumns;
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
        (start_time, user_id, level, song_id,
         artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong'
        AND e.song_title = s.title AND user_id NOT IN (
            SELECT DISTINCT s.user_id
            FROM songplays s
            WHERE s.user_id = user_id
                AND s.start_time = start_time AND s.session_id = session_id
        )
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
        AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
           EXTRACT(hr from start_time) AS hour,
           EXTRACT(d from start_time) AS day,
           EXTRACT(w from start_time) AS week,
           EXTRACT(mon from start_time) AS month,
           EXTRACT(yr from start_time) AS year,
           EXTRACT(weekday from start_time) AS weekday
    FROM (
        SELECT DISTINCT
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
        FROM staging_events s
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create, song_table_create,
    artist_table_create, time_table_create
]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
    user_table_drop, song_table_drop, artist_table_drop, time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert,
    artist_table_insert, time_table_insert
]

# ERROR OUTPUT with stl_load_error table
#
# -[ RECORD 3 ]---+------------------------------------------------------------
# userid          | 100
# slice           | 5
# tbl             | 100389
# starttime       | 2020-03-13 02:50:42.06159
# session         | 24350
# query           | 1740
# filename        | s3://udacity-dend/song-data/D/M/R/TRDMRMV128F9330FC2.json
#
# line_number     | 1
# colname         | artist_location
#
# type            | varchar
# col_length      | 256
# position        | 0
# raw_line        | {"song_id": "SORMPUE12AB0182F1F", "num_songs": 1, "title": "The Strangers", "artist_name": "St. Vincent", "artist_latitude": 19.40904, "year": 2009, "duration": 244.84526, "artist_id": "AR0JBXL1187FB52810", "artist_longitude": -99.14977, "artist_location": "ORDER &#039;ACTOR&#039; ON INSOUND: <a href=\\"http://www.insound.com/search/searchmain.jsp?query=st.+vincent+actor\\" target=\\"_blank\\" rel=\\"nofollow\\" onmousedown='UntrustedLink.bootstrap($(this), \\"\\", event)'>http://www.insound.com/search/searchmain.jsp?query=st.+vincent+actor</a>"}
# err_code        | 1204
# err_reason      | String length exceeds DDL length
