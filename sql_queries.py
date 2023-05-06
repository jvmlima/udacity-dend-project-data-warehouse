import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

REGION       = config.get('AWS',      'REGION')
ARN          = config.get('IAM_ROLE', 'ARN')
LOG_DATA     = config.get('S3',       'LOG_DATA')
LOG_JSONPATH = config.get('S3',       'LOG_JSONPATH')
SONG_DATA    = config.get('S3',       'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

# Stating ---

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id      BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    artist        VARCHAR                  NULL DISTKEY,
    auth          VARCHAR                  NULL,
    firstName     VARCHAR                  NULL,
    gender        VARCHAR                  NULL,
    itemInSession BIGINT                   NULL,
    lastName      VARCHAR                  NULL,
    length        DOUBLE PRECISION         NULL,
    level         VARCHAR                  NULL,
    location      VARCHAR                  NULL,
    method        VARCHAR                  NULL,
    page          VARCHAR                  NULL,
    registration  BIGINT                   NULL,
    sessionId     BIGINT               NOT NULL,
    song          VARCHAR                  NULL,
    status        SMALLINT                 NULL,
    ts            BIGINT                   NULL,
    userAgent     VARCHAR                  NULL,
    userId        VARCHAR              NOT NULL          
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        BIGINT               NULL,
    artist_id        VARCHAR          NOT NULL,
    artist_latitude  DOUBLE PRECISION     NULL,
    artist_longitude DOUBLE PRECISION     NULL,
    artist_location  VARCHAR              NULL,
    artist_name      VARCHAR              NULL,
    song_id          VARCHAR          NOT NULL,
    title            VARCHAR              NULL,
    duration         DOUBLE PRECISION     NULL,
    year             SMALLINT             NULL DISTKEY
);
""")

# Fact ---

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id BIGINT    IDENTITY(0,1) NOT NULL SORTKEY  PRIMARY KEY,
    start_time  TIMESTAMP               NOT NULL,
    user_id     VARCHAR                 NOT NULL,
    level       VARCHAR                     NULL,
    song_id     VARCHAR                 NOT NULL DISTKEY,
    artist_id   VARCHAR                 NOT NULL,
    session_id  VARCHAR                 NOT NULL,
    location    VARCHAR                     NULL,
    user_agent  VARCHAR                     NULL
);
""")

# Dimensions ---

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    first_name VARCHAR     NULL,
    last_name  VARCHAR     NULL, 
    gender     VARCHAR     NULL, 
    level      VARCHAR     NULL 
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR          NOT NULL SORTKEY PRIMARY KEY,  
    title     VARCHAR              NULL,  
    artist_id VARCHAR          NOT NULL,  
    year      SMALLINT             NULL,  
    duration  DOUBLE PRECISION     NULL  
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR          NOT NULL SORTKEY PRIMARY KEY,  
    name      VARCHAR              NULL,  
    location  VARCHAR              NULL,  
    latitude  DOUBLE PRECISION     NULL,  
    longitude DOUBLE PRECISION     NULL  
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time TIMESTAMP NOT NULL SORTKEY PRIMARY KEY, 
    hour       SMALLINT  NOT NULL, 
    day        SMALLINT  NOT NULL, 
    week       SMALLINT  NOT NULL, 
    month      SMALLINT  NOT NULL, 
    year       SMALLINT  NOT NULL, 
    weekday    SMALLINT  NOT NULL 
) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    REGION '{}';
""").format('staging_events',LOG_DATA,ARN,LOG_JSONPATH,REGION)

staging_songs_copy = ("""
    COPY {} FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON '{}'
    REGION '{}';
""").format('staging_songs',SONG_DATA,ARN,'auto',REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (
                         start_time,
                         user_id,
                         level,
                         song_id,
                         artist_id,
                         session_id,
                         location,
                         user_agent
    )
    SELECT  
        DISTINCT TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 second' \
                      AS start_time,
        ev.userId     AS user_id,
        ev.level      AS level,
        so.song_id    AS song_id,
        so.artist_id  AS artist_id,
        ev.sessionId  AS session_id,
        ev.location   AS location,
        ev.userAgent  AS user_agent
    FROM 
        staging_events AS ev
    JOIN 
        staging_songs AS so
        ON (ev.artist = so.artist_name)
    WHERE 
        ev.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (                 
                              user_id,
                              first_name,
                              last_name,
                              gender,
                              level
    )
    SELECT  
        DISTINCT ev.userId AS user_id,
        ev.firstName       AS first_name,
        ev.lastName        AS last_name,
        ev.gender          AS gender,
        ev.level           AS level
    FROM 
        staging_events AS ev
    WHERE 
        ev.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (
                               song_id,
                               title,
                               artist_id,
                               year,
                               duration
    )
    SELECT  
        DISTINCT so.song_id AS song_id,
        so.title            AS title,
        so.artist_id        AS artist_id,
        so.year             AS year,
        so.duration         AS duration
    FROM 
        staging_songs AS so;
""")

artist_table_insert = ("""
    INSERT INTO artists (        
                                 artist_id,
                                 name,
                                 location,
                                 latitude,
                                 longitude
    )
    SELECT  
        DISTINCT so.artist_id AS artist_id,
        so.artist_name        AS name,
        so.artist_location    AS location,
        so.artist_latitude    AS latitude,
        so.artist_longitude   AS longitude
    FROM 
        staging_songs AS so;
""")

time_table_insert = ("""
    INSERT INTO times (                  
                                            start_time,
                                            hour,
                                            day,
                                            week,
                                            month,
                                            year,
                                            weekday
    )
    SELECT  
        DISTINCT TIMESTAMP 'epoch' + ev.ts/1000  * INTERVAL '1 second' \
                                         AS start_time,
        EXTRACT(hour    FROM start_time) AS hour,
        EXTRACT(day     FROM start_time) AS day,
        EXTRACT(week    FROM start_time) AS week,
        EXTRACT(month   FROM start_time) AS month,
        EXTRACT(year    FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM    
        staging_events AS ev
    WHERE 
        ev.page = 'NextSong';
""")

# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# # create_table_queries = [user_table_create]
# # drop_table_queries = [user_table_drop]


# copy_table_queries = [staging_events_copy,staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

create_table_queries = {'staging_events': staging_events_table_create,
                        'staging_songs':  staging_songs_table_create,
                        'songplay':       songplay_table_create,
                        'users':          user_table_create,
                        'songs':          song_table_create,
                        'artists':        artist_table_create,
                        'times':          time_table_create}

drop_table_queries   = {'staging_events': staging_events_table_drop,
                        'staging_songs':  staging_songs_table_drop,
                        'songplay':       songplay_table_drop,
                        'users':          user_table_drop,
                        'songs':          song_table_drop,
                        'artists':        artist_table_drop,
                        'times':          time_table_drop}

copy_table_queries   = {'staging_events': staging_events_copy,
                        'staging_songs':  staging_songs_copy}

insert_table_queries = {'songplay':       songplay_table_insert,
                        'users':          user_table_insert,
                        'songs':          song_table_insert,
                        'artists':        artist_table_insert,
                        'times':          time_table_insert}