from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# songs input schema
songs_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", LongType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", LongType())
    ])

# logs input schema
logs_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType()),
        StructField("status", LongType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType()),
    ])

# table fields

# song fields
song_fields = ["song_id", "title", "artist_id", "year", "duration"]

# artist fields
artist_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]

# user fields
user_fields = ["userId", "firstName", "lastName", "gender", "level"]

# time fields
time_fields = ["start_time"]

# songplay fields
songplay_fields = ["start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]
