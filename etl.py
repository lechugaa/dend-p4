import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, to_timestamp
from lake_schemas import songs_schema, logs_schema, song_fields, artist_fields, user_fields, time_fields, songplay_fields


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["IAM"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["IAM"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Creates a spark session with adequate Hadoop Module configuration
    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", config["SPARK"]["HADOOP_MODULE"]) \
        .getOrCreate()
    return spark


def process_song_data(spark, song_data_path, output_data_path):
    """
    Processes song data located in S3 bucket into analytical tables and writes them down as parquet files
    back to S3.
    :param spark: spark session
    :param song_data_path: string
    :param output_data_path: string
    """

    # read song data file
    df = spark.read.json(song_data_path, schema=songs_schema)

    # extract columns to create songs table
    songs_df = df.select(*song_fields).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_df.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data_path + "songs")

    # extract columns to create artists table
    artists_df = df.select(*artist_fields).distinct()
    artists_df = artists_df \
        .withColumnRenamed("artist_name", "name") \
        .withColumnRenamed("artist_location", "location") \
        .withColumnRenamed("artist_latitude", "latitude") \
        .withColumnRenamed("artist_longitude", "longitude")
    
    # write artists table to parquet files
    artists_df.write.mode("overwrite").parquet(output_data_path + "artists")


def process_log_data(spark, song_data_path, log_data_path, output_data_path):
    """
    Processes song data located in S3 bucket into analytical tables and writes them down as parquet files
    back to S3.
    :param spark: spark session
    :param song_data_path: string
    :param log_data_path: string
    :param output_data_path: string
    """

    # read log data file
    df = spark.read.json(log_data_path, schema=logs_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    df = df.withColumn("start_time", to_timestamp(df.ts / 1000)).drop("ts")

    # extract columns for users table    
    users_df = df.select(*user_fields).distinct()
    users_df = users_df \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name")
    
    # write users table to parquet files
    users_df.write.mode("overwrite").parquet(output_data_path + "users")
    
    # extract columns to create time table
    time_df = df.select(*time_fields)
    time_df = time_df \
        .withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))
    
    # write time table to parquet files partitioned by year and month
    time_df.write.partitionBy("year", "month").mode("overwrite").parquet(output_data_path + "time")

    # read in song data to use for songplays table
    song_data_df = spark.read.json(song_data_path, schema=songs_schema)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = df.join(song_data_df,
                           (df.artist == song_data_df.artist_name) &
                           (df.song == song_data_df.title) &
                           (df.length == song_data_df.duration))
    songplays_df = songplays_df.select(*songplay_fields) \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("sessionId", "session_id") \
        .withColumnRenamed("userAgent", "user_agent") \
        .withColumn("year", year("start_time")) \
        .withColumn("month", month("start_time"))

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.partitionBy("year", "month").mode("overwrite").parquet(output_data_path + "songplays")


def main():
    """
    Main function in charge of the following tasks:
        - Create a Spark session
        - Obtain adequate paths for input and output data
        - Process song data
        - Process log data
    """
    spark = create_spark_session()
    song_data_path = config["S3"]["SONG_DATA_PATH"]
    log_data_path = config["S3"]["LOG_DATA_PATH"]
    output_data_path = config["S3"]["OUTPUT_DATA_PATH"]
    
    process_song_data(spark, song_data_path, output_data_path)
    process_log_data(spark, song_data_path, log_data_path, output_data_path)


if __name__ == "__main__":
    main()
