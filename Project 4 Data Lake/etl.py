import configparser
import os
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime



def create_spark_session():
    """
    Create and return a Spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Staging, Extracting song data json files from AWS S3 and Writing Extracted parquet data file back to AWS S3
    
    Arguments:
            spark {object}:        The entry point to programming Spark with the Dataset and DataFrame API.
            input_data {string}:   S3 bucket where Sparkify's event data is stored 
            output_data {string}:  S3 bucket to store extracted parquet data file
    Returns:
            No return values
    
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/'+'songs.parquet', partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/' + 'artists.parquet', partitionBy=['artist_id'] )


def process_log_data(spark, input_data, output_data):
    """Staging, Extracting log data json files from AWS S3 and Writing Extracted parquet data file back to AWS S3
    
    Arguments:
            spark {object}:        The entry point to programming Spark with the Dataset and DataFrame API.
            input_data {string}:   S3 bucket path where Sparkify's event data is stored 
            output_data {string}:  S3 bucket path used to store extracted parquet data file
    Returns:
            No return values
    
    """
        
    # get filepath to log data file
    # log_data = log_data = input_data + "log_data/*/*/"
    log_data = log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df =log_df.where('page="NextSong"')

    # extract columns for users table    
    users_table = log_df.select(["userId", "firstName", "lastName", "gender", "level"]).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/' + 'users.parquet', partitionBy = ['userId'])

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn('timestamp',( (log_df.ts.cast('float')/1000).cast("timestamp")) )
    
    # extract columns to create time table
    time_table = log_df.select(
                    F.col("timestamp").alias("start_time"),
                    F.hour("timestamp").alias('hour'),
                    F.dayofmonth("timestamp").alias('day'),
                    F.weekofyear("timestamp").alias('week'),
                    F.month("timestamp").alias('month'), 
                    F.year("timestamp").alias('year'), 
                    F.date_format(F.col("timestamp"), "E").alias("weekday")
                )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/' + 'time.parquet', partitionBy=['start_time'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')
    
    # join song_df and log_df
    song_log_joined_table = log_df.join(song_df, (log_df.song == song_df.title) & (log_df.artist == song_df.artist_name) & (log_df.length == song_df.duration), how='inner')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_log_joined_table.distinct() \
                        .select("userId", "timestamp", "song_id", "artist_id", "level", "sessionId", "location", "userAgent" ) \
                        .withColumn("songplay_id", F.row_number().over( Window.partitionBy('timestamp').orderBy("timestamp"))) \
                        .withColumnRenamed("userId","user_id")        \
                        .withColumnRenamed("timestamp","start_time")  \
                        .withColumnRenamed("sessionId","session_id")  \
                        .withColumnRenamed("userAgent", "user_agent") \

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/' + 'songplays.parquet',partitionBy=['start_time', 'user_id'])


def main():
    """
    Run the ETL to process the song_data and the log_data files
    """
    
    # Load AWS credentials as env vars
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    # Set input path and output path    
    input_data = "s3a://udacity-dend/"
    output_data  = "s3a://udacity-sparkify-data-lake-alisson/" 
        
    # Create a Spark Session
    spark = create_spark_session()
    
    # Run the ETL to Process song dataset and log dataset
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()