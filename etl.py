import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws_cred']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_cred']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    print(song_data)
    
    # read song data file
    df = spark.read.json(song_data)
    df.take(10)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter((df.page == 'NextSong') | (df.page == 'Home'))

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    # write users table to parquet files
    users_table = users_table.write.parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t : datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda t : datetime.fromtimestamp(t/1e3))
    df = df.withColumn('datetime', get_datetime(col('ts')))
    
    # extract columns to create time table
    time_table = df.select([monotonically_increasing_id().alias('id'),'timestamp', hour('timestamp').alias('hour'),dayofmonth('timestamp').alias('day'),weekofyear('timestamp').alias('week'), month('timestamp').alias('month'), year('timestamp').alias('year'), dayofweek('timestamp').alias('weekday')])
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title) & (df.length == song_df.duration) & (df.artist == song_df.artist_name) & (df.location == song_df.artist_location)).select([monotonically_increasing_id().alias('songplay_id'),'timestamp', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']).where(df.page == 'NextSong')

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.parquet(output_data + 'songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
