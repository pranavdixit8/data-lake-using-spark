from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp,to_date
from pyspark.sql.types import TimestampType, DateType, IntegerType




def create_spark_session():
    
    """
    Creates Spark Session


    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    preproces json files in folder song_data to create parquet song and artist files

    Args:
        spark: Spark session object
        input_data: location of input folder
        output_data: location of output folder

    """
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)


    # extract columns to create songs table
    songs_table = df.select(df.song_id, 
                            df.title, 
                            df.artist_id, 
                            df.year, 
                            df.duration)\
                            .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(os.path.join(output_data,"songs_table"))

    # extract columns to create artists table
    artists_table = df.select(df.artist_id, 
                              df.artist_name, 
                              df.artist_location, 
                              df.artist_latitude, 
                              df.artist_longitude)\
                              .withColumnRenamed("artist_name", "name")\
                              .withColumnRenamed("artist_location" ,"location")\
                              .withColumnRenamed("artist_latitude", "latitude")\
                              .withColumnRenamed("artist_longitude", "longitude")\
                              .dropDuplicates()
    
    # write artists table to parquet files
   
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data,"artists_table"))


def process_log_data(spark, input_data, output_data):
    
    """
    preproces json files in folder song_data to create parquet users, time and songplays files

    Args:
        spark: Spark session object
        input_data: location of input folder
        output_data: location of output folder

    """
 
    # get filepath to log data file
    # log_data = input_data + "log_data/*/*/*.json"
    log_data = input_data + "log_data/*.json"
    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays
    df_filtered = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table =  df_filtered.select("userId", 
                                      "firstName", 
                                      "lastName", 
                                      "gender", 
                                      "level")\
                                      .withColumnRenamed("userId","user_id")\
                                      .withColumnRenamed("firstName", "first_name")\
                                      .withColumnRenamed("lastName","last_name")\
                                      .dropDuplicates()
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users_table")

    
    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", to_date(df.timestamp))

   
    # extract columns to create time table
    time_table = df.select(["ts", "datetime"])
    time_table = time_table.withColumn("hour", hour("datetime"))\
                           .withColumn("day", dayofmonth("datetime"))\
                           .withColumn("week", weekofyear("datetime"))\
                           .withColumn("month", month("datetime"))\
                           .withColumn("year", year("datetime"))\
                           .withColumn("weekday", date_format("datetime", "E"))\
                           .withColumnRenamed("ts", "start_time")\
                           .dropDuplicates()
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data,"time_table"))

    
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.title == df.song)\
                        .select("ts", 
                                "userId",
                                "level",
                                "song_id", 
                                "artist_id", 
                                "sessionId", 
                                "location", 
                                "userAgent", "datetime")\
                                .withColumnRenamed("userId", "user_id")\
                                .withColumnRenamed("ts", "start_time")\
                                .withColumnRenamed("sessionId", "session_id")\
                                .withColumnRenamed("userAgent", "user_agent")\
                                .withColumn("month", month("datetime"))\
                                .withColumn("year", year("datetime"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data,"songplays_table"))


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    # output_data = "s3a://dend-data-lakes/"
    input_data = "data/"
    output_data = "result/"
   
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
