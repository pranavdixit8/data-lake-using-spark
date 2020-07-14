
# Project Overview

The idea of the project is to analysis the activity on a music app by artists, songs, users over time. We do so using a star schema data warehouse architecture with dimensions: artists, songs, users, time with facts being the activity by the user of playing songs from the app. We are using spark's schema-on read to create fact and dimension tables stored as parquet files.


# Commands
 
> perform etl and insert data into tables
>>python etl.py

# Files

 - ***etl.py***: this file execute the etl process for our project: loading the files in dataframes, modifying the dataframes to be loaded as fact and dimension tables as parquest files.
 - ***dl.cfg***: this is the configuration file containin the keypair



# Design


## Fact Table

 - ***songplay***: 
 > Columns: start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, datetime, month, year
 

## Dimension Tables

 - ***artist***:
 > Columns: artist_id, name, location, latitude, longitude

 
 - ***song***:
 > Columns: song_id, title, artist_id, year, duration

 
 - ***users***:
 > Columns: user_id, first_name, last_name, gender, level

 
 - ***time***:
 > Columns: start_time, datetime,hour, day, week, month, year, weekday

 
 