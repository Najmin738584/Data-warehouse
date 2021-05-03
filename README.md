## Project : Data Warehouse

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Purpose of this project is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Project Datasets :
Two datasets that reside in S3 are
1) song_data
2) log_data

### Schema for Song Play Analysis : 
Using the song and event datasets We created a star schema optimized for queries on song play analysis. This includes the following tables

Fact Table       - 1) songplays

Dimension Tables - 1) users  
                   2) songs  
                   3) artists  
                   4) time  
                   
The project includes following files:
1) create_table.py - where we created fact and dimension tables for the star schema in Redshift.
2) etl.py          - where we load data from S3 into staging tables on Redshift and then processed that data into analytics tables on 
                     Redshift.
3) sql_queries.py  - where we define SQL statements, which are imported into the two other files above.  

### Steps to follow :

To access AWS :
1) create IAM user 
2) create IAM role with AmazonS3ReadOnlyAccess access rights
3) get ARN
4) create and run Redshift cluster
5) Fill the open fields in dwh.cfg file
6) Create an AWS S3 bucket.
7) Edit dwh.cfg: add your S3 bucket name in LOG_PATH and SONG_PATH variables.
8) Data is present in data folder.To run the script to use that data,Copy log_data and song_data folders to your own S3 bucket.

After completing all steps run create_table.py & etl.py 

Deleted the redshift cluster when finished.


