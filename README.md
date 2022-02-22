# automated-Sales-predictions
Integrated data ingestion into hdfs, data cleaning, hive queries, Machine Learning model, report generation, and automated all these things using AirflowDag.
Big Data Mini-Project : Future sales prediction<br>

Batch: PG-DBDA,Sept-2021 ACTS,Pune <br>
Name : Wangawar Rahul Narsimalu<br>
PRN :  210940125055<br>

Following are the steps performed for miniproject execution:<br>

Step 1 ==> Ingested data in hdfs at /mini_project/raw/stage/

Step 2 ==> Ran MapReduce code for data quality and dumped data at dq_good

Step 3 ==> Used Spark read and write to read data from dq_good and write data in parquet format at /mini_project/raw/persist 

Step 4 ==> create database miniproject.db in hive warehouse 

Step 5 ==> Created External table, partitioned table, Bucketted table in database.
