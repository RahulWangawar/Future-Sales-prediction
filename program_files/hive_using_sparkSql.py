from pyspark.sql import SparkSession

master = 'local'
appName = 'PySpark_Dataframe Hive Operations'

warehouse_location = 'hdfs://localhost:9090/user/hive/warehouse'
metastore_location = '/home/rahulw/DBDA_HOME/apache-hive-2.3.9-bin'
# import org.apache.spark.sql.hive.HiveContext
# val sqlContext = new HiveContext(sc)
# val depts = sqlContext.sql("select * from departments")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .config('hive.metastore.warehouse.dir', metastore_location) \
    .enableHiveSupport() \
    .getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {spark.sparkContext.appName}')
    print(f'Master: {spark.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

spark.sql("CREATE DATABASE IF NOT EXISTS mini_project")
spark.sql("use mini_project;")
spark.sql("SHOW DATABASES").show()

spark.sql("create external table if not exists ext_sales (year int,month int,day int,date_block_num int,shop_id Int,item_id Int,item_price Float,item_cnt_day Int ) stored as parquet Location '/user/hive/warehouse/mini_project.db/';")

import os;
os.system("hdfs dfs -put /mini_project/raw/persist/part-00000-180b2cda-e18f-43b9-8f10-17e0b824a7ac-c000.snappy.parquet /user/hive/warehouse/mini_project/ext_sales/")
# spark.sql("dfs -put /mini_project/raw/persist/part-00000-180b2cda-e18f-43b9-8f10-17e0b824a7ac-c000.snappy.parquet /user/hive/warehouse/mini_project/ext_sales/")

# spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

spark.sql("create external table if not exists sales_partitioned (month int,day int,date_block_num int,shop_id Int,item_id Int,item_price Float,item_cnt_day Int) PARTITIONED BY (year int) stored as parquet Location '/user/hive/warehouse/mini_project.db/';")

spark.sql("insert overwrite table sales_partitioned partition(year) select month, day, date_block_num ,shop_id,item_id ,item_price ,item_cnt_day, year  from ext_sales;")
spark.sql("SHOW TABLES").show()
# spark.sql("CREATE DATABASE IF NOT EXISTS report")
# spark.sql("use report;")







