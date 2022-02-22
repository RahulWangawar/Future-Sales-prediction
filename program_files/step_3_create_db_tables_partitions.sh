#!/bin/bash
cd $HIVE_HOME

hive -e "drop database miniproject cascade;
  create database if not exists miniproject;use miniproject;
  drop table if exists ext_sales_complete;
  create external table if not exists ext_sales_complete (year int, month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) stored as parquet;"

hdfs dfs -cp -p -f /mini_project/raw/persist/* /user/hive/warehouse/miniproject.db/ext_sales_complete/

hive -e "use miniproject;
  set hive.exec.dynamic.partition.mode=nonstrict;
  drop table if exists ext_sales_partition_year;
  create external table if not exists ext_sales_partition_year (month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) PARTITIONED BY (year int);
  insert overwrite table ext_sales_partition_year partition(year) select month, day, date_block_num , shop_id, item_id, item_price, item_cnt_day, year from ext_sales_complete;

  set hive.enforce.bucketing = true;
  drop table if exists bucketted_on_month;
  create table bucketted_on_month (year int, month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) clustered by (month) into 12 buckets;
  Insert overwrite table bucketted_on_month select * from ext_sales_complete;"







