

create database rest_stg_tb
location "hdfs://127.0.0.1:9000/de_project/ext_restaurant";

create external table rest_stg_tb.order_detail(
order_created_timestamp timestamp,
status string,
price int,
discount double,
id string,
driver_id string,
user_id string,
restaurant_id string)
partitioned by (dt string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION "hdfs://127.0.0.1:9000/de_project/ext_restaurant/order_detail/";


create external table `rest_stg_tb.__order_detail_new__`(
order_created_timestamp timestamp,
status string,
price int,
discount double,
id string,
driver_id string,
user_id string,
restaurant_id string,
discount_no_null int)
partitioned by (dt string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION "hdfs://127.0.0.1:9000/de_project/ext_restaurant/order_detail_new/";


create external table rest_stg_tb.restaurant_detail(
id string,
restaurant_name string,
category string,
esimated_cooking_time double,
latitude double,
longitude double)
partitioned by (dt string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION "hdfs://127.0.0.1:9000/de_project/ext_restaurant/restaurant_detail/";


create external table `rest_stg_tb.__restaurant_detail_new__`(
id string,
restaurant_name string,
category string,
esimated_cooking_time double,
latitude double,
longitude double,
cooking_bin string)
partitioned by (dt string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION "hdfs://127.0.0.1:9000/de_project/ext_restaurant/restaurant_detail_new/";

MSCK REPAIR TABLE rest_stg_tb.restaurant_detail;
MSCK REPAIR TABLE `rest_stg_tb.__restaurant_detail_new__`;
MSCK REPAIR TABLE rest_stg_tb.order_detail;
MSCK REPAIR TABLE `rest_stg_tb.__order_detail_new__`;


/*
select * from rest_stg_tb.restaurant_detail limit 5;
select * from `rest_stg_tb.__restaurant_detail_new__` limit 5;
select * from rest_stg_tb.order_detail limit 5;
select * from `rest_stg_tb.__order_detail_new__` limit 5;
*/

/*
drop TABLE restaurant_detail;
drop TABLE `__restaurant_detail_new__`;
drop TABLE order_detail;
drop TABLE `__order_detail_new__`;
*/

/*
hdfs dfs -rm -r /de_project/ext_restaurant/order_detail/*
hdfs dfs -rm -r /de_project/ext_restaurant/order_detail_new/*
hdfs dfs -rm -r /de_project/ext_restaurant/restaurant_detail/*
hdfs dfs -rm -r /de_project/ext_restaurant/restaurant_detail_new/*
*/







