from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from datetime import date
import configparser
import pyspark
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .master("local[1]")\
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/usr/local/postgresql-42.7.1.jar") \
        .getOrCreate() # .config("spark.ui.port","4050")\

# read new order detail from hive
new_order_df = spark.read \
        .format('parquet') \
        .load('hdfs://127.0.0.1:9000/de_project/ext_restaurant/order_detail_new')
new_order_df.createOrReplaceTempView("order_detail_new")

# read new restaurant detail from hive
new_rest_df = spark.read \
        .format('parquet') \
        .load('hdfs://127.0.0.1:9000/de_project/ext_restaurant/restaurant_detail_new')
new_rest_df.createOrReplaceTempView("restaurant_detail_new")

# Get the average discount for each category via spark and convert df to pandas df
avg_cat_df = spark.sql("""
select r.category, avg(o.discount_no_null) as agv_discount
from order_detail_new as o
left join  restaurant_detail_new as r
    on o.restaurant_id = r.id
group by r.category
""").toPandas()

# Row count per each cooking_bin via spark and convert df to pandas df
cnt_cooking_bin_df = spark.sql("""
select cooking_bin, count(1) as count
from restaurant_detail_new
group by cooking_bin;
""").toPandas()

# save the both result as csv file on specific path
avg_cat_df.to_csv('/home/user/de_project/result_analysis/discount.csv', index=False)
cnt_cooking_bin_df.to_csv('/home/user/de_project/result_analysis/cooking.csv', index=False)