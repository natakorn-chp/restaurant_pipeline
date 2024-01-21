
from datetime import date
import configparser
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row

spark = SparkSession \
        .builder \
        .master("local[1]")\
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/usr/local/postgresql-42.7.1.jar") \
        .getOrCreate() 

#Read config.ini file
config_obj = configparser.ConfigParser()
config_obj.read("/home/user/de_project/config/config_info_restaurant_project.txt")

dbparam = config_obj["postgresql"]
sqluser = dbparam['sqluser']
sqlpass = dbparam['sqlpass']
dbname = dbparam['dbname']
host_nm = dbparam['host_nm']
port_nm = dbparam['port_nm']

hdfsparam = config_obj["hdfs_info"]
root_hdfs = hdfsparam['root_hdfs']
ext_pth = hdfsparam['ext_pth']

conn = "jdbc:postgresql://{}:{}/{}".format(host_nm,port_nm,dbname)


# using spark to read data from postgres db as df
dt_rest = spark.read.format("jdbc")\
        .option("url", conn)\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "raw_zone.restaurant_detail")\
        .option("user", sqluser)\
        .option("password", sqlpass)\
        .load()

dt_order = spark.read.format("jdbc")\
        .option("url", conn)\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "raw_zone.order_detail")\
        .option("user", sqluser)\
        .option("password", sqlpass)\
        .load()

# convert order_created_timestamp col to datatype timestamp
# get a partition columns named dt
dt_order = dt_order.withColumn('order_created_timestamp_new', F.to_timestamp('order_created_timestamp', 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("date", F.regexp_extract("order_created_timestamp_new","([0-9]{1,4}-[0-9]{2}-[0-9]{2})",1)) \
    .withColumn("dt",F.regexp_replace("date", "-", ""))

dt_order = dt_order.select(F.col('order_created_timestamp_new').alias('order_created_timestamp') \
    ,'status','price','discount','id','driver_id','user_id','restaurant_id','dt')

# check discount no null (optional)
# dt_order_new.select('discount','discount_no_null').filter(dt_order_new.discount.isNull()).distinct().show()

# convert null to zero for discount field
dt_order_new = dt_order.withColumn("discount_no_null", F.when(dt_order.discount.isNull() \
                        , F.lit(0)).otherwise(dt_order.discount))

dt_rest = dt_rest.withColumn('dt',F.lit('90001231'))

# convert values in esimated_cooking_time field folloing the given logic
dt_rest_new = dt_rest.withColumn("cooking_bin", F.when(dt_rest.esimated_cooking_time.between(10, 40), "1") \
                                  .when(dt_rest.esimated_cooking_time.between(41, 80), "2") \
                                  .when(dt_rest.esimated_cooking_time.between(81, 120), "3") \
                                  .otherwise("4"))

# write data as parqest file for order detail
dt_rest.write \
        .format('parquet') \
        .mode('append') \
        .partitionBy('dt') \
        .save('{}/{}/restaurant_detail/'.format(root_hdfs,ext_pth))

dt_rest_new.write \
        .format('parquet') \
        .mode('append') \
        .partitionBy('dt') \
        .save('{}/{}/restaurant_detail_new/'.format(root_hdfs,ext_pth))

# write data as parqest file for restaurant detail
dt_rest.write \
        .format('parquet') \
        .mode('append') \
        .partitionBy('dt') \
        .save('{}/{}/restaurant_detail/'.format(root_hdfs,ext_pth))

dt_rest_new.write \
        .format('parquet') \
        .mode('append') \
        .partitionBy('dt') \
        .save('{}/{}/restaurant_detail_new/'.format(root_hdfs,ext_pth))                             