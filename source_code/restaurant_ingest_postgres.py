import pandas as pd
import psycopg2
from sqlalchemy import create_engine 
import configparser

#Read config file
config_obj = configparser.ConfigParser()
config_obj.read("/home/user/de_project/config/config_info_restaurant_project.txt")

dbparam = config_obj["postgresql"]
sqluser = dbparam['sqluser']
sqlpass = dbparam['sqlpass']
dbname = dbparam['dbname']
host_nm = dbparam['host_nm']
port_nm = dbparam['port_nm']
srcparam = config_obj["data_src_path"]
src_pth = srcparam["src_pth"]


#establishing the connection
conn = psycopg2.connect(
    database=dbname
    , user=sqluser
    , password=sqlpass
    , host=host_nm
    , port=port_nm
)
# establish connections 
conn_string = 'postgresql://{}:{}@{}/{}'.format(sqluser,sqlpass,host_nm,dbname)

try:
    db = create_engine(conn_string) 
    conn_in = db.connect() 
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    #Executing an MYSQL function using the execute() method
    cursor.execute("select version()")
    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    print("Connection established to: ",data)
    
except Exception as e:
    # By this way we can know about the type of error occurring
    print("The error is: ",e)


#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Creating table as per requirement
sql_create_tb ="""create table if not exists raw_zone.order_detail(
    order_created_timestamp timestamp,
    status text,
    price int,
    discount float,
    id text,
    driver_id text,
    user_id text,
    restaurant_id text);
    
create table if not exists raw_zone.restaurant_detail(
id text,
restaurant_name text,
category text,
esimated_cooking_time float,
latitube float,
longitube float);
"""
cursor.execute(sql_create_tb)
print("Table created successfully........")
conn.commit()
conn.close() #Closing the connection

# read soruce files from local path
df_rest = pd.read_csv(src_pth+'restaurant_detail.csv')
df_order = pd.read_csv(src_pth+'order_detail_sample2.csv')

# write both df into postgres db
df_rest.to_sql('restaurant_detail', conn_in, if_exists='replace', schema='raw_zone', index=False) 
df_order.to_sql('order_detail', conn_in, if_exists='replace', schema='raw_zone', index=False) 


