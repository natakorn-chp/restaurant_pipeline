/*----------------------------------------------------------
-                        postgres                          -
----------------------------------------------------------*/

-->> create a user
CREATE USER <user_name> WITH PASSWORD '<pass>';

-- example: CREATE USER de_user WITH PASSWORD 'testuser';

-->> create a database 
CREATE DATABASE <db_name>;

-- example: CREATE DATABASE restaurant_db;

-->> create a schenma 
CREATE SCHEMA <schema_name>;

-- example: CREATE SCHEMA raw_zone;

-->> defind permision betweet user and database 

ALTER DATABASE restuarant_db OWNER TO de_user;

ALTER SCHEMA raw_zone OWNER TO de_user;

-->> create tables 
create table order_detail(
order_created_timestamp timestamp,
status text,
price int,
discount float,
id text,
driver_id text,
user_id text,
restaurant_id text);

create table restaurant_detail(
id text,
restaurant_name text,
category text,
esimated_cooking_time float,
latitube float,
longitube float);
