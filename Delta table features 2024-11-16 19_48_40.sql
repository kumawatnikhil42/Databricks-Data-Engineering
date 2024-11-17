-- Databricks notebook source
create table employees
(id int, name string, salary double);

-- COMMAND ----------

insert into employees
values 
(1,"Adam",3500.0),
(2,"Sarah",4020.5),
(3,"John",2999.3),
(4,"Thomas",4000.3),
(5,"Anna",2500.0),
(6,"Kim",6200.3)

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

update employees
set salary=salary+100
where name like "A%";

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

      describe history employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Advanced Delta Lake Features  

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees version as of 1

-- COMMAND ----------

select * from employees@v1


-- COMMAND ----------

delete from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

restore table employees to version as of 2

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

optimize employees
zorder by id

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

vacuum employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
-- MAGIC

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
-- MAGIC

-- COMMAND ----------

describe history employees

-- COMMAND ----------

drop table employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Databases and Tables 

-- COMMAND ----------

create table managed_default
  (width int ,length int, height int);
insert into managed_default
  values (3,2,1);

-- COMMAND ----------

select * from managed_default;

-- COMMAND ----------

describe extended managed_default;

-- COMMAND ----------

-- create table external_default
--   (width int, length int, height int)
-- location 'dbfs:/custom/path/default';

-- insert into external_default 
-- values (3,2,1);


-- COMMAND ----------

-- describe extended external_default;

-- COMMAND ----------

drop table managed_default;

-- COMMAND ----------

select * from managed_default;

-- COMMAND ----------

create schema new_default;

-- COMMAND ----------

describe database extended new_default;

-- COMMAND ----------

use new_default;

create table managed_new_default
  (width int, length int, height int);

insert into managed_new_default
values (3,2,1);

--------------------------------------------

-- create table external_new_default
--   (width int, length int, height int);
-- location 'dbfs:/mnt/path/external_default';

-- insert into extrenal_new_default
-- values (3,2,1);

-- COMMAND ----------

 drop table managed_new_default;

-- COMMAND ----------

-- create schema custom
-- location 'dbfs:/Shared/schemas/custom.db';

-- COMMAND ----------

-- describe database extended custom;

-- COMMAND ----------

-- use custom;

-- create table managed_custom
--   (width int, length int, height int);

-- insert into managed_custom
-- values (3,2,1);

--------------------------------------------

-- create table external_custom
--   (width int, length int, height int);
-- location 'dbfs:/mnt/path/external_custom';

-- insert into extrenal_custom
-- values (3,2,1);

-- COMMAND ----------

-- drop table external_custom;
-- drop table managed_custom;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Views

-- COMMAND ----------


create table if not exists smartphones
(id int, name string, brand string, year int );

insert into smartphones
values (1,'Iphone 14','Apple',2022),
      (2,'Iphone 13','Apple',2021),
      (3,'Iphone 6','Apple',2014),
      (4,'Ipad Air','Apple',2013),
      (5,'Galaxy s22','Samsung',2022),
      (6,'Galaxy z fold','Samsung',2022),
      (7,'Galaxy s9','Samsung',2016),
      (8,'12 Pro','Xiaomi',2022),
      (9,'Redmi 11T Pro','Xiaomi',2022),
      (10,'Redmi Note 11','Xiaomi',2021)

-- COMMAND ----------

select * from smartphones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

create view view_apple_phones
as select * from smartphones
where brand='Apple';

-- COMMAND ----------

select * from view_apple_phones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

create temp view temp_view_phones_brands
as select distinct brand from smartphones;

select * from temp_view_phones_brands;

-- COMMAND ----------

show tables;

-- COMMAND ----------

create global temp view global_temp_view_latest_phones
as select * from smartphones
where year>2020
order by year desc;


-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

drop table smartphones;
drop view view_apple_phones;
drop view global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

drop view global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


