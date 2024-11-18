-- Databricks notebook source
-- MAGIC %run /Users/kumawatnikhil42@gmail.com/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files= dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json/export_001.json`;

-- COMMAND ----------

select * from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json/export_*.json`;

-- COMMAND ----------

select * from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`;

-- COMMAND ----------

select count(*) from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`;

-- COMMAND ----------

select *,
input_file_name() source_file
from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`

-- COMMAND ----------

select * from text.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`;

-- COMMAND ----------

select * from binaryFile.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`;

-- COMMAND ----------

select * from csv.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv`;

-- COMMAND ----------

create table book_csv
  (book_id string, title string, author string, category string, price double)
  using csv
  options ( header="true" , delimiter=';')
  location "dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv"

-- COMMAND ----------

select * from book_csv;

-- COMMAND ----------

describe extended book_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files= dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC (spark.read.table('book_csv')
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .format("csv")
-- MAGIC  .option('header','true')
-- MAGIC  .option('delimiter',';')
-- MAGIC  .save("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files= dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

select count(*) from book_csv;

-- COMMAND ----------

refresh table book_csv

-- COMMAND ----------

select count(*) from book_csv;

-- COMMAND ----------

create table customers as
select * from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`;

describe extended customers;

-- COMMAND ----------

create table books_unparsed as
select * from csv.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv`;

-- COMMAND ----------

select * from books_unparsed;

-- COMMAND ----------

create temp view books_tmp_vw
  (book_id string, title string, author string, category string, price double)
  using csv
  options(
    path="dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv/export_*.csv",
    header='true',
    delimiter=';'
  );

  create table books as 
    select * from books_tmp_vw;
  select * from books;

-- COMMAND ----------

describe extended books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Writing to tables

-- COMMAND ----------

create table orders as 
select * from parquet.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders`;

-- COMMAND ----------

select * from orders;

-- COMMAND ----------

create or replace table orders as 
select * from parquet.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders`;

-- COMMAND ----------

describe history orders;

-- COMMAND ----------

-- it overwrite table not create new one
insert overwrite orders
select * from parquet.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders`

-- COMMAND ----------

describe history orders;

-- COMMAND ----------

insert into orders
select * from parquet.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders-new`;

-- COMMAND ----------

select count(*) from orders;

-- COMMAND ----------

create or replace temp view customers_updates as
select * from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json-new`;
merge into customers as c
using customers_updates as u
on c.customer_id=u.customer_id
when matched and c.email is null and u.email is not null then 
  update set email=u.email,updated=u.updated
when not matched then insert *

-- COMMAND ----------

  create or replace temp view books_updates
  (book_id string, title string, author string, category string, price double)
  using csv
  options(
    path="dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv-new",
    header='true',
    delimiter=';'
  );

  select * from books_updates;

-- COMMAND ----------

merge into books as b
using books_updates as u
on b.book_id=u.book_id and b.title=u.title
when not matched and u.category="Computer Science" then 
  insert *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Advanced Transformations

-- COMMAND ----------

select * from customers;

-- COMMAND ----------

describe customers;

-- COMMAND ----------

select customer_id,profile:first_name,profile:address:country from customers;

-- COMMAND ----------

select from_json(profile) as profile_struct
from customers;

-- COMMAND ----------

select profile from customers limit 1;

-- COMMAND ----------

create or replace temp view parsed_customers as 
  select customer_id,from_json(profile,schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) as profile_struct
   from customers;
  select * from parsed_customers;

-- COMMAND ----------

describe parsed_customers;

-- COMMAND ----------

select customer_id, profile_struct.first_name, profile_struct.address.country from parsed_customers

-- COMMAND ----------

create or replace temp view customers_final as 
  select customer_id, profile_struct.*
  from parsed_customers;

select * from customers_final;

-- COMMAND ----------

select order_id, customer_id, books
from orders;

-- COMMAND ----------

-- explode function that allows us to put each element of array on its  own row
select order_id, customer_id, explode(books) as book
from orders;

-- COMMAND ----------

-- collect_set aggregation function that allows us to collect unique values for a field, including fields within arrays
select customer_id,
  collect_set(order_id) as order_set,
  collect_set(books.book_id) as books_set
from orders
group by customer_id; 

-- COMMAND ----------

select customer_id,
  collect_set(order_id) as before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) as array_flatten
from orders
group by customer_id; 

-- COMMAND ----------

create or replace view orders_enriched as 
select * from (
  select *, explode(books) as book
  from orders
) as o
Inner join books as b
on o.book.book_id=b.book_id;

select * from orders_enriched;

-- COMMAND ----------

create or replace temp view orders_updates
as select * from parquet.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders-new`;

select * from orders
union
select * from orders_updates;

-- COMMAND ----------

select * from orders
minus
select * from orders_updates;

-- COMMAND ----------

create or replace table transactions as 

select * from (
  select 
    customer_id,
    book.book_id as book_id,
    book.quantity as quantity
  from orders_enriched 
 ) pivot (
  sum(quantity) for book_id in (
    'B01','B02','B03','B04','B05','B06',
    'B07','B08','B09','B10','B11','B12'
  )
 );

 select * from transactions;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #High order functions and sql udfs

-- COMMAND ----------

select * from orders;

-- COMMAND ----------

select order_id,
  books,
  filter(books,i -> i.quantity>=2) as multiple_copies
from orders;

-- COMMAND ----------

select order_id,multiple_copies
from (
  select
  order_id,
  filter(books,i -> i.quantity >= 2) as multiple_copies
  from orders)
where size(multiple_copies) >0;

-- COMMAND ----------

select 
  order_id,
  books,
  transform (
    books,
    b -> cast(b.subtotal * 0.8 as int)
  ) as subtotal_after_discount
from orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #UDFs

-- COMMAND ----------

create or replace function get_url(email string)
returns string

return concat("https://www.",split(email,"@")[1])

-- COMMAND ----------

select email, get_url(email) as domain
from customers;

-- COMMAND ----------

 describe function get_url

-- COMMAND ----------

 describe function extended get_url

-- COMMAND ----------

create function site_type (email string)
returns string
return case
          when email like "%.com" then "Commercial business"
          when email like "%.org" then "Non-profits organization"
          when email like "%.edu" then "Educational institution"
          else concat("unfollow extension for domain:",split(email,"@")[1])
       End;

-- COMMAND ----------

select email, site_type(email) as domain_category
from customers;

-- COMMAND ----------

drop function get_url;
drop function site_type;

-- COMMAND ----------


