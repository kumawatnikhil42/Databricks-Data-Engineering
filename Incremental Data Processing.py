# Databricks notebook source
files= dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists customers2 as
# MAGIC select * from json.`dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json`;
# MAGIC
# MAGIC describe extended customers2;

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view if not exists books_tmp_vw
# MAGIC   (book_id string, title string, author string, category string, price double)
# MAGIC   using csv
# MAGIC   options(
# MAGIC     path="dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/books-csv/export_*.csv",
# MAGIC     header='true',
# MAGIC     delimiter=';'
# MAGIC   );
# MAGIC
# MAGIC   create table books1 as 
# MAGIC     select * from books_tmp_vw;
# MAGIC   select * from books1;

# COMMAND ----------

(spark.readStream
      .table("books1")
      .createOrReplaceTempView("books_streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(book_id) as total_books
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author

# COMMAND ----------

# %sql
# select * 
# from books_streaming_tmp_vw
# order by author

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view author_counts_tmp_vw as (
# MAGIC   select author, count(book_id) as total_books
# MAGIC   from books_streaming_tmp_vw
# MAGIC   group by author
# MAGIC )

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(processingTime="4 seconds")
      .outputMode("complete")
      .option("checkpointlocation","dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/author_counts_checkpoint")
      .table("author_counts")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into books1
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC          ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC       ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books1
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC          ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC          ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointlocation","dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts;

# COMMAND ----------

# MAGIC %md
# MAGIC #AutoLoader

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointlocation", "dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates;

# COMMAND ----------

load_new_data()

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe history orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_updates;

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders_checkpoint",True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Multi-Hop Architecture

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
      .format("cloudFiles")
      .option("cloudfiles.format",'parquet')
      .option("cloudFiles.schemalocation","dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/checkpoints/orders_raw")
      .load("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/orders-raw")
      .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC    SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC    FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from orders_tmp;

# COMMAND ----------

#Creating Bronze Table

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointlocation", "dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze;

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze;

# COMMAND ----------

(spark.read
      .format("json")
      .load("dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/customers-json")
      .createOrReplaceTempView("customers_lookup")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup;

# COMMAND ----------

(spark.readStream
      .table("orders_bronze")
      .createOrReplaceTempView("orders_bronze_tmp")      
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp as o
# MAGIC   INNER JOIN customers_lookup as c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointlocation", "dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver;

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver;

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC )
# MAGIC

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointlocation", "dbfs:/FileStore/shared_uploads/kumawatnikhil42@gmail.com/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books")) #gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

# for stopping all active streams
for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


