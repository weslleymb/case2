# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

bronze_path_tweets = "dbfs:/mnt/bronze/tweets"

silver_path_tweets = "dbfs:/mnt/silver/vendas"

# COMMAND ----------

df_bronze_tweets = spark.read.format("delta").load(bronze_path_tweets)

df_bronze_tweets\
  .withColumn("chave", concat(col("marca"), lit(" - "), col("linha")))\
  .withColumn("msg", regexp_replace(col("mensagem"), "[\n\r]", " "))\
  .select("chave", "id", "usuario", "msg", "data", "arquivo_origem")

df_bronze_tweets\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_tweets)
