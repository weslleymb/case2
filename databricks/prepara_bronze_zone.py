# Databricks notebook source
# MAGIC %run "./funcoes"

# COMMAND ----------

bronze_path_recurso = "dbfs:/mnt/bronze/tweets"

if arquivo_existe(bronze_path_recurso):
  dbutils.fs.rm(bronze_path_recurso, True)

# COMMAND ----------

bronze_path_recurso = "dbfs:/mnt/bronze/tweets"

df = spark.read.load(bronze_path_recurso)
