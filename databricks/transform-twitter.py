# Databricks notebook source
# DBTITLE 1,Bibliotecas
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Variaveis
bronze_path_tweets = "dbfs:/mnt/bronze/tweets"

silver_path_tweets = "dbfs:/mnt/silver/tweets"

# COMMAND ----------

# DBTITLE 1,Funcoes UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Checagem arquivo origem
if arquivo_existe(bronze_path_tweets) == False:
  dbutils.notebook.exit('stop')
else:
  df_bronze_tweets = spark.read.format("delta").load(bronze_path_tweets)
  if (df_bronze_tweets.count()==0): 
    dbutils.notebook.exit('stop')

# COMMAND ----------

# DBTITLE 1,Chaveamento e Armazenamento
df_bronze_tweets_chaveado = df_bronze_tweets\
  .withColumn("chave", concat(col("marca"), lit(" - "), col("linha")))\
  .withColumn("msg", regexp_replace(col("mensagem"), "[\n\r]", " "))\
  .drop("mensagem")\
  .select("chave", "id", "usuario", "msg", "data", "marca", "linha", "arquivo_origem", "data_carga")

df_bronze_tweets_chaveado\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_tweets)
