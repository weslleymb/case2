# Databricks notebook source
# DBTITLE 1,Bibliotecas
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Variaveis
silver_path_tweets = "dbfs:/mnt/silver/tweets"

gold_path_tweets = "dbfs:/mnt/silver/tweets"

# COMMAND ----------

# DBTITLE 1,Funcoes UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Checagem arquivo origem
if arquivo_existe(silver_path_tweets) == False:
  dbutils.notebook.exit('stop')
else:
  df_silver_tweets = spark.read.format("delta").load(silver_path_tweets)
  if (df_silver_tweets.count()==0): 
    dbutils.notebook.exit('stop')

# COMMAND ----------

# DBTITLE 1,Novos Tweets
if arquivo_existe(gold_path_tweets):
  df_gold_tweets = spark.read.format("delta").load(gold_path_tweets)
  df_process_tweets = df_silver_tweets.join(df_gold_tweets, on=[df_silver_tweets.id == df_gold_tweets.id], how='leftanti')
else:
  df_process_tweets = df_silver_tweets

# COMMAND ----------


