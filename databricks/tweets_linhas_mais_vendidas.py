# Databricks notebook source
# DBTITLE 1,Instalacao biblioteca Twitter
pip install tweepy

# COMMAND ----------

# DBTITLE 1,Importacao Bibliotecas
import tweepy
import pandas as pd
import pytz
from datetime import datetime
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Variaveis
dbutils.widgets.text("input", "","")
param = dbutils.widgets.get("input")

raw_consumer_key = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-key")

raw_consumer_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-secret")

raw_access_token = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token")

raw_access_token_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token-secret")

var_timezone = pytz.timezone('America/Sao_Paulo')

var_data_carga = datetime.now(tz=var_timezone)

gold_path_tweets_linhas_mais_vendidas = "dbfs:/mnt/goldcase2/tweets_linhas_mais_vendidas"

# COMMAND ----------

autorizar = tweepy.OAuthHandler(raw_consumer_key, raw_consumer_secret)

autorizar.set_access_token(raw_access_token, raw_access_token_secret)

api = tweepy.API(autorizar)

pd_process_tweets = pd.DataFrame(columns = ["id", "usuario", "mensagem", "data", "linha"])

query = 'Boticário ' + param

resultados = api.search_tweets(q=query, lang='pt-br', count=50)

for tweet in resultados:

  pd_process_tweets = pd_process_tweets.append({'id':tweet.id, 'usuario':tweet.user.name, 'mensagem':tweet.text, 'data':tweet.created_at, 'linha':param}, ignore_index=True)
    
df_process_tweets = spark.createDataFrame(pd_process_tweets)

# COMMAND ----------

# DBTITLE 1,Tratamento
df_process_tweets_tratado = df_process_tweets\
  .withColumn("msg", regexp_replace(col("mensagem"), "[\n\r]", " "))\
  .select("id", "usuario", "msg", col("data").cast("date").alias("dt_tweet"), col("linha").alias("str_linha"))

# COMMAND ----------

# DBTITLE 1,Armazenamento
df_process_tweets_tratado\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .save(gold_path_tweets_linhas_mais_vendidas)
