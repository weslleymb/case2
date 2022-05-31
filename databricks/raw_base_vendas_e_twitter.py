# Databricks notebook source
# DBTITLE 1,Instalacao biblioteca Twitter
pip install tweepy

# COMMAND ----------

# DBTITLE 1,Importacao - UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Importacao bibliotecas
import tweepy
import pandas as pd
from pyspark.sql.functions import *
import pytz
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Variaveis
dbutils.widgets.text("input", "","")
param = dbutils.widgets.get("input")

raw_path_storage = "dbfs:/mnt/raw/"

raw_path_recurso = raw_path_storage + param

raw_consumer_key = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-key")

raw_consumer_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-secret")

raw_access_token = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token")

raw_access_token_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token-secret")

bronze_tweets = "dbfs:/mnt/bronze/tweets"

bronze_base_vendas = "dbfs:/mnt/bronze/base_vendas"

var_timezone = pytz.timezone('America/Sao_Paulo')

var_data_carga = datetime.now(tz=var_timezone)

# COMMAND ----------

# DBTITLE 1,Gera base de "linhas" para pesquisa 
df_base_vendas = spark.read.csv(raw_path_recurso, header='true')

df_pesquisa = df_base_vendas.select("MARCA", "LINHA").distinct()

pd_pesquisa = df_pesquisa.toPandas()

# COMMAND ----------

# DBTITLE 1,Extracao tweets
autorizar = tweepy.OAuthHandler(raw_consumer_key, raw_consumer_secret)

autorizar.set_access_token(raw_access_token, raw_access_token_secret)

api = tweepy.API(autorizar)

pd_tweets = pd.DataFrame(columns = ["id", "usuario", "mensagem", "data", "marca", "linha", "arquivo_origem", "data_carga"])

for marca, linha in pd_pesquisa.itertuples(index=False):
  
  query = marca + ' ' + linha

  resultados = api.search_tweets(q=query, lang='pt-br', count=50)

  for tweet in resultados:
    
    pd_tweets = pd_tweets.append({'id':tweet.id, 'usuario':tweet.user.name, 'mensagem':tweet.text, 'data':tweet.created_at, 'marca':marca, 'linha':linha, 'arquivo_origem':param, 'data_carga':var_data_carga}, ignore_index=True)
    
df_tweets = spark.createDataFrame(pd_tweets)

#df_tweets.display()

# COMMAND ----------

# DBTITLE 1,Base Vendas com Referencias
df_base_vendas_referencia = df_base_vendas\
  .withColumn("arquivo_origem", lit(param))\
  .withColumn("data_carga", lit(var_data_carga))

# COMMAND ----------

# DBTITLE 1,Armazenamento tweets
if arquivo_existe(raw_path_recurso):
  df_tweets\
    .write\
    .format("delta")\
    .mode("append")\
    .option("overwriteSchema", "true")\
    .save(bronze_tweets)
else:
  df_tweets\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save(bronze_tweets)

# COMMAND ----------

# DBTITLE 1,Armazenamento base vendas
if arquivo_existe(raw_path_recurso):
  df_base_vendas_referencia\
    .write\
    .format("delta")\
    .mode("append")\
    .option("overwriteSchema", "true")\
    .save(bronze_base_vendas)
else:
  df_base_vendas_referencia\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save(bronze_base_vendas)
