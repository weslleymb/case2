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
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Variaveis
dbutils.widgets.text("input", "","")
param = dbutils.widgets.get("input")

raw_path_storage = "dbfs:/mnt/raw/"

raw_path_arquivo = raw_path_storage + param

raw_consumer_key = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-key")

raw_consumer_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-api-secret")

raw_access_token = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token")

raw_access_token_secret = dbutils.secrets.get(scope = "dbw-kv-scope", key = "twitter-access-token-secret")

bronze_path_tweets = "dbfs:/mnt/bronze/tweets"

bronze_path_vendas = "dbfs:/mnt/bronze/vendas"

var_timezone = pytz.timezone('America/Sao_Paulo')

var_data_carga = datetime.now(tz=var_timezone)

# COMMAND ----------

# DBTITLE 1,Gera base de "linhas" para pesquisa 
df_raw_vendas = spark.read.csv(raw_path_arquivo, header='true')

df_process_pesquisa = df_raw_vendas.select("MARCA", "LINHA").distinct()

pd_process_pesquisa = df_process_pesquisa.toPandas()

# COMMAND ----------

# DBTITLE 1,Extracao tweets
autorizar = tweepy.OAuthHandler(raw_consumer_key, raw_consumer_secret)

autorizar.set_access_token(raw_access_token, raw_access_token_secret)

api = tweepy.API(autorizar)

pd_process_tweets = pd.DataFrame(columns = ["id", "usuario", "mensagem", "data", "marca", "linha", "arquivo_origem", "data_carga"])

for marca, linha in pd_process_pesquisa.itertuples(index=False):
  
  query = marca + ' ' + linha

  resultados = api.search_tweets(q=query, lang='pt-br', count=50)

  for tweet in resultados:
    
    pd_process_tweets = pd_process_tweets.append({'id':tweet.id, 'usuario':tweet.user.name, 'mensagem':tweet.text, 'data':tweet.created_at, 'marca':marca, 'linha':linha, 'arquivo_origem':param, 'data_carga':var_data_carga}, ignore_index=True)
    
df_process_tweets = spark.createDataFrame(pd_process_tweets)

#df_process_tweets.display()

# COMMAND ----------

# DBTITLE 1,Base Vendas com Referencias
df_process_vendas_referenciada = df_raw_vendas\
  .withColumn("arquivo_origem", lit(param))\
  .withColumn("data_carga", lit(var_data_carga))

#df_process_vendas_referenciada.display()

# COMMAND ----------

# DBTITLE 1,Armazenamento base vendas
if arquivo_existe(bronze_path_vendas) == False:
  df_process_vendas_referenciada\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .save(bronze_path_vendas)
else:
  df_destino = DeltaTable.forPath(spark, bronze_path_vendas)
  df_destino.alias("destino")\
    .merge(
      df_process_vendas_referenciada.alias("origem"), """
        destino.ID_MARCA = origem.ID_MARCA 
        and destino.MARCA = origem.MARCA 
        and destino.ID_LINHA = origem.ID_LINHA
        and destino.LINHA = origem.LINHA
        and destino.DATA_VENDA = origem.DATA_VENDA
        and destino.arquivo_origem = origem.arquivo_origem
      """
    )\
    .whenNotMatchedInsert(values = {
      "ID_MARCA": "origem.ID_MARCA",
      "MARCA": "origem.MARCA",
      "ID_LINHA": "origem.ID_LINHA", 
      "LINHA": "origem.LINHA", 
      "DATA_VENDA": "origem.DATA_VENDA",
      "QTD_VENDA": "origem.QTD_VENDA",
      "arquivo_origem": "origem.arquivo_origem",
      "data_carga": "origem.data_carga"
    })\
    .execute()

# COMMAND ----------

# DBTITLE 1,Armazenamento tweets
if arquivo_existe(bronze_path_tweets) == False:
  df_process_tweets\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .save(bronze_path_tweets)
else:
  df_destino = DeltaTable.forPath(spark, bronze_path_tweets)
  df_destino.alias("destino")\
    .merge(
      df_process_tweets.alias("origem"), """
        destino.id = origem.id 
        and destino.usuario = origem.usuario 
        and destino.mensagem = origem.mensagem
        and destino.data = origem.data
        and destino.marca = origem.marca
        and destino.linha = origem.linha
        and destino.arquivo_origem = origem.arquivo_origem
        and destino.data_carga = origem.data_carga
      """
    )\
    .whenNotMatchedInsert(values = {
      "id": "origem.id",
      "usuario": "origem.usuario",
      "mensagem": "origem.mensagem", 
      "data": "origem.data", 
      "marca": "origem.marca",
      "linha": "origem.linha",
      "arquivo_origem": "origem.arquivo_origem",
      "data_carga": "origem.data_carga"
    })\
    .execute()
