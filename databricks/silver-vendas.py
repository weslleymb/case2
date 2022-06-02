# Databricks notebook source
# DBTITLE 1,Bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Variaveis
bronze_path_vendas = "dbfs:/mnt/bronze/vendas"

silver_path_vendas = "dbfs:/mnt/silver/vendas"

# COMMAND ----------

# DBTITLE 1,Funcoes UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Checagem arquivo origem
if arquivo_existe(bronze_path_vendas) == False:
  dbutils.notebook.exit('stop')
else:
  df_bronze_vendas = spark.read.format("delta").load(bronze_path_vendas)
  if (df_bronze_vendas.count()==0): 
    dbutils.notebook.exit('stop')

# COMMAND ----------

# DBTITLE 1,Dedup
particao_dedup = Window.partitionBy("ID_MARCA", "MARCA", "ID_LINHA", "LINHA", "DATA_VENDA").orderBy(col("data_carga").cast("date").desc())

df_process_vendas_dedup = df_bronze_vendas\
  .withColumn("chave", concat(col("str_marca"), lit(" - "), col("str_linha")))\
  .withColumn("int_marca", col("ID_MARCA").cast("integer"))\
  .withColumn("str_marca", trim("MARCA"))\
  .withColumn("int_linha", col("ID_LINHA").cast("integer"))\
  .withColumn("str_linha", trim("LINHA"))\
  .withColumn("ano", year("DATA_VENDA"))\
  .withColumn("mes", month("DATA_VENDA"))\
  .withColumn("dt_venda", to_date("DATA_VENDA"))\
  .withColumn("int_qtd_venda", col("QTD_VENDA").cast("integer"))\
  .withColumn(
    "dedup", 
    row_number()
    .over(particao_dedup)
  )\
  .filter("dedup = 1")\
  .select("chave", "int_marca", "str_marca", "int_linha", "str_linha", "ano", "mes", "dt_venda", "int_qtd_venda", "data_carga")\

#df_process_vendas_dedup.display()

# COMMAND ----------

# DBTITLE 1,Armazenamento
if arquivo_existe(silver_path_vendas) == False:
  df_process_vendas_dedup\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .save(silver_path_vendas)
else:
  df_destino = DeltaTable.forPath(spark, silver_path_vendas)
  df_destino.alias("destino")\
    .merge(
      df_process_vendas_dedup.alias("origem"), """
        destino.chave = origem.chave 
        and destino.int_marca = origem.int_marca 
        and destino.str_marca = origem.str_marca 
        and destino.int_linha = origem.int_linha
        and destino.str_linha = origem.str_linha
        and destino.ano = origem.ano
        and destino.mes = origem.mes
        and destino.dt_venda = origem.dt_venda
      """
    )\
    .whenMatchedUpdate(set = {
      "int_qtd_venda": "origem.int_qtd_venda", 
      "data_carga": "origem.data_carga"
    })\
    .whenNotMatchedInsert(values = {
      "chave": "origem.chave",
      "int_marca": "origem.int_marca",
      "str_marca": "origem.str_marca", 
      "int_linha": "origem.int_linha", 
      "str_linha": "origem.str_linha",
      "ano": "origem.ano",
      "mes": "origem.mes",
      "dt_venda": "origem.dt_venda"
      "int_qtd_venda": "origem.int_qtd_venda"
      "data_carga": "origem.data_carga"
    })\
    .execute()

# COMMAND ----------

# DBTITLE 1,Tabela Vendas

