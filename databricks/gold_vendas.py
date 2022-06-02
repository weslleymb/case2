# Databricks notebook source
# DBTITLE 1,Bibliotecas
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Variaveis
silver_path_vendas = "dbfs:/mnt/silver/vendas"

gold_path_tabela1 = "dbfs:/mnt/gold/tabela1_ano_mes"

gold_path_tabela2 = "dbfs:/mnt/gold/tabela2_marca_linha"

gold_path_tabela3 = "dbfs:/mnt/gold/tabela3_marca_ano_mes"

gold_path_tabela4 = "dbfs:/mnt/gold/tabela4_linha_ano_mes"

gold_path_marca_linha = "dbfs:/mnt/gold/marca_linha"

gold_path_vendas = "dbfs:/mnt/gold/vendas"

# COMMAND ----------

# DBTITLE 1,Funcoes UDF
# MAGIC %run "./funcoes"

# COMMAND ----------

# DBTITLE 1,Checagem arquivo origem
if arquivo_existe(silver_path_vendas) == False:
  dbutils.notebook.exit('stop')
else:
  df_silver_vendas = spark.read.format("delta").load(silver_path_vendas)
  if (df_silver_vendas.count()==0): 
    dbutils.notebook.exit('stop')

# COMMAND ----------

# MAGIC %md
# MAGIC #Entregas

# COMMAND ----------

# DBTITLE 1,Tabela1: Consolidado de vendas por ano e mês
df_tabela1 = df_silver_vendas\
  .groupBy("ano", "mes").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela1\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .save(gold_path_tabela1)

# COMMAND ----------

# DBTITLE 1,Tabela2: Consolidado de vendas por marca e linha;
df_tabela2 = df_silver_vendas\
  .groupBy("str_marca", "str_linha").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela2\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .save(gold_path_tabela2)

# COMMAND ----------

# DBTITLE 1,Tabela3: Consolidado de vendas por marca, ano e mês;
df_tabela3 = df_silver_vendas\
  .groupBy("str_marca", "ano", "mes").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela3\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .save(gold_path_tabela3)

# COMMAND ----------

# DBTITLE 1,Tabela4: Consolidado de vendas por linha, ano e mês;
df_tabela4 = df_silver_vendas\
  .groupBy("str_linha", "ano", "mes").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela4\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .save(gold_path_tabela4)

# COMMAND ----------

# DBTITLE 1,Modelo DW - Tabela Marca e Linha
df_marca_linha = df_silver_vendas\
  .select("chave", "int_marca", "str_marca", "int_linha", "str_linha")\
  .distinct()

df_marca_linha\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(gold_path_marca_linha)

# COMMAND ----------

# DBTITLE 1,Modelo DW - Tabela Vendas
df_vendas = df_silver_vendas.distinct()

df_vendas\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .save(gold_path_vendas)
