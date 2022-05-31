# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Variaveis
bronze_path_base_vendas = "dbfs:/mnt/bronze/base_vendas"

silver_path_tabela1 = "dbfs:/mnt/silver/tabela1_ano_mes"

silver_path_tabela2 = "dbfs:/mnt/silver/tabela2_marca_linha"

silver_path_tabela3 = "dbfs:/mnt/silver/tabela3_marca_ano_mes"

silver_path_tabela4 = "dbfs:/mnt/silver/tabela4_linha_ano_mes"

silver_path_marca_linha = "dbfs:/mnt/silver/marca_linha"

silver_path_vendas = "dbfs:/mnt/silver/vendas"

# COMMAND ----------

df_bronze_base_vendas = spark.read.format("delta").load(bronze_path_base_vendas)

# COMMAND ----------

# DBTITLE 1,Dedup
particao_dedup = Window.partitionBy("ID_MARCA", "MARCA", "ID_LINHA", "LINHA", "DATA_VENDA").orderBy(col("QTD_VENDA").cast("integer").desc())

df_bronze_base_vendas_dedup = df_bronze_base_vendas\
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
  .drop("ID_MARCA", "MARCA", "ID_LINHA", "LINHA", "DATA_VENDA", "QTD_VENDA", "dedup")

#df_bronze_base_vendas_dedup.display()

# COMMAND ----------

# DBTITLE 1,Tabela1
df_tabela1 = df_bronze_base_vendas_dedup\
  .groupBy("ano", "mes").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela1\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_tabela1)

# COMMAND ----------

# DBTITLE 1,Tabela2
df_tabela2 = df_bronze_base_vendas_dedup\
  .groupBy("str_marca", "str_linha").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela2\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_tabela2)

# COMMAND ----------

# DBTITLE 1,Tabela3
df_tabela3 = df_bronze_base_vendas_dedup\
  .groupBy("str_marca", "ano", "mes").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela3\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_tabela3)

# COMMAND ----------

# DBTITLE 1,Tabela4
df_tabela4 = df_bronze_base_vendas_dedup\
  .groupBy("str_linha", "ano", "mes").agg(sum(col("int_qtd_venda")).alias("qtd"))

df_tabela4\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_tabela4)

# COMMAND ----------

# DBTITLE 1,Tabela Marca Linha
df_marca_linha = df_bronze_base_vendas_dedup\
  .withColumn("chave", concat(col("str_marca"), lit(" - "), col("str_linha")))\
  .select("chave", "int_marca", "str_marca", "int_linha", "str_linha").distinct()

df_marca_linha\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_marca_linha)

# COMMAND ----------

# DBTITLE 1,Tabela Vendas
df_vendas = df_bronze_base_vendas_dedup\
  .withColumn("chave", concat(col("str_marca"), lit(" - "), col("str_linha")))\
  .select("chave", "int_marca", "str_marca", "int_linha", "str_linha", "ano", "mes", "dt_venda", "int_qtd_venda")\
  .distinct()

df_vendas\
  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save(silver_path_vendas)
