# Databricks notebook source

import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze
import dbtools

table = "rd_sih"
path_full_load=f'/mnt/datalake/DataSUS/rd/csv'
path_incremental=f'/mnt/datalake/DataSUS/rd/csv'
file_format='csv'
table_name=table
database_name='bronze.datasus'
id_fields= ["N_AIH", "DT_SAIDA", "IDENT"]
timestamp_field='DT_SAIDA'
partition_fields=["ANO_CMPT", "MES_CMPT"]
read_options = {'sep': ';', 'header': "true"}

ingestao = IngestaoBronze(
            path_full_load=path_full_load,
            path_incremental=path_incremental,
            file_format=file_format,
            table_name=table_name,
            database_name=database_name,
            id_fields=id_fields,
            timestamp_field=timestamp_field,
            partition_fields=partition_fields,
            read_options=read_options,
            spark=spark,
)



# COMMAND ----------

if not dbtools.table_exists(spark, database_name, table):
    df_null = spark.createDataFrame(data=[], schema=ingestao.schema)
    ingestao.save_full(df_null)
    dbutils.fs.rm(ingestao.checkpoint_path, True)



# COMMAND ----------

ingestao.process_stream()

# COMMAND ----------

import os

os.listdir("/dbfs/mnt/datalake/DataSUS/rd")
