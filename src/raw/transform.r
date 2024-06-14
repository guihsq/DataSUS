# Databricks notebook source
install.packages("read.dbc")

# COMMAND ----------

library (read.dbc)

dbc_folder = "/dbfs/mnt/datalake/DataSUS/rd/dbc"
csv_folder = "/dbfs/mnt/datalake/DataSUS/rd/csv"

files = list.files(dbc_folder, full.names=TRUE)
for(i in files) {
  print(i)
  df=read.dbc(i)
  pathsplited = strsplit(i, "/")[[1]]
  file = gsub(".dbc", ".csv", pathsplited[length(pathsplited)])
  write.csv2(df, paste(csv_folder, file, sep="/"), row.names=FALSE)
}



# COMMAND ----------


