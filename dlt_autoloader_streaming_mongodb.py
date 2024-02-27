

#-- CREATE STREAMING LIVE TABLE conlibv_pfaascdlr_bronze
#-- AS SELECT * FROM cloud_files('s3://databricks-e2demofieldengwest/ad_auction_edge/conlibv_pfaascdlr/', 'parquet', map("cloudFiles.inferColumnTypes", "true"))

import dlt

@dlt.table
def conlibv_pfaascdlr_bronze():
  df = spark.read.format("parquet").load("s3://databricks-e2demofieldengwest/ad_auction_edge/conlibv_pfaascdlr/")
  return (
  df.dropDuplicates()
)


import dlt

@dlt.table
def conlibv_pfvehfile_bronze():
  df = spark.read.format("parquet").load("s3://databricks-e2demofieldengwest/ad_auction_edge/conlibv_pfvehfile/")
  return (
  df.dropDuplicates()
)
  



#import dlt

#@dlt.table
#def conlibv_pfvehfile_bronze_new():
#  df = spark.readStream.format("cloudFiles") \
#    .option("cloudFiles.format", "parquet") \
#    .option("cloudFiles.useNotifications","true") \
#    .load("s3://ad-demo-bucket-24/auction_edge/conlibv_pfvehfile/")
#  return (
#    df.dropDuplicates()
#)

connectionString='mongodb+srv://ad-mongo-user:SxihFrDkzUxX80Fg@cluster0.xjzrcs2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
database="databricks-db1"
collection="collection1"

#ad-mongo-user
#SxihFrDkzUxX80Fg
#mongodb+srv://ad-mongo-user:SxihFrDkzUxX80Fg@cluster0.xjzrcs2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0

#display(df)
df.write.format("mongo").option("spark.mongodb.output.uri", connectionString).option("database",database).option("collection","filteredSales").mode("append").save()


