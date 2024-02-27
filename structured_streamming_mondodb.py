#bronze
file_path = "s3://databricks-e2demofieldengwest/ad_auction_edge/conlibv_pfaascdlr/"
checkpoint_path = "s3://databricks-e2demofieldengwest/ad_auction_edge/check_point/conlibv_pfaascdlr/"

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.schemaEvolutionMode","rescue")
    .load(file_path)
)

display(raw_df)



#silver 
from pyspark.sql.functions import col, current_timestamp

transformed_df = (raw_df.dropDuplicates()
  )

display(transformed_df)


#gold to mongo


target_path = "/tmp/ss-tutorial/"
target_checkpoint_path = "s3://databricks-e2demofieldengwest/ad_auction_edge/check_point/conlibv_pfaascdlr_mongo/"

connectionString='mongodb+srv://ad-mongo-user:SxihFrDkzUxX80Fg@cluster0.xjzrcs2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
database="databricks-db1"
collection="collection1"


transformed_df.writeStream \
  .trigger(availableNow=True) \
  .option("checkpointLocation", target_checkpoint_path) \
  .option("path", target_path) \
  .format("mongo") \
  .option("spark.mongodb.output.uri", connectionString) \
  .option("database",database) \
  .option("collection",collection) \
  .start()

#mongo need single user cluster as its not supported on shared cluster
