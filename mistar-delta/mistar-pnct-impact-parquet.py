from pyspark.sql.functions import expr,greatest
from pyspark.sql import SparkSession
import boto3
import sys



if (len(sys.argv) < 4):
    print('Please provide database and output path  as arguments')
    exit(1)

databaseName = sys.argv[1]
databaseName2 = sys.argv[2]
pdsdataOpPath = sys.argv[3]


spark = SparkSession.builder.appName('impact').config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").config("spark.sql.broadcastTimeout","1200").enableHiveSupport().getOrCreate()

#grab max values
maxSSDate = spark.sql("select coalesce(max(sourcesystemdate),cast('2017-07-31' as string)) as maxssdate from "+databaseName2+".impact").collect()[0].asDict()['maxssdate']
lastIngestionDate=spark.sql("select coalesce(max(rovertimestamp), cast('2017-07-31' as timestamp)) AS maxrover from "+databaseName2+".impact where sourcesystemdate='"+maxSSDate+"'").collect()[0].asDict()['maxrover']

#read in the staging data
im_df = spark.sql("SELECT *, from_utc_timestamp(rovertimestamp,'America/New_York') sourcesystemtimestamp, \
                        cast(from_utc_timestamp(rovertimestamp,'America/New_York') as date) sourcesystemdate \
                    FROM "+databaseName+".impact\
                    WHERE dboperationtype <> 'D'")

#Bring in Deltas only
imD_df = im_df.where(im_df['sourcesystemdate']>=maxSSDate).where(im_df['rovertimestamp'] > lastIngestionDate) \
    .drop(im_df.audtdateadded).drop(im_df.dboperationtype)

dist_df = imD_df.distinct()

#write out newdata
dist_df.write.partitionBy("sourcesystemdate").parquet(pdsdataOpPath,mode="append")

#bring new partitions into Athena
spark.sql("MSCK REPAIR TABLE "+databaseName2+".impact")