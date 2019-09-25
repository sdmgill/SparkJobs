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


spark = SparkSession.builder.appName('latch').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

maxSSDate = spark.sql("select coalesce(max(sourcesystemdate),cast('2017-07-31' as string)) as maxssdate from "+databaseName2+".latch").collect()[0].asDict()['maxssdate']
lastIngestionDate=spark.sql("select coalesce(max(rovertimestamp), cast('2017-07-31' as timestamp)) AS maxrover from "+databaseName2+".latch where sourcesystemdate='"+maxSSDate+"'").collect()[0].asDict()['maxrover']

#read in the initial data
la_df = spark.sql("SELECT *, from_utc_timestamp(rovertimestamp,'America/New_York') sourcesystemtimestamp, \
                            cast(from_utc_timestamp(rovertimestamp,'America/New_York') as date) sourcesystemdate \
                    FROM "+databaseName+".latch \
                    WHERE dboperationtype <> 'D'")

#Bring in Deltas only
laD_df = la_df.where(la_df['sourcesystemdate']>=maxSSDate).where(la_df['rovertimestamp'] > lastIngestionDate) \
    .drop(la_df.audtdateadded).drop(la_df.dboperationtype)

dist_df = laD_df.distinct()


#write out newdata
dist_df.write.partitionBy("sourcesystemdate").parquet(pdsdataOpPath,mode="append")


spark.sql("MSCK REPAIR TABLE "+databaseName2+".latch")
