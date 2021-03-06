import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, current_timestamp, expr
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit

if (len(sys.argv) < 4):
    print('Please provide database and output path  as arguments')
    exit(1)

databaseName = sys.argv[1]
databaseName2 = sys.argv[2]
pdsdataOpPath = sys.argv[3]

spark = SparkSession.builder.appName('position').config("hive.metastore.client.factory.class",
                                                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

# grab max values
maxSSDate = spark.sql(
    "select coalesce(max(sourcesystemdate),cast('2017-07-31' as string)) as maxssdate from " + databaseName2 + ".position where sourcesystemdate <= cast(from_utc_timestamp(current_timestamp,'America/New_York') as date)").collect()[
    0].asDict()['maxssdate']
lastIngestionDate = spark.sql(
    "select coalesce(max(servertimestamp), cast('2017-07-31' as timestamp)) AS maxrover from " + databaseName2 + ".position where sourcesystemdate='" + maxSSDate + "'").collect()[
    0].asDict()['maxrover']

# read in the staging data
pos_DF = spark.sql("SELECT *, from_utc_timestamp(servertimestamp,'America/New_York') sourcesystemtimestamp, \
                            cast(from_utc_timestamp(servertimestamp,'America/New_York') as date) sourcesystemdate \
                    FROM " + databaseName + ".position \
                    WHERE dboperationtype <> 'D'")

# Bring in Deltas only
posD_df = pos_DF.where(pos_DF['sourcesystemdate'] >= maxSSDate).where(pos_DF['servertimestamp'] > lastIngestionDate) \
    .drop(pos_DF.audtdateadded).drop(pos_DF.dboperationtype)

dist_df = posD_df.distinct()

# write out newdata
dist_df.write.partitionBy("sourcesystemdate").parquet(pdsdataOpPath, mode="append")

# bring new partitions into Athena
spark.sql("MSCK REPAIR TABLE " + databaseName2 + ".position")
