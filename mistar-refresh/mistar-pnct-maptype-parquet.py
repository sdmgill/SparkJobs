#******************************************************************************
#    Section:     Sparkjobs - n4
#
#    Work Item:   mistar-pnct-maptype-parquet.py
#
#    Purpose:     Spark job that pulls in PNCT data from
#				  staging combined and populates maptype parquet table.
#
#    Modifications (Latest entry on first line)
#    Date           		Name                   Revision Notes
#    -----------   	 	--------------------   --------------------------
#    12/25/2019     	Alex Sims            		Original
#  ******************************************************************************


from pyspark.sql.functions import expr,greatest
from pyspark.sql import SparkSession
import boto3
import sys
if (len(sys.argv) < 4):
    print('Please provide database and output path as arguments')
    exit(1)

databaseName = sys.argv[1]
databaseName2 = sys.argv[2]
pdsdataOpPath = sys.argv[3]

spark = SparkSession.builder.appName('maptype').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

# read in the initial data (PNCT)
rdDF = spark.sql("SELECT DISTINCT sourcesystem, \
								max(audtdateadded) audtdateadded, \
								maptypeid, \
								description \
                            FROM "+databaseName+".maptype \
                            WHERE dboperationtype <> 'D' \
                            GROUP BY sourcesystem, \
                               maptypeid, \
								description")

# Bring in distinct only
dist_df = rdDF.distinct()

# write out newdata
dist_df.write.partitionBy('sourcesystem').parquet(pdsdataOpPath, mode="overwrite")


# Updating Athena Partitions
spark.sql("MSCK REPAIR TABLE "+databaseName2+".maptype")