#******************************************************************************
#    Section:     Sparkjobs - n4
#
#    Work Item:   mistar-pnct-roverdetails-parquet.py
#
#    Purpose:     Spark job that pulls in PNCT data from
#				  staging combined and populates roverdetails parquet table.
#
#    Modifications (Latest entry on first line)
#    Date           		Name                   Revision Notes
#    -----------   	 	--------------------   --------------------------
#    01/29/2019     	Alex Sims            		Original
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

spark = SparkSession.builder.appName('roverdetails').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

# read in the initial data (PNCT)
rdDF = spark.sql("SELECT DISTINCT \
								   sourcesystem, \
								   mjid, \
								   chid, \
								   lastdetailtimestamp, \
                                   concat(year(coalesce(lastdetailtimestamp, cast('1900-01-01' as DATE))),right( '0' || month(coalesce(lastdetailtimestamp, cast('1900-01-01' as DATE))),2)) yrmon, \
								   from_utc_timestamp(lastdetailtimestamp, 'America/New_York') sourcesystemtimestamp, \
								   ipaddress, \
								   softwareversion, \
								   navversion, \
								   updateversion, \
								   gpshwversion1, \
								   gpshwversion2, \
								   powerhwversion, \
								   powerfwversion, \
								   coalesce(startuptimestamp,cast('1900-01-01' as timestamp)) startuptimestamp, \
								   fleetmanagementversion, \
								   keepaliveversion, \
								   vehicleviewversion, \
								   bootloaderfwversion, \
								   gyroserialnumber, \
								   flags, \
								   navbootloaderversion \
                            FROM "+databaseName+".roverdetails \
                            WHERE dboperationtype <> 'D'")

# N4 updates to PNCT
maxrdDF = spark.sql("SELECT mjid, \
                    MAX(lastdetailtimestamp) lastdetailtimestamp, \
                    coalesce(max(startuptimestamp),cast('1900-01-01' as timestamp)) startuptimestamp \
                    FROM "+databaseName+".roverdetails \
                    WHERE dboperationtype <> 'D' \
                    GROUP BY mjid")

# Join initial to updates
joinDF = rdDF.join(maxrdDF, (rdDF.mjid == maxrdDF.mjid) & (rdDF.lastdetailtimestamp == maxrdDF.lastdetailtimestamp)  \
                    & (rdDF.startuptimestamp == maxrdDF.startuptimestamp)).drop(maxrdDF.mjid).drop(maxrdDF.lastdetailtimestamp).drop(maxrdDF.startuptimestamp)

# Bring in distinct only
dist_df = rdDF.distinct()

# write out newdata
dist_df.write.partitionBy('yrmon').parquet(pdsdataOpPath, mode="overwrite")


# Updating Athena Partitions
spark.sql("MSCK REPAIR TABLE "+databaseName2+".roverdetails")