#******************************************************************************
#    Section:     Sparkjobs - n4
#
#    Work Item:   mistar-pnct-vehicledetails-parquet.py
#
#    Purpose:     Spark job that pulls in PNCT data from
#				  staging combined and populates vehicledetails parquet table.
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

spark = SparkSession.builder.appName('vehicledetails').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

# read in the initial data (PNCT)
rdDF = spark.sql("SELECT sourcesystem, \
                            mjid, \
                            chid, \
                            coalesce(lastdetailtimestamp, cast('1900-01-01' as timestamp)) lastdetailtimestamp, \
                            concat(year(coalesce(lastdetailtimestamp, cast('1900-01-01' as DATE))),right( '0' || month(coalesce(lastdetailtimestamp, cast('1900-01-01' as DATE))),2)) yrmon, \
                            coalesce(lastfueltimestamp, cast('1900-01-01' as timestamp)) lastfueltimestamp, \
                            coalesce(lastrefueltimestamp, cast('1900-01-01' as timestamp)) lastrefueltimestamp, \
                            coalesce(lasttiretimestamp, cast('1900-01-01' as timestamp)) lasttiretimestamp, \
                            coalesce(lastimpacttimestamp, cast('1900-01-01' as timestamp)) lastimpacttimestamp, \
                            vehicletypeid, \
                            coalesce(vehiclesubtypeid, 0) vehiclesubtypeid, \
                            trailcolor, \
                            status, \
                            tosserveripaddress, \
                            coalesce(lastplctimestamp, cast('1900-01-01' as timestamp)) lastplctimestamp \
                    FROM "+databaseName+".vehicledetails \
                    WHERE dboperationtype <> 'D'")

# N4 updates to PNCT
maxrdDF = spark.sql("SELECT mjid, \
                            coalesce(max(lastdetailtimestamp), cast('1900-01-01' as timestamp)) lastdetailtimestamp, \
                            coalesce(max(lastfueltimestamp), cast('1900-01-01' as timestamp)) lastfueltimestamp, \
                            coalesce(max(lastrefueltimestamp), cast('1900-01-01' as timestamp)) lastrefueltimestamp, \
                            coalesce(max(lasttiretimestamp), cast('1900-01-01' as timestamp)) lasttiretimestamp, \
                            coalesce(max(lastimpacttimestamp), cast('1900-01-01' as timestamp)) lastimpacttimestamp, \
                            coalesce(max(vehiclesubtypeid), 0) vehiclesubtypeid, \
                            coalesce(max(lastplctimestamp), cast('1900-01-01' as timestamp)) lastplctimestamp \
                    FROM "+databaseName+".vehicledetails \
                    WHERE dboperationtype <> 'D' \
                    GROUP BY mjid")

# Join initial to updates
joinDF = rdDF.join(maxrdDF, (rdDF.mjid == maxrdDF.mjid) & (rdDF.lastdetailtimestamp == maxrdDF.lastdetailtimestamp) & \
                   (rdDF.lastfueltimestamp == maxrdDF.lastfueltimestamp) & (rdDF.lastrefueltimestamp == maxrdDF.lastrefueltimestamp) & \
                   (rdDF.lasttiretimestamp == maxrdDF.lasttiretimestamp) & (rdDF.lastimpacttimestamp == maxrdDF.lastimpacttimestamp) & \
                   (rdDF.vehiclesubtypeid == maxrdDF.vehiclesubtypeid) & (rdDF.lastplctimestamp == maxrdDF.lastplctimestamp))\
    .drop(maxrdDF.mjid).drop(maxrdDF.lastdetailtimestamp).drop(maxrdDF.lastfueltimestamp).drop(maxrdDF.lastrefueltimestamp)\
    .drop(maxrdDF.lasttiretimestamp).drop(maxrdDF.lastimpacttimestamp).drop(maxrdDF.vehiclesubtypeid).drop(maxrdDF.lastplctimestamp)

# Bring in distinct only
dist_df = joinDF.distinct()

# write out newdata
dist_df.write.partitionBy('yrmon').parquet(pdsdataOpPath, mode="overwrite")


# Updating Athena Partitions
spark.sql("MSCK REPAIR TABLE "+databaseName2+".vehicledetails")