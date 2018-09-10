from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('flatFileGeneration3').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Getting Last ingestion date

lastIngestionDate_df=spark.sql("select coalesce(max(rovertimestamp), cast('2018-08-01' as timestamp)) AS maxrover from parquet.pdsdataextraction")

# Reading the data
pos_df = spark.sql("SELECT rovertimestamp,sourcesystemtimestamp,sourcesystemdate,latchtypeid,vehicletypeid,mjid,chid,azimuth,localx,localy,lot,spreaderazimuth,spreaderx,spreadery,velocity,spreaderlength,spreaderheight FROM parquet.positionenh")
latch_df = spark.sql("SELECT rovertimestamp as latchtimestamp,mjid,chid,latchtypeid,containerlot,containerrow,containertier,container1spot,container1id,container2spot,container2id,matchedchid,distancetravelled,duration,flags as latchflags FROM parquet.latch")
fuel_df = spark.sql("SELECT rovertimestamp as lastfueltimestamp,mjid,chid,level1 as fuellevel1,level2 as fuellevel2 FROM parquet.fuel")
impact_df = spark.sql("SELECT rovertimestamp as impacttimestamp,mjid,chid,sensorid1,ximpact1,yimpact1,zimpact1,sensorid2,ximpact2,yimpact2,zimpact2,sensorid3,ximpact3,yimpact3,zimpact3,sensorid4,ximpact4,yimpact4,zimpact4,flags as impactflags FROM parquet.impact")
latchtype_df = spark.sql("SELECT latchtypeid,description AS spreaderstate FROM parquet.latchtype")
vehtype_df = spark.sql("SELECT vehicletypeid,description AS vehicletype FROM parquet.vehicletype")

# Only Bring in Deltas
posF_df=pos_df.join(lastIngestionDate_df,pos_df.rovertimestamp > lastIngestionDate_df.maxrover).drop(lastIngestionDate_df.maxrover)
latchF_df=latch_df.join(lastIngestionDate_df,latch_df.latchtimestamp > lastIngestionDate_df.maxrover).drop(lastIngestionDate_df.maxrover)
fuelF_df=fuel_df.join(lastIngestionDate_df,fuel_df.lastfueltimestamp > lastIngestionDate_df.maxrover).drop(lastIngestionDate_df.maxrover)
impactF_df=impact_df.join(lastIngestionDate_df,impact_df.impacttimestamp > lastIngestionDate_df.maxrover).drop(lastIngestionDate_df.maxrover)

# Join data
poslt_df = posF_df.join(latchtype_df, posF_df.latchtypeid == latchtype_df.latchtypeid).drop(latchtype_df.latchtypeid).drop(posF_df.latchtypeid)
posltvt_df = poslt_df.join(vehtype_df, poslt_df.vehicletypeid == vehtype_df.vehicletypeid).drop(vehtype_df.vehicletypeid).drop(poslt_df.vehicletypeid)
posltvtla_df = posltvt_df.join(latchF_df, (posltvt_df.mjid == latchF_df.mjid) & (posltvt_df.chid == latchF_df.chid) & (latchF_df.latchtimestamp >= posltvt_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (latchF_df.latchtimestamp <= posltvt_df.rovertimestamp), how = 'left').drop(latchF_df.mjid).drop(latchF_df.chid).drop(latchF_df.latchtypeid)
posltvtlafu_df = posltvtla_df.join(fuelF_df, (posltvtla_df.mjid == fuelF_df.mjid) & (posltvtla_df.chid == fuelF_df.chid) & (fuelF_df.lastfueltimestamp >= posltvtla_df.rovertimestamp + expr('INTERVAL -301 SECONDS')) & (fuelF_df.lastfueltimestamp <= posltvtla_df.rovertimestamp), how = 'left').drop(fuelF_df.mjid).drop(fuelF_df.chid)
posltvtlafuim_df = posltvtlafu_df.join(impactF_df,(posltvtlafu_df.mjid == impactF_df.mjid) & (posltvtlafu_df.chid == impactF_df.chid) & (impactF_df.impacttimestamp >= posltvtlafu_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (impactF_df.impacttimestamp <= posltvtlafu_df.rovertimestamp), how = 'left').drop(impactF_df.mjid).drop(impactF_df.chid)

# Write out.
posltvtlafuim_df.write.partitionBy("sourcesystemdate").parquet("s3://data-lake-us-west-2-062519970039/parquet/lut/pdsdataextraction",mode="append")

