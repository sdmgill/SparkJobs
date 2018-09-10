from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('flatFileGeneration3').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

pos_df = spark.sql("SELECT rovertimestamp,sourcesystemtimestamp,sourcesystemdate,latchtypeid,vehicletypeid,mjid,chid,azimuth,localx,localy,spreaderazimuth,spreaderx,spreadery,velocity,spreaderlength,spreaderheight FROM parquet.position WHERE sourcesystemdate >= cast('2018-08-01' as date)")
latch_df = spark.sql("SELECT rovertimestamp as latchtimestamp,mjid,chid,latchtypeid,containerlot,containerrow,containertier,container1spot,container1id,container2spot,container2id,matchedchid,distancetravelled,duration,flags as latchflags FROM parquet.latch WHERE sourcesystemdate >= cast('2018-08-01' as date)")
fuel_df = spark.sql("SELECT rovertimestamp as lastfueltimestamp,mjid,chid,level1 as fuellevel1,level2 as fuellevel2 FROM parquet.fuel WHERE sourcesystemdate >= cast('2018-08-01' as date)")
impact_df = spark.sql("SELECT rovertimestamp as impacttimestamp,mjid,chid,sensorid1,ximpact1,yimpact1,zimpact1,sensorid2,ximpact2,yimpact2,zimpact2,sensorid3,ximpact3,yimpact3,zimpact3,sensorid4,ximpact4,yimpact4,zimpact4,flags as impactflags FROM parquet.impact WHERE sourcesystemdate >= cast('2018-08-01' as date)")
latchtype_df = spark.sql("SELECT latchtypeid,description AS spreaderstate FROM parquet.latchtype")
vehtype_df = spark.sql("SELECT vehicletypeid,description AS vehicletype FROM parquet.vehicletype")

poslt_df = pos_df.join(latchtype_df, pos_df.latchtypeid == latchtype_df.latchtypeid).drop(latchtype_df.latchtypeid).drop(pos_df.latchtypeid)
posltvt_df = poslt_df.join(vehtype_df, poslt_df.vehicletypeid == vehtype_df.vehicletypeid).drop(vehtype_df.vehicletypeid).drop(poslt_df.vehicletypeid)
posltvtla_df = posltvt_df.join(latch_df, (posltvt_df.mjid == latch_df.mjid) & (posltvt_df.chid == latch_df.chid) & (latch_df.latchtimestamp >= posltvt_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (latch_df.latchtimestamp <= posltvt_df.rovertimestamp), how = 'left').drop(latch_df.mjid).drop(latch_df.chid).drop(latch_df.latchtypeid)
posltvtlafu_df = posltvtla_df.join(fuel_df, (posltvtla_df.mjid == fuel_df.mjid) & (posltvtla_df.chid == fuel_df.chid) & (fuel_df.lastfueltimestamp >= posltvtla_df.rovertimestamp + expr('INTERVAL -301 SECONDS')) & (fuel_df.lastfueltimestamp <= posltvtla_df.rovertimestamp), how = 'left').drop(fuel_df.mjid).drop(fuel_df.chid)
posltvtlafuim_df = posltvtlafu_df.join(impact_df,(posltvtlafu_df.mjid == impact_df.mjid) & (posltvtlafu_df.chid == impact_df.chid) & (impact_df.impacttimestamp >= posltvtlafu_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (impact_df.impacttimestamp <= posltvtlafu_df.rovertimestamp), how = 'left').drop(impact_df.mjid).drop(impact_df.chid)


posltvtlafuim_df.write.partitionBy("sourcesystemdate").parquet("s3://data-lake-us-west-2-062519970039/parquet/lut/pdsdataextraction",mode="overwrite")