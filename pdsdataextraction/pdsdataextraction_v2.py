import sys
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

if (len(sys.argv) < 3):
    print('Please provide database and output path as arguments')
    exit(1)

databaseName = sys.argv[1]
pdsdataOpPath = sys.argv[2]

spark = SparkSession.builder.appName('flatFileGeneration3').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Getting Last ingestion date4
maxSSDate = spark.sql("select coalesce(max(sourcesystemdate),cast('2017-07-31' as string)) as maxssdate from "+databaseName+".pdsdataextraction").collect()[0].asDict()['maxssdate']
lastIngestionDate=spark.sql("select coalesce(max(rovertimestamp), cast('2017-07-31' as timestamp)) AS maxrover from "+databaseName+".pdsdataextraction where sourcesystemdate='"+maxSSDate+"'").collect()[0].asDict()['maxrover']

# Reading the data
pos_df = spark.sql("SELECT rovertimestamp,sourcesystemtimestamp,sourcesystemdate,latchtypeid,vehicletypeid,mjid,chid,azimuth,localx,localy,lot,spreaderazimuth,spreaderx,spreadery,velocity,spreaderlength,spreaderheight FROM "+databaseName+".positionenh")
latch_df = spark.sql("SELECT rovertimestamp as latchtimestamp,mjid,chid,latchtypeid,containerlot,containerrow,containertier,container1spot,container1id,container2spot,container2id,matchedchid,distancetravelled,duration,flags as latchflags,sourcesystemdate FROM "+databaseName+".latch")
fuel_df = spark.sql("SELECT rovertimestamp as lastfueltimestamp,mjid,chid,level1 as fuellevel1,level2 as fuellevel2,sourcesystemdate FROM "+databaseName+".fuel")
impact_df = spark.sql("SELECT rovertimestamp as impacttimestamp,mjid,chid,sensorid1,ximpact1,yimpact1,zimpact1,sensorid2,ximpact2,yimpact2,zimpact2,sensorid3,ximpact3,yimpact3,zimpact3,sensorid4,ximpact4,yimpact4,zimpact4,flags as impactflags,sourcesystemdate FROM "+databaseName+".impact")
latchtype_df = spark.sql("SELECT latchtypeid,description AS spreaderstate FROM "+databaseName+".latchtype")
vehtype_df = spark.sql("SELECT vehicletypeid,description AS vehicletype FROM "+databaseName+".vehicletype")
tireflat_df = spark.sql("SELECT * from "+databaseName+".tireenh").drop("sourcesystemtimestamp","lot","sourcesystem")

# Only Bring in Deltas
posF_df=pos_df.where(pos_df['sourcesystemdate']>=maxSSDate).where(pos_df['rovertimestamp'] > lastIngestionDate)
latchF_df=latch_df.where(latch_df['sourcesystemdate']>=maxSSDate).where(latch_df['latchtimestamp'] > lastIngestionDate)
fuelF_df=fuel_df.where(fuel_df['sourcesystemdate']>=maxSSDate).where(fuel_df['lastfueltimestamp'] > lastIngestionDate)
impactF_df=impact_df.where(impact_df['sourcesystemdate']>=maxSSDate).where(impact_df['impacttimestamp'] > lastIngestionDate)
tireflatF_df=tireflat_df.where(tireflat_df['sourcesystemdate']>=maxSSDate).where(tireflat_df['rovertimestamp'] > lastIngestionDate)

# Join data
poslt_df = posF_df.join(broadcast(latchtype_df), posF_df.latchtypeid == latchtype_df.latchtypeid).drop(latchtype_df.latchtypeid).drop(posF_df.latchtypeid)
posltvt_df = poslt_df.join(broadcast(vehtype_df), poslt_df.vehicletypeid == vehtype_df.vehicletypeid).drop(vehtype_df.vehicletypeid).drop(poslt_df.vehicletypeid)
posltvtla_df = posltvt_df.join(latchF_df, (posltvt_df.mjid == latchF_df.mjid) & (posltvt_df.chid == latchF_df.chid) & (latchF_df.latchtimestamp >= posltvt_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (latchF_df.latchtimestamp <= posltvt_df.rovertimestamp), how = 'left').drop(latchF_df.mjid).drop(latchF_df.chid).drop(latchF_df.latchtypeid).drop(latchF_df.sourcesystemdate)
posltvtlafu_df = posltvtla_df.join(fuelF_df, (posltvtla_df.mjid == fuelF_df.mjid) & (posltvtla_df.chid == fuelF_df.chid) & (fuelF_df.lastfueltimestamp >= posltvtla_df.rovertimestamp + expr('INTERVAL -301 SECONDS')) & (fuelF_df.lastfueltimestamp <= posltvtla_df.rovertimestamp), how = 'left').drop(fuelF_df.mjid).drop(fuelF_df.chid).drop(fuelF_df.sourcesystemdate)
posltvtlafuim_df = posltvtlafu_df.join(impactF_df,(posltvtlafu_df.mjid == impactF_df.mjid) & (posltvtlafu_df.chid == impactF_df.chid) & (impactF_df.impacttimestamp >= posltvtlafu_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (impactF_df.impacttimestamp <= posltvtlafu_df.rovertimestamp), how = 'left').drop(impactF_df.mjid).drop(impactF_df.chid).drop(impactF_df.sourcesystemdate)
posltvtlafuimtf_df = posltvtlafuim_df.join(tireflatF_df,(posltvtlafuim_df.chid == tireflatF_df.chid) & (tireflatF_df.rovertimestamp >= posltvtlafuim_df.rovertimestamp + expr('INTERVAL -301 SECONDS')) & (tireflatF_df.rovertimestamp <= posltvtlafuim_df.rovertimestamp),how = 'left').drop(tireflatF_df.chid).drop(tireflatF_df.rovertimestamp).drop(tireflatF_df.servertimestamp).drop(tireflatF_df.sourcesystemdate)


posltvtlafuimtf_df.write.partitionBy("sourcesystemdate").parquet(pdsdataOpPath,mode='append')\

query = "MSCK REPAIR TABLE "+databaseName+".pdsdataextraction"
spark.sql(query)
