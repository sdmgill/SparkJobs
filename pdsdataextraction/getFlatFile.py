from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row;
from pyspark.sql.functions import col  
from pyspark.sql.types import *
from pyspark.sql import Row, functions as sparkFunction
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import expr
import boto3


spark = SparkSession.builder.appName('pdsdataextraction').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Getting Last ingestion date

lastIngestionDate=spark.sql("select coalesce(max(rovertimestamp), cast('2017-08-01' as timestamp)) as lastIngestionDate from parquet.ingestion_tracker where source_name= 'pdsdataextraction' ").first()["lastIngestionDate"]

#Reading the data 


#Reading the data 

pos_df=spark.read.parquet("s3://data-lake-us-west-2-062519970039/parquet/enhanced/positionenh").selectExpr("rovertimestamp","sourcesystemtimestamp","sourcesystemdate","latchtypeid","vehicletypeid","mjid","chid","azimuth","localx","localy","lot","spreaderazimuth","spreaderx","spreadery","velocity","spreaderlength","spreaderheight").where(col("rovertimestamp")>=(lastIngestionDate))


latch_df=spark.read.parquet("s3n://data-lake-us-west-2-062519970039/parquet/mistar/latch").selectExpr("rovertimestamp as latchtimestamp","mjid","chid","latchtypeid","containerlot","containerrow","containertier","container1spot","container1id","container2spot","container2id","matchedchid","distancetravelled","duration","flags as latchflags","sourcesystemdate").where(col("rovertimestamp")>=(lastIngestionDate)).drop("sourcesystemdate")

fuel_df=spark.read.parquet("s3n://data-lake-us-west-2-062519970039/parquet/mistar/fuel").selectExpr("RoverTimestamp as lastfueltimestamp","level1 as fuellevel1","level2 as fuellevel2","mjid","chid").where(col("rovertimestamp")>=lastIngestionDate)

impact_df=spark.read.parquet("s3n://data-lake-us-west-2-062519970039/parquet/mistar/impact").selectExpr("rovertimestamp as impacttimestamp","mjid","chid","sensorid1","ximpact1","yimpact1","zimpact1","sensorid2","ximpact2","yimpact2","zimpact2","sensorid3","ximpact3","yimpact3","zimpact3","sensorid4","ximpact4","yimpact4","zimpact4","flags as impactflags").where(col("rovertimestamp")>=lastIngestionDate)


latchtype_df=spark.read.parquet("s3n://data-lake-us-west-2-062519970039/parquet/mistar/latchtype").selectExpr("latchtypeid","description AS spreaderstate")

vehtype_df=spark.read.parquet("s3n://data-lake-us-west-2-062519970039/parquet/mistar/vehicletype").selectExpr("vehicletypeid","description AS vehicletype")



#transformation

poslt_df = pos_df.join(latchtype_df, pos_df.latchtypeid == latchtype_df.latchtypeid).drop(latchtype_df.latchtypeid).drop(pos_df.latchtypeid)

posltvt_df = poslt_df.join(vehtype_df, poslt_df.vehicletypeid == vehtype_df.vehicletypeid).drop(vehtype_df.vehicletypeid).drop(poslt_df.vehicletypeid)

posltvtla_df = posltvt_df.join(latch_df, (posltvt_df.mjid == latch_df.mjid) & (posltvt_df.chid == latch_df.chid) & (latch_df.latchtimestamp >= posltvt_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (latch_df.latchtimestamp <= posltvt_df.rovertimestamp), how = 'left').drop(latch_df.mjid).drop(latch_df.chid).drop(latch_df.latchtypeid)

posltvtlafu_df = posltvtla_df.join(fuel_df, (posltvtla_df.mjid == fuel_df.mjid) & (posltvtla_df.chid == fuel_df.chid) & (fuel_df.lastfueltimestamp >= posltvtla_df.rovertimestamp + expr('INTERVAL -301 SECONDS')) & (fuel_df.lastfueltimestamp <= posltvtla_df.rovertimestamp), how = 'left').drop(fuel_df.mjid).drop(fuel_df.chid)

posltvtlafuim_df = posltvtlafu_df.join(impact_df,(posltvtlafu_df.mjid == impact_df.mjid) & (posltvtlafu_df.chid == impact_df.chid) & (impact_df.impacttimestamp >= posltvtlafu_df.rovertimestamp + expr('INTERVAL -1 SECONDS')) & (impact_df.impacttimestamp <= posltvtlafu_df.rovertimestamp), how = 'left').drop(impact_df.mjid).drop(impact_df.chid)


posltvtlafuim_df.write.partitionBy("sourcesystemdate").parquet("s3://data-lake-us-west-2-062519970039/parquet/lut/pdsdataextraction",mode="append")

#Writing in ingestion table

posltvtlafuim_df.selectExpr("'pdsdataextraction' as source_name","count(*) as ingested_row_count", "max(rovertimestamp) as rovertimestamp","current_date() as ingestiontimestamp").write.parquet("s3://data-lake-us-west-2-062519970039/parquet/ingestionTracker",mode="append")

# Updating Athena Partitions
s3_output = 's3://pa-athena-custom-results/lambda/'
database = 'parquet'
query = "MSCK REPAIR TABLE parquet.pdsdataextraction"

def run_query(query, database, s3_output):
    client = boto3.client('athena', region_name='us-west-2')
    
    response = client.start_query_execution(QueryString=query, 
    QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output,})
        
    return response['QueryExecutionId']
	
def get_query(exid):
    client = boto3.client('athena', region_name='us-west-2')
    
    response = client.get_query_results(
        QueryExecutionId=exid)
    
    return response
	
query_id = run_query(query, database, s3_output)

ready = 0
while ready < 1:
    try:
        result = get_query(query_id)
        ready = 1
    except:
        continue

print("successful")




