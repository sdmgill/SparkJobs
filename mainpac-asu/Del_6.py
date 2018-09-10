import warnings
from pyspark.sql.functions import unix_timestamp,col,udf, min,max, round,lit
from pyspark.sql.types import DateType,TimestampType,IntegerType,StringType
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName('Script6').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Read Files
#meter_readings = spark.read.csv('s3://pa-sparktraining/contest/meter_readings.csv',inferSchema=True, header=True)
#operational_asset = spark.read.csv('s3://pa-sparktraining/contest/operational_assets.csv',inferSchema=True, header=True)


opview = spark.sql("SELECT name operationalviewname, operationalviewid FROM parquet.operationalview")
lupt = spark.sql("SELECT tablevalue Value, tablename SortKeyName, tableid FROM parquet.lookuptable")
meter = spark.sql("SELECT operationalassetid, updatetime Date, metername FROM parquet.meter")
opass = spark.sql("SELECT operationalassetid, operationalassetname Name, operationalassetdescription, sortkeyid, operationalviewid FROM parquet.operationalasset")

operational_asset = opass.join(lupt, opass.sortkeyid == lupt.tableid).drop(lupt.Value).drop(opass.operationalviewid).drop(opass.sortkeyid).drop(lupt.tableid)

mrA = meter.join(opass, meter.operationalassetid == opass.operationalassetid).drop(opass.operationalassetid)
mrB = mrA.join(lupt, mrA.sortkeyid == lupt.tableid).drop(lupt.tableid).drop(lupt.SortKeyName).drop(opass.sortkeyid)
meter_readings = mrB.join(opview,mrB.operationalviewid == opview.operationalviewid).drop(mrB.operationalviewid).drop(opview.operationalviewid)


OA = operational_asset
OA = OA.filter((OA.SortKeyName != 'AUXILIARY') & (OA.SortKeyName != 'INFRASTRUCTURE'))
OA = OA.filter(~(OA.Name.like ("%GROUP%")) & ~(OA.Name.like ("%AA%"))& ~(OA.Name.like ("%TM%")) & ~(OA.Name.like ("%PRE-TRIP%")) & ~(OA.Name.like ("%WITHOUT%")) & ~(OA.Name.like ("%RETIRED%"))& ~(OA.Name.like ("%MISC%")))

MR = meter_readings.select(['operationalassetid','Value','Date','Name','operationalassetdescription','metername','operationalviewname'])


from datetime import datetime, timedelta
func = udf(lambda x: datetime.strptime(x,'%m/%d/%y'),DateType())

#conversions to date
#MR=MR.withColumn('Date',func(col('Date')))

#Group by
MR.createOrReplaceTempView('MR')

MR_Agg=spark.sql("Select operationalassetid,Name,operationalassetdescription,metername,operationalviewname,sum(Value) as SumOfValue,max(Date) as MaxDate,min(Date) as MinDate from MR group by operationalassetid,Name,operationalassetdescription,metername,operationalviewname")

#get date difference
MR_Agg = MR_Agg.withColumn('Date_Diff',unix_timestamp('MaxDate','yyyy-mm-dd') - unix_timestamp('MinDate','yyyy-mm-dd'))
#converting to days
MR_Agg = MR_Agg.withColumn('Date_Diff',MR_Agg['Date_Diff'].cast(IntegerType())/(24*60*60))

MR_Agg = MR_Agg.withColumn('Ratio',MR_Agg['SumOfValue']/MR_Agg['Date_Diff'])

joined = MR_Agg.join(OA[['operationalassetid','SortKeyName']],on ='operationalassetid',how='left').select(MR_Agg.operationalassetid, MR_Agg.SumOfValue, MR_Agg.MaxDate, MR_Agg.MinDate, MR_Agg.operationalviewname, MR_Agg.operationalassetdescription, MR_Agg.Date_Diff,
MR_Agg.Ratio, MR_Agg.metername, OA.Name, OA.SortKeyName)


#joined.coalesce(1).write.save(path='csv3', format='csv', mode='append', sep='\t')


#Output to S3

joined.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Del_6_Output",mode="overwrite")
