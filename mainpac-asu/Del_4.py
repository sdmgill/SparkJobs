import warnings
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName('Script4').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Read Files
#meter_readings = spark.read.csv('s3://pa-sparktraining/contest/meter_readings.csv',inferSchema=True, header=True)
#OA = spark.read.csv('s3://pa-sparktraining/contest/operational_assets.csv',inferSchema=True, header=True)
#WO = spark.read.csv('s3://pa-sparktraining/contest/work_orders.csv',inferSchema=True, header=True)


opass = spark.sql("SELECT operationalassetid, operationalassetname Name, installdate Year, operationalassetname, sortkeyid, operationalviewid FROM parquet.operationalasset")
opass2 = spark.sql("SELECT operationalassetid, operationalassetname Name, operationalviewid FROM parquet.operationalasset")
worder = spark.sql("SELECT operationalassetid, workorderid FROM parquet.workorder")
meter = spark.sql("SELECT operationalassetid, meterdescription, createtime Year, metername  FROM parquet.meter")
lupt = spark.sql("SELECT tablevalue Value, tableid, tablename SortKeyName, tabledescription CauseOfFaultDescription FROM parquet.lookuptable")
opview = spark.sql("SELECT name operationalviewname, operationalviewid FROM parquet.operationalview")
wtype = spark.sql("SELECT category worktypecategory, worktypeid FROM parquet.worktype")

OA = opass.join(lupt, opass.sortkeyid == lupt.tableid).drop(lupt.Value).drop(opass.operationalviewid).drop(opass.sortkeyid).drop(lupt.tableid).drop(lupt.CauseOfFaultDescription).drop(opass.operationalassetname)

woA = worder.join(lupt, worder.workorderid == lupt.tableid).drop(lupt.Value).drop(lupt.SortKeyName).drop(lupt.tableid).drop(lupt.CauseOfFaultDescription)
woB = woA.join(wtype, woA.workorderid == wtype.worktypeid).drop(wtype.worktypeid).drop(woA.workorderid)
woC = woB.join(opass2, woB.operationalassetid == opass2.operationalassetid).drop(opass2.operationalassetid)
work_orders = woC.join(opview,woC.operationalviewid == opview.operationalviewid).drop(opview.operationalviewid).drop(woC.operationalviewid)

mrA = meter.join(opass, meter.operationalassetid == opass.operationalassetid).drop(opass.operationalassetid).drop(opass.Year).drop(opass.operationalassetname).drop(meter.metername)
mrB = mrA.join(lupt, mrA.sortkeyid == lupt.tableid).drop(lupt.tableid).drop(lupt.SortKeyName).drop(opass.sortkeyid).drop(lupt.CauseOfFaultDescription)
meter_readings = mrB.join(opview,mrB.operationalviewid == opview.operationalviewid).drop(mrB.operationalviewid).drop(opview.operationalviewid).drop(opview.operationalviewname)

#exclude certain values
OA = OA.filter((OA.SortKeyName != 'AUXILIARY') & (OA.SortKeyName != 'INFRASTRUCTURE'))
OA = OA.filter(~(OA.Name.like ("%GROUP%")) & ~(OA.Name.like ("%AA%"))& ~(OA.Name.like ("%TM%")) & ~(OA.Name.like ("%PRE-TRIP%")) & ~(OA.Name.like ("%WITHOUT%")) & ~(OA.Name.like ("%RETIRED%"))& ~(OA.Name.like ("%MISC%")))


work_orders = work_orders.filter(~(work_orders.Name.like ("%GROUP%")) & ~(work_orders.Name.like ("%AA%"))& ~(work_orders.Name.like ("%TM%")) & ~(work_orders.Name.like ("%PRE-TRIP%")) & ~(work_orders.Name.like ("%WITHOUT%")) & ~(work_orders.Name.like ("%RETIRED%"))& ~(work_orders.Name.like ("%MISC%")))

#set work type to failure
work_orders_failures = work_orders[work_orders['WorkTypeCategory'] == 'Failure']

#number of failures
work_orders_failures.createOrReplaceTempView('temp')

temp1=spark.sql("Select operationalassetid,count(*) over (partition by operationalassetid order by operationalassetid) as count,last_value(Name) over (partition by operationalassetid order by operationalassetid) as Name,last_value(operationalviewname) over (partition by operationalassetid order by operationalassetid) as operationalviewname from temp")

temp1.createOrReplaceTempView('temp1')

failures=spark.sql("Select operationalassetid,count,Name,operationalviewname from temp1 group by operationalassetid,count,Name,operationalviewname")



#Operational Asset Subset
operational_asset_subset = OA.select('operationalassetid','Year')
#sum to get usage per operational asset
meter_readings.createOrReplaceTempView('meter_readings')

temp2=spark.sql("Select operationalassetid,sum(value) over (partition by operationalassetid order by operationalassetid) as sum,last_value(MeterDescription) over (partition by operationalassetid order by operationalassetid) as MeterDescription from meter_readings")

temp2.createOrReplaceTempView('temp2')

meter_reading_by_OAID=spark.sql("Select operationalassetid,sum,MeterDescription from temp2 group by operationalassetid,sum,MeterDescription")
# Join tables
joined = operational_asset_subset.join(meter_reading_by_OAID.join(failures,how = 'inner',on = 'operationalassetid'), how='inner', on ='operationalassetid')
# Download
#joined.coalesce(1).write.save(path='csv2', format='csv', mode='append', sep='\t')

#Output to S3
joined.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Del_4_Output",mode="overwrite")
