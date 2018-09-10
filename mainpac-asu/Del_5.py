import warnings
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName('Script5').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Read Files
#work_orders = spark.read.csv('s3://pa-sparktraining/contest/work_orders.csv',inferSchema=True, header=True)
#operational_asset = spark.read.csv('s3://pa-sparktraining/contest/operational_assets.csv',inferSchema=True, header=True)

opass = spark.sql("SELECT operationalassetid, operationalassetname name, installdate year, sortkeyid FROM parquet.operationalasset")
opass2 = spark.sql("SELECT operationalassetid, operationalassetname name, operationalviewid FROM parquet.operationalasset")
worder = spark.sql("SELECT operationalassetid, workorderid FROM parquet.workorder")
lupt = spark.sql("SELECT tablevalue Value, tableid, tablename sortkeyname, tabledescription causeoffaultdescription FROM parquet.lookuptable")
opview = spark.sql("SELECT name operationalviewname, operationalviewid FROM parquet.operationalview")
wtype = spark.sql("SELECT category worktypecategory, worktypeid FROM parquet.worktype")

OA = opass.join(lupt, opass.sortkeyid == lupt.tableid).drop(lupt.Value).drop(lupt.tableid).drop(lupt.causeoffaultdescription).drop(opass.sortkeyid)

woA = worder.join(lupt, worder.workorderid == lupt.tableid).drop(lupt.Value).drop(lupt.sortkeyname).drop(lupt.tableid)
woB = woA.join(wtype, woA.workorderid == wtype.worktypeid).drop(wtype.worktypeid).drop(woA.workorderid)
woC = woB.join(opass2, woB.operationalassetid == opass2.operationalassetid).drop(opass2.operationalassetid).drop(opass2.name)
WO = woC.join(opview,woC.operationalviewid == opview.operationalviewid).drop(opview.operationalviewid).drop(woC.operationalviewid)


#subset of only columns needed
WO_1 = WO.join(OA[['operationalassetid']],WO.operationalassetid==OA.operationalassetid).select(WO.operationalassetid,WO.causeoffaultdescription,WO.operationalviewname,WO.worktypecategory)
WO_subset=WO_1
OA_subset = OA.select('operationalassetid','name','year','sortkeyname')

#type = failure
set1 = WO_subset[WO_subset['worktypecategory']=='Failure']

#exclude null values
set2 = set1.na.drop(subset=['causeoffaultdescription'])

set1.createOrReplaceTempView('temp')

set2=spark.sql("Select * from temp where causeoffaultdescription!='NULL'")

set3 = OA_subset

#exclude certain values
set3 = set3.filter((set3.sortkeyname != 'AUXILIARY') & (set3.sortkeyname != 'INFRASTRUCTURE'))
set3 = set3.filter(~(set3.name.like ("%GROUP%")) & ~(set3.name.like ("%AA%"))& ~(set3.name.like ("%TM%")) & ~(set3.name.like ("%PRE-TRIP%")) & ~(set3.name.like ("%WITHOUT%")) & ~(set3.name.like ("%RETIRED%"))& ~(set3.name.like ("%MISC%")))

set2 = set2.filter(~(set2.operationalviewname.like ("%GROUP%")) & ~(set2.operationalviewname.like ("%AA%"))& ~(set2.operationalviewname.like ("%TM%")) & ~(set2.operationalviewname.like ("%PRE-TRIP%")) & ~(set2.operationalviewname.like ("%WITHOUT%")) & ~(set2.operationalviewname.like ("%RETIRED%"))& ~(set2.operationalviewname.like ("%MISC%")))

set4 = set3.select('operationalassetid','year','sortkeyname', 'name')

#merge tables
joined = set2.join(set4, set2.operationalassetid==set4.operationalassetid, how='right').select(set4.operationalassetid, set4.name, set2.causeoffaultdescription, set2.operationalviewname, set2.worktypecategory, set4.year, set4.sortkeyname)


# Download
#joined.coalesce(1).write.save(path='csv', format='csv', mode='append', sep='\t')

#Output to S3

joined.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Del_5_Output",mode="overwrite")
