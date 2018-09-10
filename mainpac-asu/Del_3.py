import pandas as pd
import warnings
from pyspark.sql.types import *
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName('Script3').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

#Read Files
opass = spark.sql("SELECT operationalassetid, operationalassetname Name, installdate Year, operationalassetname, sortkeyid FROM parquet.operationalasset")
opass2 = spark.sql("SELECT operationalassetid, operationalassetname, installdate Year, sortkeyid FROM parquet.operationalasset")
worder = spark.sql("SELECT operationalassetid, workorderid FROM parquet.workorder")
lupt = spark.sql("SELECT tablevalue Value, tableid, tablename SortKeyName, tabledescription CauseOfFaultDescription FROM parquet.lookuptable")
opview = spark.sql("SELECT name operationalviewname, operationalviewid FROM parquet.operationalview")
wtype = spark.sql("SELECT name worktypename, worktypeid FROM parquet.worktype")
model_df = spark.sql("SELECT ta.keyid operationalassetid,MIN(ta.datavalue) Model FROM parquet.tableattribute ta LEFT OUTER JOIN parquet.lookuptable lt ON lt.tableid = ta.attributenameid WHERE lt.tablename = 'MODEL NO' GROUP BY keyid")
make_df = spark.sql("SELECT ta.keyid operationalassetid,MIN(ta.datavalue) Make FROM parquet.tableattribute ta LEFT OUTER JOIN parquet.lookuptable lt ON lt.tableid = ta.attributenameid WHERE lt.tablename = 'MAKE' GROUP BY keyid")

oaA = opass.join(lupt, opass.sortkeyid == lupt.tableid).drop(lupt.Value).drop(lupt.tableid).drop(lupt.CauseOfFaultDescription).drop(opass.sortkeyid).drop(opass.operationalassetname)
oaB = oaA.join(model_df, oaA.operationalassetid == model_df.operationalassetid).drop(model_df.operationalassetid)
operational_asset = oaB.join(make_df, oaB.operationalassetid == make_df.operationalassetid).drop(make_df.operationalassetid)

woA = worder.join(wtype, worder.workorderid == wtype.worktypeid).drop(worder.workorderid)
woB = woA.join(opass2, woA.operationalassetid == opass2.operationalassetid).drop(opass2.operationalassetid)
woC = woB.join(lupt, woB.sortkeyid == lupt.tableid).drop(lupt.Value).drop(lupt.tableid).drop(woB.sortkeyid).drop(lupt.SortKeyName)
woD = woC.join(model_df, woC.operationalassetid == model_df.operationalassetid).drop(model_df.operationalassetid)
work_orders = woD.join(make_df, woD.operationalassetid == make_df.operationalassetid).drop(make_df.operationalassetid)



#get rid of null values
work_orders.createOrReplaceTempView('work_orders')
operational_asset.createOrReplaceTempView('operational_asset')

workOrderTable1=spark.sql("Select * from work_orders where Model!='NULL' or Make!='NULL'")
operAssTable1=spark.sql("Select * from operational_asset where Make!='NULL'")

#Convert to Pandas
workOrderTable2 = workOrderTable1.toPandas()
operAssTable = operAssTable1.toPandas()

#Additional filters in Operational Asset - May be required - Confirm with MARTHA
operAssTable=operAssTable[(operAssTable["SortKeyName"]!="INFRASTRUCTURE") & (operAssTable["SortKeyName"]!="AUXILIARY")]

operAssTable['Name']=operAssTable['Name'].astype(str)
operAssTable=operAssTable[~operAssTable["Name"].str.contains("GROUP") & ~operAssTable["Name"].str.contains("AA") & ~operAssTable["Name"].str.contains("TM") & ~operAssTable["Name"].str.contains("PRE-TRIP") & ~operAssTable["Name"].str.contains("WITHOUT") & ~operAssTable["Name"].str.contains("RETIRED") & ~operAssTable["Name"].str.contains("MISC")]

workOrderTable2=workOrderTable2[~workOrderTable2["operationalassetname"].str.contains("GROUP") & ~workOrderTable2["operationalassetname"].str.contains("AA") & ~workOrderTable2["operationalassetname"].str.contains("TM") & ~workOrderTable2["operationalassetname"].str.contains("PRE-TRIP") & ~workOrderTable2["operationalassetname"].str.contains("WITHOUT") & ~workOrderTable2["operationalassetname"].str.contains("RETIRED") & ~workOrderTable2["operationalassetname"].str.contains("MISC")]


#Inner Join between Operational Assets and Workorders
workOrderTable = pd.merge(workOrderTable2, operAssTable[['operationalassetid']],  how='inner', left_on=['operationalassetid'], right_on = ['operationalassetid'])

workOrderTable['MakenMod']=workOrderTable['Make']+"-"+workOrderTable['Model']
operAssTable['MakenMod']=operAssTable['Make']+"-"+operAssTable['Model']

#Taking only the columns that are required
workOrderTable = workOrderTable[['operationalassetid','operationalassetname','Make','Model','Year','CauseOfFaultDescription','MakenMod','worktypename']]
operAssTable = operAssTable[['operationalassetid','Name','Make','Model','Year', 'MakenMod','SortKeyName']]

#List of Incorrect Makes to correct
replaceList1=['terex/noel','terex/noell','terex noel','terex noell','noell','terex/fantuzzi reggiane','terex/fantuzzi']
replaceList2=['fantuzzi reggiani', 'fantuzzi','fantuzzi reggiane']
replaceList3=['chev', 'chevrolet', 'chevy']
replaceList4=['frueha','fruehauf','freuhauf']

workOrderTable['Make']=workOrderTable['Make'].str.strip()
workOrderTable['Model']=workOrderTable['Model'].str.strip()
workOrderTable['MakenMod']=workOrderTable['MakenMod'].str.strip()

operAssTable['Make']=operAssTable['Make'].str.strip()
operAssTable['Model']=operAssTable['Model'].str.strip()
operAssTable['MakenMod']=operAssTable['MakenMod'].str.strip()

#To replace and correct values of Makes
for operAssData in [workOrderTable,operAssTable]:
    operAssData['Make']=operAssData['Make'].str.lower()
    operAssData['Model']=operAssData['Model'].str.lower()
    operAssData['MakenMod']=operAssData['MakenMod'].str.lower()

    for element in replaceList1:
        operAssData.replace(to_replace=element, value='terex', inplace=True)
    for element in replaceList2:
        operAssData.replace(to_replace=element, value='fantuzzi', inplace=True)
    for element in replaceList3:
        operAssData.replace(to_replace=element, value='chevrolet', inplace=True)
    for element in replaceList4:
        operAssData.replace(to_replace=element, value='fruehauf', inplace=True)

    operAssData.replace(to_replace='liebherr-america', value='liebherr', inplace=True)
    operAssData.replace(to_replace='maffi', value='mafi', inplace=True)
    operAssData.replace(to_replace='sterling/lorain', value='sterling', inplace=True)
    operAssData.replace(to_replace='taylor dunn', value='taylor', inplace=True)
    operAssData.replace(to_replace='theuer', value='theurer', inplace=True)
    operAssData.replace(to_replace='utility enterprise', value='utility', inplace=True)
    operAssData.replace(to_replace='tug mfg', value='tug', inplace=True)
    operAssData.replace(to_replace='track mobile', value='trackmobile', inplace=True)

    operAssData.replace(to_replace='treberg', value='terberg', inplace=True)
    operAssData.replace(to_replace='fontai', value='fontaine', inplace=True)

operMake=operAssTable.groupby('Make')["operationalassetid"].count().reset_index(name="CountOfAssetMakes")

#Take only Fail, Failold, SCH, SCHold, Tires work types
workOrderMMFails=workOrderTable[(workOrderTable.worktypename == 'FAIL') | (workOrderTable.worktypename == 'FAILold') | (workOrderTable.worktypename == 'SCH') | (workOrderTable.worktypename == 'SCHold') | (workOrderTable.worktypename == 'TIRES')]
#workOrderMMFails.to_csv('workOrderMMFails.csv',index=False)

wOrderOper1 = pd.merge(workOrderMMFails, operMake,  how='left', left_on=['Make'], right_on = ['Make'])

#Counts of Work Orders Based on Make
workOrderMake=workOrderMMFails.groupby('Make')["operationalassetid"].count().reset_index(name="CountOfMakesInWorkOrders")

wOrderOper2 = pd.merge(wOrderOper1, workOrderMake,  how='left', left_on=['Make'], right_on = ['Make'])


#Counts of Work Orders Based on Model
workOrderModel=workOrderMMFails.groupby('MakenMod')["operationalassetid"].count().reset_index(name="CountOfModels")
#workOrderModel2=workOrderMMFails.groupby('MakenMod').count().reset_index(name="CountOfModels")

wOrderOper3 = pd.merge(wOrderOper2, workOrderModel,  how='left', left_on=['MakenMod'], right_on = ['MakenMod'])

#Counts of Operational Assets Based on Model
operModel=operAssTable.groupby('MakenMod')["operationalassetid"].count().reset_index(name="CountOfAssetModels")

wOrderOper4 = pd.merge(wOrderOper3, operModel,  how='left', left_on=['MakenMod'], right_on = ['MakenMod'])

wOrderOper5 = pd.merge(wOrderOper4, operAssTable[['operationalassetid','SortKeyName']],  how='left', left_on=['operationalassetid'], right_on = ['operationalassetid'])

#Output to S3
final = spark.createDataFrame(wOrderOper5)


final.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Del_3_Output",mode="overwrite")
