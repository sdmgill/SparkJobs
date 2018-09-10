import pandas as pd
import warnings
from pyspark.sql.types import *
from pyspark.sql.functions import format_number,dayofmonth,hour,dayofyear,month,year,weekofyear,date_format, to_date
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName('Script7').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

pd.set_option('display.max_columns', None)

#Read Files

OA_DF = spark.sql("SELECT operationalassetid, operationalassetname,year(installdate) year FROM parquet.operationalasset")

worder = spark.sql("SELECT workorderid, raisedate, finishdate, operationalassetid, worktypeid FROM parquet.workorder")
worktype = spark.sql("SELECT worktypeid, category worktypecategory FROM parquet.worktype")
oawo_df = worder.join(OA_DF,OA_DF.operationalassetid == worder.operationalassetid).drop(OA_DF.operationalassetid)

work_orders_DF = oawo_df.join(worktype,oawo_df.worktypeid == worktype.worktypeid).drop(worktype.worktypeid).drop(oawo_df.worktypeid).drop(oawo_df.workorderid)

#Convert dates to datetime and drop null rows
#work_orders_DF = work_orders_DF.filter(work_orders_DF["raisedate"] != 'null')
#work_orders_DF = work_orders_DF(col('raisedate').isNotNull())
work_orders_DF = work_orders_DF.na.drop(subset=["raisedate"])

#func = udf(lambda x: datetime.strptime(x,'%m/%d/%y'),DateType())
#work_orders_DF = work_orders_DF.withColumn('finishdate',func(col('finishdate'))).withColumn('raisedate',func(col('raisedate')))

#Use only work orders from 2016 and beyond
work_orders_DF = work_orders_DF.withColumn("yearONLY",year(work_orders_DF['raisedate']))
work_orders_DF = work_orders_DF.withColumn("yearONLY", work_orders_DF["yearONLY"].cast(IntegerType()))
work_orders_DF = work_orders_DF.filter(work_orders_DF["yearONLY"] >= 2016)
work_orders_DF = work_orders_DF.drop("yearONLY")

#Create df with only failures
worder_fail_DF = work_orders_DF.filter(work_orders_DF["worktypecategory"] == "Failure").select(
    'operationalassetid', 'operationalassetname' ,'worktypecategory', 'raisedate')

#create dataframe with max dates for each OAID
worder_fail_max_DF = worder_fail_DF.groupBy("operationalassetid", "operationalassetname").agg({"raisedate" : 'max'})

#For all assets, if the last failed date is before the date that the data was accessed, add another row as "Non Failure"
#convert DFs to pandas
pd_worder_DF = work_orders_DF.toPandas()
pd_worder_fail_DF = worder_fail_DF.toPandas()
pd_worder_fail_max_DF = worder_fail_max_DF.toPandas()
OA = OA_DF.toPandas()

#find max and min raise dates
max_date = pd_worder_fail_DF['raisedate'].max()
min_date = pd_worder_fail_DF['raisedate'].min()

#For all assets for which the last failed date is before the date that the data was accessed, add another row as "Non Failure"
for index, row in pd_worder_fail_max_DF.iterrows():
    if row['max(raisedate)'] <= max_date:
        pd_worder_fail_DF = pd_worder_fail_DF.append({'operationalassetid':row['operationalassetid'],
                                            'operationalassetname':row['operationalassetname'],
                                            'raisedate':max_date,'worktypecategory': 'Non Failure'},
                                           ignore_index = True)


#Sort Values
pd_worder_fail_DF = pd_worder_fail_DF.sort_values(['operationalassetid','raisedate'])

#Create df for maintenance
MT = pd_worder_DF[pd_worder_DF['worktypecategory'] == 'Maintenance'].loc[:,
                                                ('operationalassetid','operationalassetname','finishdate')]



#Create columns
pd_worder_fail_DF['sincefailure'] = 0
#Create initial date shift
pd_worder_fail_DF['raise_date_shifted'] = pd_worder_fail_DF['raisedate'].shift(1)


#Look for assets that are different
pd_worder_fail_DF['differentasset'] = pd_worder_fail_DF['operationalassetid'].diff()
pd_worder_fail_DF[pd_worder_fail_DF['differentasset'] != 0.0]['differentasset'] = pd_worder_fail_DF['raisedate']

#For assets that have not changed, "unshift" the dates

pd_worder_fail_DF['previousdate'] = 0


for i, row in pd_worder_fail_DF.iterrows():
    if row['differentasset'] != 0.0:
        pd_worder_fail_DF.loc[i,'previousdate'] = min_date
    else:
        pd_worder_fail_DF.loc[i,'previousdate'] = row['raise_date_shifted']

#drop helper columns
pd_worder_fail_DF = pd_worder_fail_DF.drop(columns = ['differentasset','raise_date_shifted'])


#Convert to datetime
pd_worder_fail_DF['previousdate'] = pd.to_datetime(pd_worder_fail_DF['previousdate'])
pd_worder_fail_DF['raisedate'] = pd.to_datetime(pd_worder_fail_DF['raisedate'])
MT['finishdate'] = pd.to_datetime(MT['finishdate'])

#Calculate date since failure --- CONVERT raisedate TO DATETIME
pd_worder_fail_DF['sincefailure'] = pd_worder_fail_DF['raisedate'] - pd_worder_fail_DF['previousdate']

#create column 'Cumulative Maintenance'
pd_worder_fail_DF['cumulative_maintenance'] = 0

#Calculate Cumulative Maintenance
for ID in pd_worder_fail_DF['operationalassetid'].unique():
    for i, r in pd_worder_fail_DF[pd_worder_fail_DF['operationalassetid'] == ID].iterrows():
        temp = 0
        for j, s in MT[MT['operationalassetid'] == ID].iterrows():
            if s['finishdate'] <= r['raisedate']:
                temp = temp + 1
        pd_worder_fail_DF.loc[i,'cumulative_maintenance'] = temp

# Keep only necessary columns
pd_worder_fail_DF = pd_worder_fail_DF.loc[:,('operationalassetid','worktypecategory','sincefailure','operationalassetname','raisedate', 'cumulative_maintenance')]

date_delta = max_date - min_date

MA = pd_worder_DF[pd_worder_DF['worktypecategory'] == 'Maintenance']
FA = pd_worder_DF[pd_worder_DF['worktypecategory'] == 'Failure']

#OA maintained but not failed
MA_not_FA = MA.merge(FA, on = 'operationalassetid', how = 'left', indicator = True)
MA_not_FA = MA_not_FA[MA_not_FA['_merge'] == 'left_only']['operationalassetid'].unique()

#Calculate Cumulative Maintenance
for ID in pd_worder_fail_DF['operationalassetid'].unique():
    for i, r in pd_worder_fail_DF[pd_worder_fail_DF['operationalassetid'] == ID].iterrows():
        temp = 0
        for j, s in MT[MT['operationalassetid'] == ID].iterrows():
            if s['finishdate'] <= r['raisedate']:
                temp = temp + 1
        pd_worder_fail_DF.loc[i,'cumulative_maintenance'] = temp


# Keep only necessary columns
pd_worder_fail_DF = pd_worder_fail_DF.loc[:,('operationalassetid','worktypecategory','sincefailure','operationalassetname','raisedate', 'cumulative_maintenance')]

date_delta = max_date - min_date

MA = pd_worder_DF[pd_worder_DF['worktypecategory'] == 'Maintenance']
FA = pd_worder_DF[pd_worder_DF['worktypecategory'] == 'Failure']

#OA maintained but not failed
MA_not_FA = MA.merge(FA, on = 'operationalassetid', how = 'left', indicator = True)
MA_not_FA = MA_not_FA[MA_not_FA['_merge'] == 'left_only']['operationalassetid'].unique()

#Create dataframe for no failures
No_Failure = pd.DataFrame(columns = ['operationalassetid','worktypecategory','sincefailure','raisedate'])

for i in MA_not_FA:
    No_Failure = No_Failure.append({
                                    'operationalassetid': i,
                                    'worktypecategory':'Non Failure',
                                    'sincefailure': date_delta,
                                    'raisedate':max_date
                                    }, ignore_index=True)

#Convert OAID back to INT
No_Failure['operationalassetid']=No_Failure['operationalassetid'].apply(int)
#work_orders_OAname['operationalassetid']=work_orders_OAname['operationalassetid'].apply(int)

#get df with just OAID and OAName
work_orders_OAname = pd_worder_DF.groupby('operationalassetid',as_index = False)['operationalassetname'].last()

#Get OA Name
No_Failure = No_Failure.merge(work_orders_OAname.loc[:,('operationalassetid','operationalassetname')], how = 'left', on = 'operationalassetid')

# ### Combine Above

pd_worder_fail_DF = pd.concat([No_Failure,pd_worder_fail_DF])

pd_worder_fail_DF = pd_worder_fail_DF.reset_index()

#Sort Work Orders
pd_worder_fail_DF = pd_worder_fail_DF.sort_values(['operationalassetid','raisedate'])
MT = MT.sort_values(['operationalassetid','finishdate'])

#create column 'Cumulative Maintenance'
pd_worder_fail_DF['cumulative_maintenance'] = 0

#Convert to datetime
pd_worder_fail_DF['raisedate'] = pd.to_datetime(pd_worder_fail_DF['raisedate'])

#Calculate Cumulative Maintenance
for ID in pd_worder_fail_DF['operationalassetid'].unique():
    for i, r in pd_worder_fail_DF[pd_worder_fail_DF['operationalassetid'] == ID].iterrows():
        temp = 0
        for j, s in MT[MT['operationalassetid'] == ID].iterrows():
            if s['finishdate'] <= r['raisedate']:
                temp = temp + 1
        pd_worder_fail_DF.loc[i,'cumulative_maintenance'] = temp


#Create df with OAID and year
OA_Age = OA.loc[:,('operationalassetid','year')]

#Drop null and unknown values in year column
OA_Age = OA_Age[(OA['year'].isnull() == False)]


#Convert year to datetime
OA_Age['year'] = pd.to_datetime(OA_Age['year'])

#Merge columns
pd_worder_fail_DF = pd_worder_fail_DF.merge(OA_Age, on = 'operationalassetid', how = 'inner')

#Create Age Column
pd_worder_fail_DF['Age'] = pd_worder_fail_DF['raisedate'] - pd_worder_fail_DF['year']

#Create final dataset
WO_raisedate_final = pd_worder_fail_DF.loc[:,('operationalassetid','worktypecategory','Age','sincefailure','operationalassetname','cumulative_maintenance')]

#Map WorkTeypCategory to be 0 or 1
WO_raisedate_final['worktypecategory'] = WO_raisedate_final['worktypecategory'].map({'Non Failure':0,'Failure': 1})

WO_raisedate_final = WO_raisedate_final.rename(columns = {"worktypecategory":'Fail'})

WO_raisedate_final['Age']=WO_raisedate_final['Age'].astype(str)
WO_raisedate_final['sincefailure']=WO_raisedate_final['sincefailure'].astype(str)

#Output to S3
final = spark.createDataFrame(WO_raisedate_final)
final.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Deliverable_7_SA",mode="overwrite")


######################
#Read Files
######################

del7 = WO_raisedate_final

opass = spark.sql("SELECT operationalassetid, operationalassetname Name, sortkeyid, operationalviewid FROM parquet.operationalasset")
worder2 = spark.sql("SELECT operationalassetid FROM parquet.workorder")
opview = spark.sql("SELECT name operationalviewname, operationalviewid FROM parquet.operationalview")
lupt = spark.sql("SELECT tableid, tablename sortkeyname FROM parquet.lookuptable")
make_df = spark.sql("SELECT ta.keyid operationalassetid,MIN(ta.datavalue) Make FROM parquet.tableattribute ta LEFT OUTER JOIN parquet.lookuptable lt ON lt.tableid = ta.attributenameid WHERE lt.tablename = 'MAKE' GROUP BY keyid")


OA_DF2 = opass.join(lupt, opass.sortkeyid == lupt.tableid).drop(lupt.tableid).drop(opass.sortkeyid).drop(opass.operationalviewid).drop(opass.operationalviewid)

woA = worder2.join(make_df, worder2.operationalassetid == make_df.operationalassetid).drop(make_df.operationalassetid)
woB = woA.join(opass, woA.operationalassetid == opass.operationalassetid).drop(opass.Name).drop(opass.sortkeyid).drop(opass.operationalassetid)
work_orders_DF2 = woB.join(opview, woB.operationalviewid == opview.operationalviewid).drop(woB.operationalviewid).drop(opview.operationalviewid)


workOrderTable1 = work_orders_DF2.toPandas()
operAssTable1 = OA_DF2.toPandas()


#Removing unwanted data
operAssTable2=operAssTable1[(operAssTable1["sortkeyname"]!="INFRASTRUCTURE") & (operAssTable1["sortkeyname"]!="AUXILIARY")]

operAssTable2['Name']=operAssTable2['Name'].astype(str)
operAssTable3=operAssTable2[~operAssTable2["Name"].str.contains("GROUP") & ~operAssTable2["Name"].str.contains("AA") & ~operAssTable2["Name"].str.contains("TM") & ~operAssTable2["Name"].str.contains("PRE-TRIP") & ~operAssTable2["Name"].str.contains("WITHOUT") & ~operAssTable2["Name"].str.contains("RETIRED") & ~operAssTable2["Name"].str.contains("MISC")]

replaceList1=['terex/noel','terex/noell','terex noel','terex noell','noell','terex/fantuzzi reggiane','terex/fantuzzi']
replaceList2=['fantuzzi reggiani', 'fantuzzi','fantuzzi reggiane']
replaceList3=['chev', 'chevrolet', 'chevy']
replaceList4=['frueha','fruehauf','freuhauf']

workOrderTable1['Make']=workOrderTable1['Make'].str.strip()
workOrderTable1['Make']=workOrderTable1['Make'].str.lower()

for element in replaceList1:
    workOrderTable1.replace(to_replace=element, value='terex', inplace=True)
for element in replaceList2:
    workOrderTable1.replace(to_replace=element, value='fantuzzi', inplace=True)
for element in replaceList3:
    workOrderTable1.replace(to_replace=element, value='chevrolet', inplace=True)
for element in replaceList4:
    workOrderTable1.replace(to_replace=element, value='fruehauf', inplace=True)

workOrderTable1.replace(to_replace='liebherr-america', value='liebherr', inplace=True)
workOrderTable1.replace(to_replace='maffi', value='mafi', inplace=True)
workOrderTable1.replace(to_replace='sterling/lorain', value='sterling', inplace=True)
workOrderTable1.replace(to_replace='taylor dunn', value='taylor', inplace=True)
workOrderTable1.replace(to_replace='theuer', value='theurer', inplace=True)
workOrderTable1.replace(to_replace='utility enterprise', value='utility', inplace=True)
workOrderTable1.replace(to_replace='tug mfg', value='tug', inplace=True)
workOrderTable1.replace(to_replace='track mobile', value='trackmobile', inplace=True)
workOrderTable1.replace(to_replace='treberg', value='terberg', inplace=True)
workOrderTable1.replace(to_replace='fontai', value='fontaine', inplace=True)

#Joing the files

work_order_unique = workOrderTable1.groupby('operationalassetid',as_index = False).last()

work_order_unique = work_order_unique.loc[:,('operationalviewname','operationalassetid','Make')]

data1 = pd.merge(work_order_unique,operAssTable2[['operationalassetid','sortkeyname']],
                 how='inner', left_on=['operationalassetid'], right_on = ['operationalassetid'])

data2 = pd.merge(del7, data1,  how='inner', left_on=['operationalassetid'], right_on = ['operationalassetid'])

data2 = data2.drop(columns=['operationalassetid'])
data2 = data2.drop_duplicates()

#Output to S3
final = spark.createDataFrame(data2)

final.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Del_7_Output",mode="overwrite")
