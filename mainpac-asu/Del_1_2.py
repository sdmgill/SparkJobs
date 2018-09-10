import pandas as pd
import warnings
from pyspark.sql.functions import unix_timestamp,col,udf, min,max, round,lit
from pyspark.sql.types import *
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
spark = SparkSession.builder.appName('Script1-2').config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()


#Read Files
opass = spark.sql("SELECT operationalassetid, operationalassetname Name, installdate Year, operationalassetname, operationalviewid, sortkeyid FROM parquet.operationalasset")
opass2 = spark.sql("SELECT operationalassetid, operationalassetname, operationalviewid FROM parquet.operationalasset")
worder = spark.sql("SELECT operationalassetid, raisedate, finishdate, workorderid FROM parquet.workorder")
meter = spark.sql("SELECT operationalassetid, updatetime Date, metername, meterdescription FROM parquet.meter")
lupt = spark.sql("SELECT tablevalue Value, tableid, tablename SortKeyName, tabledescription CauseOfFaultDescription FROM parquet.lookuptable")
opview = spark.sql("SELECT name operationalviewname, operationalviewid FROM parquet.operationalview")
wtype = spark.sql("SELECT category worktypecategory, worktypeid FROM parquet.worktype")

tasks = spark.sql("SELECT operationalassetid, tasksynclevel, frequency FROM parquet.task")

mrA = meter.join(opass, meter.operationalassetid == opass.operationalassetid).drop(opass.operationalassetid).drop(opass.Year).drop(opass.Name).drop(opass.operationalassetname).drop(meter.metername)
mrB = mrA.join(lupt, mrA.sortkeyid == lupt.tableid).drop(lupt.tableid).drop(lupt.SortKeyName).drop(opass.sortkeyid).drop(lupt.CauseOfFaultDescription)
meter_readings = mrB.join(opview,mrB.operationalviewid == opview.operationalviewid).drop(mrB.operationalviewid).drop(opview.operationalviewid).drop(opview.operationalviewname)

operational_asset = opass.join(lupt, opass.sortkeyid == lupt.tableid).drop(lupt.Value).drop(opass.sortkeyid).drop(lupt.tableid).drop(opass.Year).drop(lupt.CauseOfFaultDescription).drop(opass.operationalassetname)

woA = worder.join(opass2, worder.operationalassetid == opass2.operationalassetid).drop(opass2.operationalassetid)
woB = woA.join(wtype, woA.workorderid == wtype.worktypeid).drop(wtype.worktypeid).drop(woA.workorderid)
work_orders = woB.join(opview,woB.operationalviewid == opview.operationalviewid).drop(opview.operationalviewid).drop(woB.operationalviewid)

#conversions
# to date
#func = udf(lambda x: datetime.strptime(x,'%m/%d/%y'),DateType())
#work_orders=work_orders.withColumn('finishdate',func(col('finishdate')))
#work_orders=work_orders.withColumn('raisedate',func(col('raisedate')))
#meter_readings=meter_readings.withColumn('Date',func(col('Date')))
# to integer
meter_readings = meter_readings.withColumn('Value',meter_readings['Value'].cast(IntegerType()))


#only take hour readings from meter readings table
meter_readings = meter_readings.filter(meter_readings.meterdescription == 'Hour Meter')

#create a subset of OAID
OA = operational_asset
subset_OAID = OA.select('operationalassetid')

subset_OAID.createOrReplaceTempView('subset_OAID')
tasks.createOrReplaceTempView('tasks')
meter_readings.createOrReplaceTempView('meter_readings')
work_orders.createOrReplaceTempView('work_orders')

tasks = spark.sql('Select * from tasks where operationalassetid in (Select distinct operationalassetid from subset_OAID)')

meter_readings = spark.sql('Select * from meter_readings where operationalassetid in (Select distinct operationalassetid from subset_OAID)')
work_orders = spark.sql('Select * from work_orders where operationalassetid in (Select distinct operationalassetid from subset_OAID)')

#exclude certain values
OA = OA.filter((OA.SortKeyName != 'AUXILIARY') & (OA.SortKeyName != 'INFRASTRUCTURE'))
OA = OA.filter(~(OA.Name.like ("%GROUP%")) & ~(OA.Name.like ("%AA%"))& ~(OA.Name.like ("%TM%")) & ~(OA.Name.like ("%PRE-TRIP%")) & ~(OA.Name.like ("%WITHOUT%")) & ~(OA.Name.like ("%RETIRED%"))& ~(OA.Name.like ("%MISC%")))
work_orders = work_orders.filter(~(work_orders.operationalassetname.like ("%GROUP%")) & ~(work_orders.operationalassetname.like ("%AA%"))& ~(work_orders.operationalassetname.like ("%TM%")) & ~(work_orders.operationalassetname.like ("%PRE-TRIP%")) & ~(work_orders.operationalassetname.like ("%WITHOUT%")) & ~(work_orders.operationalassetname.like ("%RETIRED%"))& ~(work_orders.operationalassetname.like ("%MISC%")))

#failed work orders
work_orders_failures = work_orders[work_orders['worktypecategory'] == 'Failure'].select('operationalassetid','raisedate')

#only include Maintenance
work_orders_maintenance =work_orders[work_orders['worktypecategory'] == 'Maintenance'].select('operationalassetid','finishdate')

#Get the equipment type from the OA table
OA_equip_type = OA.select('operationalassetid','SortKeyName')

#merge tables
work_orders = work_orders.join(OA_equip_type, on = 'operationalassetid' )

tasks.createOrReplaceTempView('tasks')

tasks_frequency_max = spark.sql('Select operationalassetid,max(tasksynclevel) as tasksynclevel from tasks group by operationalassetid')

tasks_frequency = tasks.join(tasks_frequency_max, (tasks.operationalassetid ==tasks_frequency_max.operationalassetid) & (tasks.tasksynclevel==tasks_frequency_max.tasksynclevel), how='inner').select(tasks.operationalassetid,tasks.frequency)

#group meter_readings by operationalassetid
reading = meter_readings.groupby('operationalassetid').sum('Value').select('operationalassetid',col('sum(Value)').alias('Value'))

#min_date
min_date = meter_readings.groupby('operationalassetid').agg(min('Date')).select('operationalassetid',col('min(Date)').alias('MinDate'))

#max_date
max_date = meter_readings.groupby('operationalassetid').agg(max('Date')).select('operationalassetid',col('max(Date)').alias('MaxDate'))

#merge readings, min_date, max_date
reading_merged = reading.join(min_date, on = 'operationalassetid').join(max_date, on ='operationalassetid')

#get date difference
reading_merged = reading_merged.withColumn('DateRange',unix_timestamp('MaxDate','yyyy-mm-dd') - unix_timestamp('MinDate','yyyy-mm-dd'))
#converting to days
reading_merged = reading_merged.withColumn('DateRange',reading_merged['DateRange'].cast(IntegerType())/(24*60*60))


#get rid of null values #Commented out bc value col. is empty
#reading_merged.createOrReplaceTempView('reading_merged')
#reading_merged=spark.sql("Select * from reading_merged where Value is not NULL")
#reading_merged=spark.sql("Select * from reading_merged where Value!=0")

#convert 'Value' to int
reading_merged = reading_merged.withColumn('Value',reading_merged['Value'].cast(IntegerType()))

#merge reading and tasks frequency
reading_merged = reading_merged.join(tasks_frequency, on = 'operationalassetid')

# get estimated date at which they should be maintained
reading_merged = reading_merged.withColumn('Estimated',(reading_merged['DateRange'] * reading_merged['frequency'])/reading_merged['Value'] )
reading_merged = reading_merged.withColumn('Estimated',(round(col('Estimated'),0)))

reading_merged = reading_merged.withColumn('PM_Margin',lit(350))
reading_merged = reading_merged.withColumn('PM_Margin_Days',(reading_merged['DateRange'] * reading_merged['PM_Margin'])/reading_merged['Value'] )
reading_merged = reading_merged.withColumn('PM_Margin_Days',(round(col('PM_Margin_Days'),0)))

#reading_merged = reading_merged.filter(reading_merged.Estimated < 800)
#reading_merged = reading_merged.filter(reading_merged.PM_Margin_Days < 800)
#reading_merged = reading_merged.filter(reading_merged.Value !=1) #Value is empty

#remove records with 0 days
#reading_merged = reading_merged.filter(reading_merged.Estimated !=0)
#reading_merged = reading_merged.filter(reading_merged.PM_Margin_Days !=0)

#create df for margin days
margin_days = reading_merged.select('operationalassetid','PM_Margin_Days')

#Get rid of negative values #Commented out bc value col. is empty
#reading_merged = reading_merged[reading_merged['Value'] >=0 ]

#get dictionary of max Date
MR_max_dict = {}

pd_reading_merged =reading_merged.toPandas()

for i, row in pd_reading_merged.iterrows():
    MR_max_dict[row['operationalassetid']] = row['MaxDate']


lst = []
#for i, row in pd_reading_merged.iterrows():
#    if row['operationalassetid'] in MR_max_dict.keys():
#        temp = row['MinDate']
#        while temp <= MR_max_dict[row['operationalassetid']]:
#            lst.append([row['operationalassetid'],row['Value'],row['MinDate'],row['MaxDate'],row['DateRange'],row['frequency'],row['Estimated'], row['PM_Margin_Days'], temp])
#            temp = temp + timedelta(days=row['Estimated'])



df1=pd.DataFrame(lst,columns = ['operationalassetid','Value','Date_x','Date_y','DateRange','frequency','Estimated','PM_Margin_Days','Cumulative Frequency'])
df1["flag"]=0
df1['task_done'] = 0

df1_subset = df1.loc[:,('operationalassetid','Cumulative Frequency')]


df_1 = df1_subset
df_2 = work_orders_maintenance.toPandas()
df_3 = work_orders_failures.toPandas()
frames = [df_1,df_2,df_3]
result = pd.concat(frames)


work_orders.createOrReplaceTempView('work_orders')

OAON=spark.sql("SELECT operationalassetid,operationalassetname from (Select operationalassetid, last_value(operationalassetname) over (partition by operationalassetid order by operationalassetid) as operationalassetname from work_orders) group by operationalassetid,operationalassetname")

OALoc =spark.sql("SELECT operationalassetid,operationalviewname from (Select operationalassetid, last_value(operationalviewname) over (partition by operationalassetid order by operationalassetid) as operationalviewname from work_orders) group by operationalassetid,operationalviewname")

OA.createOrReplaceTempView('OA')

OAEquip=spark.sql("SELECT operationalassetid,SortKeyName from (Select operationalassetid, last_value(SortKeyName) over (partition by operationalassetid order by operationalassetid) as SortKeyName from OA) group by operationalassetid,SortKeyName")


OAON = OAON.toPandas()

OAEquip=OAEquip.toPandas()
OALoc=OALoc.toPandas()

result['operationalassetid']=result['operationalassetid'].apply(int)

result = result.merge(OAON.loc[:,('operationalassetid','operationalassetname')], how = 'left',on='operationalassetid').merge(OAEquip.loc[:,('operationalassetid','SortKeyName')], how = 'left',on='operationalassetid').merge(OALoc.loc[:,('operationalassetid','operationalviewname')], how = 'left',on='operationalassetid')



result['Type'] = ''
result['All'] = ''
result['All'] = pd.to_datetime(result['All'])



#Change Type to Finish Date
result.loc[result['finishdate'].isnull() == False,'Type'] = 'PM Completed'
result.loc[result['Cumulative Frequency'].isnull() == False,'Type'] = 'Scheduled PM Est'
result.loc[result['raisedate'].isnull() == False,'Type'] = 'Failure'

#Change Finish Date to Cumulative Frequency
for i, row in result.iterrows():
    if row['Type'] == 'Scheduled PM Est':
        result.loc[i, 'All'] = row['Cumulative Frequency']
    elif row['Type'] == 'Failure':
        result.loc[i,'All'] = row['raisedate']
    else:
        result.loc[i,'All'] = row['finishdate']

#result['Cumulative Frequency'].fillna(result['finishdate'], inplace=True)

groupby = result[result['Type'] == 'Failure'].groupby('operationalassetname',as_index = False)['Type'].count()

groupby = groupby.rename(index = str, columns = {'Type':'No_Failures'})


result = result.merge(groupby, how = 'left', on = 'operationalassetname')


result['No_Failures'] = result['No_Failures'].fillna(0)

result['No_Failures'] = result['No_Failures'].astype(int)

result.loc[:,('operationalassetid','operationalassetname','operationalviewname','SortKeyName','All','Type', 'No_Failures')].to_csv('subset_result.csv')

margin_days=margin_days.toPandas()



## Meeting Compliance?

#merge with margin_days
result = result.merge(margin_days, on='operationalassetid', how='inner')


PM_Failure = result[result['Type']=='Failure']


#PM Estimated Schedules
PM_Est = result[result['Type']=='Scheduled PM Est']
PM_Est['Done?'] = False



#PM Completed Maintenance
PM_Done = result[result['Type']=='PM Completed']
PM_Done['Used?'] = False




#Create column Margin+All
PM_Est['Margin+All'] = PM_Est['All'] + pd.to_timedelta(PM_Est['PM_Margin_Days'],unit='D')


#sort by Margin + All
PM_Done = PM_Done.sort_values(['operationalassetid','All'])
PM_Est = PM_Est.sort_values(['operationalassetid','Margin+All'])

#for i, row in PM_Est.iterrows():
#    for j, row2 in PM_Done[(PM_Done['operationalassetid'] == row['operationalassetid'])].iterrows():
#        if ((row['Margin+All'] >= row2['All']) & (row2['Used?'] == 0) & (row['All'] <= row2['All'])):
#            PM_Est.loc[i,'Done?'] = True
#            PM_Done.loc[j,'Used?'] = True
#            break

df_1_1 = PM_Est
df_2_2 = PM_Done
df_3_3 = PM_Failure
frames_2 = [df_1_1,df_2_2,df_3_3]

final_result = pd.concat(frames_2)

final_result = final_result.loc[:,('operationalassetid','operationalassetname','operationalviewname','SortKeyName','All','Type', 'No_Failures','Done?')]

#Output to S3
final = spark.createDataFrame(final_result)

final.write.parquet("s3://data-lake-us-west-2-062519970039/parquet/reports/mainpac/Del_1_2_Output",mode="overwrite")
