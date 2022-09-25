from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import json
import requests
import csv
from pyspark.sql.functions import expr
from pyspark.sql.functions import *



 

def filtersize(lines):
    return len(lines[0].split("\",\"")) == 69

def filters(lines):
    return len(lines[0].split(",")) == 12

def slitting(lines):
  return list(lines[0].split("\",\""))

def filtering(lines):
  return list(lines[0].split(","))



def main(input, input2, output):
  inp = sc.textFile(input).map(lambda x: x.split('\n'))
  filteredLogs=inp.filter(filtersize)
  data = filteredLogs.map(slitting)

  schema=types.StructType([
  types.StructField('Industry', types.StringType()),
  types.StructField('Jan16',types.StringType()),
  types.StructField('Feb16',types.StringType()),
  types.StructField('Mar16',types.StringType()),
  types.StructField('Apr16',types.StringType()),
  types.StructField('May16',types.StringType()),
  types.StructField('Jun16',types.StringType()),
  types.StructField('Jul16',types.StringType()),
  types.StructField('Aug16',types.StringType()),
  types.StructField('Sep16',types.StringType()),
  types.StructField('Oct16',types.StringType()),
  types.StructField('Nov16',types.StringType()),
  types.StructField('Dec16',types.StringType()),
  types.StructField('Jan17',types.StringType()),
  types.StructField('Feb17',types.StringType()),
  types.StructField('Mar17',types.StringType()),
  types.StructField('Apr17',types.StringType()),
  types.StructField('May17',types.StringType()),
  types.StructField('Jun17',types.StringType()),
  types.StructField('Jul17',types.StringType()),
  types.StructField('Aug17',types.StringType()),
  types.StructField('Sep17',types.StringType()),
  types.StructField('Oct17',types.StringType()),
  types.StructField('Nov17',types.StringType()),
  types.StructField('Dec17',types.StringType()),
  types.StructField('Jan18',types.StringType()),
  types.StructField('Feb18',types.StringType()),
  types.StructField('Mar18',types.StringType()),
  types.StructField('Apr18',types.StringType()),
  types.StructField('May18',types.StringType()),
  types.StructField('Jun18',types.StringType()),
  types.StructField('Jul18',types.StringType()),
  types.StructField('Aug18',types.StringType()),
  types.StructField('Sep18',types.StringType()),
  types.StructField('Oct18',types.StringType()),
  types.StructField('Nov18',types.StringType()),
  types.StructField('Dec18',types.StringType()),
  types.StructField('Jan19',types.StringType()),
  types.StructField('Feb19',types.StringType()),
  types.StructField('Mar19',types.StringType()),
  types.StructField('Apr19',types.StringType()),
  types.StructField('May19',types.StringType()),
  types.StructField('Jun19',types.StringType()),
  types.StructField('Jul19',types.StringType()),
  types.StructField('Aug19',types.StringType()),
  types.StructField('Sep19',types.StringType()),
  types.StructField('Oct19',types.StringType()),
  types.StructField('Nov19',types.StringType()),
  types.StructField('Dec19',types.StringType()),
  types.StructField('Jan20',types.StringType()),
  types.StructField('Feb20',types.StringType()),
  types.StructField('Mar20',types.StringType()),
  types.StructField('Apr20',types.StringType()),
  types.StructField('May20',types.StringType()),
  types.StructField('Jun20',types.StringType()),
  types.StructField('Jul20',types.StringType()),
  types.StructField('Aug20',types.StringType()),
  types.StructField('Sep20',types.StringType()),
  types.StructField('Oct20',types.StringType()),
  types.StructField('Nov20',types.StringType()),
  types.StructField('Dec20',types.StringType()),
  types.StructField('Jan21',types.StringType()),
  types.StructField('Feb21',types.StringType()),
  types.StructField('Mar21',types.StringType()),
  types.StructField('Apr21',types.StringType()),
  types.StructField('May21',types.StringType()),
  types.StructField('Jun21',types.StringType()),
  types.StructField('Jul21',types.StringType()),
  types.StructField('Aug21',types.StringType()),])

  Gdp_data = spark.createDataFrame(data,schema)
  Gdp_data = Gdp_data.filter(Gdp_data["Jan16"]!="January 2016")
  Res_df = Gdp_data.select("Industry",expr("stack(68, 'Jan16',Jan16,'Feb16',Feb16,'Mar16',Mar16,'Apr16',Apr16,'May16',May16,'Jun16',Jun16,'Jul16',Jul16,'Aug16',Aug16,'Sep16',Sep16,'Oct16',Oct16,'Nov16',Nov16,'Dec16',Dec16,'Jan17',Jan17,'Feb17',Feb17,'Mar17',Mar17,'Apr17',Apr17,'May17',May17,'Jun17',Jun17,'Jul17',Jul17,'Aug17',Aug17,'Sep17',Sep17,'Oct17',Oct17,'Nov17',Nov17,'Dec17',Dec17,'Jan18',Jan18,'Feb18',Feb18,'Mar18',Mar18,'Apr18',Apr18,'May18',May18,'Jun18',Jun18,'Jul18',Jul18,'Aug18',Aug18,'Sep18',Sep18,'Oct18',Oct18,'Nov18',Nov18,'Dec18',Dec18,'Jan19',Jan19,'Feb19',Feb19,'Mar19',Mar19,'Apr19',Apr19,'May19',May19,'Jun19',Jun19,'Jul19',Jul19,'Aug19',Aug19,'Sep19',Sep19,'Oct19',Oct19,'Nov19',Nov19,'Dec19',Dec19,'Jan20',Jan20,'Feb20',Feb20,'Mar20',Mar20,'Apr20',Apr20,'May20',May20,'Jun20',Jun20,'Jul20',Jul20,'Aug20',Aug20,'Sep20',Sep20,'Oct20',Oct20,'Nov20',Nov20,'Dec20',Dec20,'Jan21',Jan21,'Feb21',Feb21,'Mar21',Mar21,'Apr21',Apr21,'May21',May21,'Jun21',Jun21,'Jul21',Jul21,'Aug21',Aug21) as (Date,Gdp)"))
  data = Res_df.withColumn('Gdp_new', regexp_replace('Gdp', '"', '')).withColumn('industry', regexp_replace('Industry', '"', '')).cache()
  
  
  date_data = data.withColumn("new_dt", to_date(col("Date"),"MMMyy")).drop('Date').withColumnRenamed('new_dt','Date')
  all_ind_data = date_data.filter((date_data['Industry'] != 'All industries  [T001] 4')).drop('Gdp')
  all_ind_data.printSchema()
  dates = ("2019-01-01",  "2021-08-01")
  filtered_date_data = all_ind_data.where(functions.col('Date').between(*dates)) 
  filtered_date_data.show(truncate = False)
  casted_data = filtered_date_data.withColumn('Gdp', regexp_replace('Gdp_new', ',', '')).drop('Gdp_new')
  fil= casted_data.withColumn('Gdp_new',col('Gdp').cast(types.IntegerType())).drop('Gdp')
  average_gdp_data = fil.groupBy('Industry').agg({'Gdp_new':'avg'})
  
  industry_data = data.filter(data['Industry']== 'All industries  [T001] 4').drop('Gdp')
  filter_industry_data = industry_data.withColumn('Gdp', regexp_replace('Gdp_new', ',', '')).drop('Gdp_new')
  industry_datatoInt= filter_industry_data.withColumn('Gdp_new',col('Gdp').cast(types.IntegerType())).drop('Gdp')
  
  
  unmenployemt_schema=types.StructType([
  types.StructField('date', types.StringType()),
  types.StructField('Canada',types.StringType()),
  types.StructField('Newfoundland',types.StringType()),
  types.StructField('Prince Edward Island',types.StringType()),
  types.StructField('Nova Scotia', types.StringType()),
  types.StructField('New Brunswick',types.StringType()),
  types.StructField('Ontario',types.StringType()),
  types.StructField('Quebec',types.StringType()),
  types.StructField('Manitoba',types.StringType()),
  types.StructField('Saskatchewan',types.StringType()),
  types.StructField('Alberta',types.StringType()),
  types.StructField('BC',types.StringType())])

  unemployment = sc.textFile(input2).map(lambda x: x.split('\n'))
  filtered_data = unemployment.filter(filters)
  split_data = filtered_data.map(filtering)
  unemp_data = spark.createDataFrame(split_data, unmenployemt_schema)
  df = unemp_data.filter((unemp_data['Saskatchewan']!='') & (unemp_data['date']!="Date"))
  rate = df.select('date','Canada')
  result = rate.withColumn('date_new', regexp_replace('date', '-', '')).drop('date')
  joined_data = industry_datatoInt.join(result , [result['date_new']== industry_datatoInt['Date']], 'inner')
  final_ind_data = joined_data.withColumnRenamed("Date","Month-year").withColumnRenamed("Gdp_new","Gdp").withColumnRenamed("Canada","Unemployment Rate")
  final_data = final_ind_data.select('Month-year','Gdp','Unemployment Rate')
  #Changes to get back the original date format Start
  final_data = final_data.withColumn("Date_original_format", final_data["Month-year"])
  #Changes to get back the original date format End
  ata = final_data.withColumn('Month-year_n', to_date(col('Month-year'),"MMMyy")).drop('Month-year').withColumnRenamed('Month-year_n','Month-year')
  ata.show(truncate = False)
  
  #Changes to get back the original date format Start
  ata = ata.coalesce(1).cache()
  ata_without_original_dates = ata.drop("Date_original_format")
  ata_without_original_dates.write.csv("s3://c732-301427187-a5-dm/unemp_gdp_data_formatted", mode='overwrite',header = True)
  ata_copy_for_covid_correlation = ata.drop("Month-year")
  ata_copy_for_covid_correlation.coalesce(1).write.csv(output +'/unemployment_gdp', mode='overwrite',header = True)
  average_gdp_data.coalesce(1).write.csv(output +'/industry_gdp', mode='overwrite',header = True)
  # ata.coalesce(1).write.csv(output, mode='overwrite',header = True) Original code has been commented
  #Changes to get back the original date format End

  #final_data.coalesce(1).write.csv(output, mode='overwrite',header = True)
  # final_data.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(output)
  
if __name__ == '__main__':
    conf = SparkConf().setAppName('Price Calculations')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    input = sys.argv[1]
    input2 = sys.argv[2]
    output = sys.argv[3]
    main(input, input2, output)    

