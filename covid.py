import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,functions
from pyspark.sql.functions import date_sub,next_day,col,lit,when,upper,from_json,explode,create_map,count,max,sum as Ssum
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,TimestampType,ArrayType,DateType
import sys
import time
import re
from math import sqrt
from datetime import timedelta, date

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def main(inputs,output):
    
    myCovDFSchema=StructType([
        StructField("province",StringType()),
        StructField("last_updated",TimestampType()),
        StructField("data",ArrayType(StructType([
            StructField("date",DateType()),
            StructField("change_cases",IntegerType()),
            StructField("change_fatalities",IntegerType()),
            StructField("change_tests",IntegerType()),
            StructField("change_hospitalizations",IntegerType()),
            StructField("change_criticals",IntegerType()),
            StructField("change_recoveries",IntegerType()),
            StructField("change_vaccinations",IntegerType()),
            StructField("change_vaccinated",IntegerType()),
            StructField("change_boosters_1",IntegerType()),
            StructField("change_vaccines_distributed",IntegerType()),
            StructField("total_cases",IntegerType()),
            StructField("total_fatalities",IntegerType()),
            StructField("total_tests",IntegerType()),
            StructField("total_hospitalizations",IntegerType()),
            StructField("total_criticals",IntegerType()),
            StructField("total_recoveries",IntegerType()),
            StructField("total_vaccinations",IntegerType()),
            StructField("total_vaccinated",IntegerType()),
            StructField("total_boosters_1",IntegerType()),
            StructField("total_vaccines_distributed",IntegerType())
            ])))
        ])

    myCovDF=spark.read.json(inputs,schema=myCovDFSchema)
    #myCovDataDF=myCovDF.withColumn("data",from_json("data",myCovDataSchema)).select(col("province"), col("data.*"))
    myCovDF=myCovDF.withColumn("Province",upper(myCovDF['province'])).drop(myCovDF['province'])
    myCovDF=myCovDF.withColumn("ProvinceFull",when(myCovDF['province']=="BC","British Columbia").otherwise(
        when(myCovDF['province']=="AB","Alberta").otherwise(
            when(myCovDF['province']=="ON","Ontario").otherwise(
                when(myCovDF['province']=="QC","Quebec").otherwise(
                    when(myCovDF['province']=="SK","Saskatchewan").otherwise(
                        when(myCovDF['province']=="MB","Manitoba").otherwise(
                            when(myCovDF['province']=="NB","New Brunswick").otherwise(
                                when(myCovDF['province']=="NS","Nova Scotia").otherwise(
                                    when(myCovDF['province']=="NL","Newfoundland and Labrador").otherwise(
                                        when(myCovDF['province']=="PE","Prince Edward Island").otherwise(
                                            when(myCovDF['province']=="YT","Yukon").otherwise(
                                                when(myCovDF['province']=="NU","Nunavut").otherwise(
                                                    when(myCovDF['province']=="NT","Northwest Territories").otherwise("Common"))))))))))))))

    myCovDataDF=myCovDF.withColumn("DataExploded",explode(myCovDF["data"])).cache()
    myDayTotCovDF=myCovDataDF.select(myCovDataDF["province"],myCovDataDF["ProvinceFull"],myCovDataDF["DataExploded.date"],myCovDataDF["DataExploded.total_cases"],myCovDataDF["DataExploded.total_tests"],myCovDataDF["DataExploded.total_vaccinations"],myCovDataDF["DataExploded.total_recoveries"],myCovDataDF["DataExploded.total_criticals"])
    myDayCumCovDF=myCovDataDF.select(myCovDataDF["province"],myCovDataDF["ProvinceFull"],myCovDataDF["DataExploded.date"],myCovDataDF["DataExploded.change_cases"],myCovDataDF["DataExploded.change_tests"],myCovDataDF["DataExploded.change_vaccinations"],myCovDataDF["DataExploded.change_recoveries"],myCovDataDF["DataExploded.change_criticals"])
    
    myWeekProvinceCovDF=myDayCumCovDF.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7)).groupBy("Province","FridayWeek").agg(Ssum(myDayCumCovDF["change_cases"]))
    #myWeekCumCovDF=myDayCumCovDF.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7)).groupBy("FridayWeek").agg(Ssum(myDayCumCovDF["change_cases"]))
    myWeekProvinceCovDF=myWeekProvinceCovDF.sort(myWeekProvinceCovDF["FridayWeek"]).withColumnRenamed("sum(change_cases)","Cases per Week").cache()
    myWeekBCCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="BC").withColumnRenamed("Cases per Week","British Columbia Cases/Week")
    myWeekABCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="AB").withColumnRenamed("Cases per Week","Alberta Cases/Week")
    myWeekONCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="ON").withColumnRenamed("Cases per Week","Ontario Cases/Week")
    myWeekQCCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="QC").withColumnRenamed("Cases per Week","Quebec Cases/Week")
    myWeekSKCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="SK").withColumnRenamed("Cases per Week","Saskatchewan Cases/Week")
    myWeekMBCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="MB").withColumnRenamed("Cases per Week","Manitoba Cases/Week")
    myWeekNBCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="NB").withColumnRenamed("Cases per Week","New Brunswick Cases/Week")
    myWeekNSCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="NS").withColumnRenamed("Cases per Week","Nova Scotia Cases/Week")
    myWeekNLCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="NL").withColumnRenamed("Cases per Week","Newfoundland  Cases/Week")
    myWeekPECovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="PE").withColumnRenamed("Cases per Week","Prince Edward Cases/Week")
    myWeekYTCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="YT").withColumnRenamed("Cases per Week","Yukon Cases/Week")
    myWeekNUCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="NU").withColumnRenamed("Cases per Week","Nuvanut  Cases/Week")
    myWeekNTCovDF=myWeekProvinceCovDF.where(myWeekProvinceCovDF["Province"]=="NT").withColumnRenamed("Cases per Week","Northwest Territories Cases/Week")
    #myWeekCovDF=myDayCovDF.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7))
    myCovDFdtSchema=StructType([
        StructField("date",StringType()),
        StructField("Cases per Week",IntegerType())
        ])
    start_dt = date(2016,1,1)
    end_dt = date(2020,1,23)
    date_case_list=[]
    for dt in daterange(start_dt, end_dt):
        date_case_list.append((dt.strftime("%Y-%m-%d"),0))
    records=spark.createDataFrame(date_case_list,schema=myCovDFdtSchema)
    myWeekDtDF=records.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7)).groupBy("FridayWeek").agg(Ssum(records["Cases per Week"])).cache()
    myWeekCovDtDFFinal=myWeekDtDF.sort(myWeekDtDF["FridayWeek"]).withColumnRenamed("sum(Cases per Week)","Cases per Week")
    myWeekVacDtDFFinal=myWeekDtDF.sort(myWeekDtDF["FridayWeek"]).withColumnRenamed("sum(Cases per Week)","Vaccines per Week")
    #myWeekDtDFFinal.write.json(output + '/dates')

    myWeekCumCovDF=myWeekProvinceCovDF.groupBy("FridayWeek").agg(Ssum(myWeekProvinceCovDF["Cases per Week"]))
    myWeekCumCovDF=myWeekCumCovDF.sort(myWeekCumCovDF["FridayWeek"]).withColumnRenamed("sum(Cases per Week)","Cases per Week")
    #myWeekCovDF.tiake(10)i
    myWeekCumCovDFFinal=myWeekCovDtDFFinal.union(myWeekCumCovDF)
    myWeekCumCovDFFinal.write.json(output + '/Per_Week_Cases', mode="overwrite")
    myWeekBCCovDF.write.json(output + '/BC_Cases', mode="overwrite")
    myWeekABCovDF.write.json(output + '/AB_Cases', mode="overwrite")
    myWeekONCovDF.write.json(output + '/ON_Cases', mode="overwrite")
    myWeekQCCovDF.write.json(output + '/QC_Cases', mode="overwrite")
    myWeekSKCovDF.write.json(output + '/SK_Cases', mode="overwrite")
    myWeekMBCovDF.write.json(output + '/MB_Cases', mode="overwrite")
    myWeekNBCovDF.write.json(output + '/NB_Cases', mode="overwrite")
    myWeekNSCovDF.write.json(output + '/NS_Cases', mode="overwrite")
    myWeekNLCovDF.write.json(output + '/NL_Cases', mode="overwrite")
    myWeekPECovDF.write.json(output + '/PE_Cases', mode="overwrite")
    myWeekYTCovDF.write.json(output + '/YT_Cases', mode="overwrite")
    myWeekNTCovDF.write.json(output + '/NT_Cases', mode="overwrite")
    myWeekNUCovDF.write.json(output + '/NU_Cases', mode="overwrite")



    myWeekProvinceVacDF=myDayCumCovDF.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7)).groupBy("Province","FridayWeek").agg(Ssum(myDayCumCovDF["change_vaccinations"]))
    #myWeekCumCovDF=myDayCumCovDF.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7)).groupBy("FridayWeek").agg(Ssum(myDayCumCovDF["change_cases"]))
    myWeekProvinceVacDF=myWeekProvinceVacDF.sort(myWeekProvinceVacDF["FridayWeek"]).withColumnRenamed("sum(change_vaccinations)","Vaccines per Week").cache()
    myWeekBCVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="BC").withColumnRenamed("Vaccines per Week","British Columbia Vaccines/Week")    
    myWeekABVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="AB").withColumnRenamed("Vaccines per Week","Alberta Vaccines/Week")
    myWeekONVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="ON").withColumnRenamed("Vaccines per Week","Ontario Vaccines/Week")
    myWeekQCVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="QC").withColumnRenamed("Vaccines per Week","Quebec Vaccines/Week")
    myWeekSKVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="SK").withColumnRenamed("Vaccines per Week","Saskatchewan Vaccines/Week")
    myWeekMBVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="MB").withColumnRenamed("Vaccines per Week","Manitoba Vaccines/Week")
    myWeekNBVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="NB").withColumnRenamed("Vaccines per Week","New Brunswick Vaccines/Week")
    myWeekNSVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="NS").withColumnRenamed("Vaccines per Week","Nova Scotia Vaccines/Week")
    myWeekNLVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="NL").withColumnRenamed("Vaccines per Week","Newfoundland  Vaccines/Week")
    myWeekPEVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="PE").withColumnRenamed("Vaccines per Week","Prince Edward Vaccines/Week")
    myWeekYTVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="YT").withColumnRenamed("Vaccines per Week","Yukon Cases/Week")
    myWeekNUVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="NU").withColumnRenamed("Vaccines per Week","Nuvanut  Cases/Week")
    myWeekNTVacDF=myWeekProvinceVacDF.where(myWeekProvinceVacDF["Province"]=="NT").withColumnRenamed("Vaccines per Week","Northwest Territories Cases/Week")
    #myWeekCovDF=myDayCovDF.withColumn("FridayWeek",date_sub(next_day('date',"Friday"),7))
    myWeekCumVacDF=myWeekProvinceVacDF.groupBy("FridayWeek").agg(Ssum(myWeekProvinceVacDF["Vaccines per Week"]))
    myWeekCumVacDF=myWeekCumVacDF.sort(myWeekCumVacDF["FridayWeek"]).withColumnRenamed("sum(Vaccines per Week)","Vaccines per Week")
    myWeekCumVacDFFinal=myWeekVacDtDFFinal.union(myWeekCumVacDF)
    myWeekCumVacDFFinal.write.json(output + '/Per_Week_Vaccinations', mode="overwrite")
    myWeekBCVacDF.write.json(output + '/BC_Vaccinations', mode="overwrite")
    myWeekABVacDF.write.json(output + '/AB_Vaccinations', mode="overwrite")
    myWeekONVacDF.write.json(output + '/ON_Vaccinations', mode="overwrite")
    myWeekQCVacDF.write.json(output + '/QC_Vaccinations', mode="overwrite")
    myWeekSKVacDF.write.json(output + '/SK_Vaccinations', mode="overwrite")
    myWeekMBVacDF.write.json(output + '/MB_Vaccinations', mode="overwrite")
    myWeekNBVacDF.write.json(output + '/NB_Vaccinations', mode="overwrite")
    myWeekNSVacDF.write.json(output + '/NS_Vaccinations', mode="overwrite")
    myWeekNLVacDF.write.json(output + '/NL_Vaccinations', mode="overwrite")
    myWeekPEVacDF.write.json(output + '/PE_Vaccinations', mode="overwrite")
    myWeekYTVacDF.write.json(output + '/YT_Vaccinations', mode="overwrite")
    myWeekNUVacDF.write.json(output + '/NU_Vaccinations', mode="overwrite")
    myWeekNTVacDF.write.json(output + '/NT_Vaccinations', mode="overwrite")

    ########COVID-19 CASES CONTIBUTION BY EACH PROVINCE########
    latest_observation_date_per_province = myDayTotCovDF.groupBy(myDayTotCovDF["ProvinceFull"]).agg(max(myDayTotCovDF["date"]).alias("Latest Observation Date"))
    latest_observation_date_per_province = latest_observation_date_per_province.withColumnRenamed("ProvinceFull", "ProvinceFull_Duplicate")
    join_condition = [latest_observation_date_per_province["Latest Observation Date"] == myDayTotCovDF["date"] , latest_observation_date_per_province["ProvinceFull_Duplicate"] == myDayTotCovDF["ProvinceFull"]]
    latest_casecount_per_province_by_date = latest_observation_date_per_province.join(myDayTotCovDF, join_condition, "inner")
    latest_casecount_per_province_by_date = latest_casecount_per_province_by_date.drop("ProvinceFull_Duplicate", "Latest Observation Date")
    total_cases_in_province_and_country = latest_casecount_per_province_by_date.crossJoin(latest_casecount_per_province_by_date.groupby().agg(Ssum('total_cases').alias('sum_total_cases')))
    total_cases_in_province_and_country = total_cases_in_province_and_country.select("*", ((total_cases_in_province_and_country['total_cases'] / total_cases_in_province_and_country['sum_total_cases']) * 100).alias("Contribution_Cases"))
    # total_cases_in_province_and_country = total_cases_in_province_and_country.drop("sum_total_cases") Can be removed if we think sum of all the cases is not useful
    total_cases_in_province_and_country = total_cases_in_province_and_country.repartition(1) #there will be only 10 rows i.e. 1 row for each province so no performance imapct is expected
    total_cases_in_province_and_country.coalesce(1).write.option("header","true").csv(output + '/prov', mode = "overwrite")
    ########COVID-19 CASES CONTIBUTION BY EACH PROVINCE########
    
    ########COVID-19 STATS FOR ML MODEL########
    ml_model_data = myCovDataDF.select(myCovDataDF["DataExploded.date"],
        myCovDataDF["DataExploded.total_cases"],
        myCovDataDF["DataExploded.total_tests"],
        myCovDataDF["DataExploded.total_vaccinations"],
        myCovDataDF["DataExploded.total_recoveries"],
        myCovDataDF["DataExploded.total_criticals"],
        myCovDataDF["DataExploded.total_hospitalizations"],
        myCovDataDF["DataExploded.total_fatalities"])
    ml_model_data.show()
    ml_model_data.write.option("header","true").csv(output + '/ml_data', mode = "overwrite")
    ########COVID-19 STATS FOR ML MODEL########


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setAppName('Covid')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark=SparkSession(sc)
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    main(inputs,output)
