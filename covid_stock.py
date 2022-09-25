import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,functions
from pyspark.sql.functions import to_date,date_sub,next_day,col,lit,when,upper,from_json,explode,create_map,month,year,dayofmonth,avg,count,max,sum as Ssum
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,TimestampType,ArrayType,DateType,FloatType
import sys
import time
import re
from datetime import datetime
from math import sqrt

def extract_data(line_re):
    #line_re = myinp.split(",")
    datedic= {'Jan': '01',
            'Feb': '02',
            'Mar': '03',
            'Apr': '04',
            'May': '05',
            'Jun': '06',
            'Jul': '07',
            'Aug': '08',
            'Sep': '09',
            'Oct': '10',
            'Nov': '11',
            'Dec': '12'}
    monthwrd=line_re[2][0:3]
    yr=line_re[2][3:5]
    date_str="20" + yr + "-" + datedic[monthwrd] + "-" + "01"
    #myDate=dt.datetime.strptime(date_str,"%Y-%m-%d")
    #myDate=myDate.date()
    #myDate_wT=myDate.isoformat()
    #print(myDate)
    #return (str(date_str),int(line_re[1]),float(line_re[2]))
    return (str(date_str),int(line_re[0]),float(line_re[1]))

def main(stockinputs,vaccineinputs,covidinputs,unemGDPinputs,output):
    
    myStockDFSchema=StructType([
        StructField("Ticker",StringType()),
        StructField("Observation Date",DateType()),
        StructField("Low",FloatType()),
        StructField("High",FloatType()),
        StructField("Open",FloatType()),
        StructField("Close",FloatType()),
        StructField("Volume",IntegerType()),
        StructField("day_of_week",IntegerType()),
        ])
    
    myCovDFSchema=StructType([
        StructField("FridayWeek",DateType()),
        StructField("Cases per Week",IntegerType())
        ])

    myVacDFSchema=StructType([
        StructField("FridayWeek",DateType()),
        StructField("Vaccines per Week",IntegerType())
        ])

    myUnempDFSchema=StructType([
        StructField("Date_unemp",StringType()),
        StructField("GDP",IntegerType()),
        StructField("Unemployment Rate",FloatType())
        ])

    myStockAllDF=spark.read.json(stockinputs,schema=myStockDFSchema)
    myCovDF=spark.read.json(covidinputs,schema=myCovDFSchema)
    myCovDFmonth=myCovDF.withColumn("Month",month(myCovDF["FridayWeek"])).withColumn("Year",year(myCovDF["FridayWeek"]))
    myCovDFmonthTemp=myCovDFmonth.groupBy(myCovDFmonth["Month"],myCovDFmonth["Year"]).agg(Ssum(myCovDFmonth["Cases per Week"]))
    #myCovDFmonthSum=myCovDFmonth.join(myCovDFmonthTemp,((myCovDFmonthTemp["Month"]==myCovDFmonth["Month"]) & (myCovDFmonth["Year"]==myCovDFmonthTemp["Year"]))).drop(myCovDFmonth["Year"]).drop(myCovDFmonth["Month"])
    #myCovDFmonthSumFinal=myCovDFmonthSum.where(dayofmonth(myCovDFmonthSum["FridayWeek"])=="01")
    #myCovDFmonth.sort(myCovDFmonth["FridayWeek"]).write.json(output + "/Monthly1")
    #myCovDFmonthSum.sort(myCovDFmonthSum["FridayWeek"]).write.json(output + "/monthTemp")
    #myCovDFmonthSumFinal.sort(myCovDFmonthSumFinal["FridayWeek"]).write.json(output + "/Monthly")
    myVacDF=spark.read.json(vaccineinputs,schema=myVacDFSchema)
    #myCovDataDF=myCovDF.withColumn("data",from_json("data",myCovDataSchema)).select(col("province"), col("data.*"))
    myStockDF = myStockAllDF.withColumn("FridayWeekStock",date_sub(next_day('Observation Date',"Friday"),7)).groupBy("FridayWeekStock","Ticker").agg(avg(myStockAllDF["Close"]))
    

    myjoinCovStk=myStockDF.join(myCovDF,myCovDF["FridayWeek"]==myStockDF["FridayWeekStock"]).select(myStockDF["FridayWeekStock"], myCovDF["Cases per Week"],myStockDF["avg(Close)"], myStockDF["Ticker"]) 
    myjoinCovStk = myjoinCovStk.withColumnRenamed("avg(Close)","Close")
    myjoinCovStkAC=myjoinCovStk.where(myjoinCovStk["Ticker"]=="AC")
    myjoinCovStkBMO=myjoinCovStk.where(myjoinCovStk["Ticker"]=="BMO")
    myjoinCovStkSHOP=myjoinCovStk.where(myjoinCovStk["Ticker"]=="SHOP")
    myjoinCovStkWELL=myjoinCovStk.where(myjoinCovStk["Ticker"]=="WELL")
    myjoinCovStkBNS=myjoinCovStk.where(myjoinCovStk["Ticker"]=="BNS")
    
    myjoinCovStkAC.coalesce(1).sort(myjoinCovStkAC["FridayWeekStock"]).write.option("header","true").csv(output + "/AC_Cases", mode="overwrite")
    myjoinCovStkBMO.coalesce(1).sort(myjoinCovStkBMO["FridayWeekStock"]).write.option("header","true").csv(output + "/BMO_Cases", mode="overwrite")
    myjoinCovStkSHOP.coalesce(1).sort(myjoinCovStkSHOP["FridayWeekStock"]).write.option("header","true").csv(output + "/SHOP_Cases", mode="overwrite")
    myjoinCovStkWELL.coalesce(1).sort(myjoinCovStkWELL["FridayWeekStock"]).write.option("header","true").csv(output + "/WELL_Cases", mode="overwrite")
    myjoinCovStkBNS.coalesce(1).sort(myjoinCovStkBNS["FridayWeekStock"]).write.option("header","true").csv(output + "/BNS_Cases", mode="overwrite")
    
    myjoinVacStk=myStockDF.join(myVacDF,myVacDF["FridayWeek"]==myStockDF["FridayWeekStock"]).select(myVacDF["FridayWeek"], myVacDF["Vaccines per Week"],myStockDF["avg(Close)"], myStockDF["Ticker"])
    myjoinVacStk = myjoinVacStk.withColumnRenamed("avg(Close)","Close")
    myjoinVacStkAC=myjoinVacStk.where(myjoinVacStk["Ticker"]=="AC")
    myjoinVacStkBMO=myjoinVacStk.where(myjoinVacStk["Ticker"]=="BMO")
    myjoinVacStkSHOP=myjoinVacStk.where(myjoinVacStk["Ticker"]=="SHOP")
    myjoinVacStkWELL=myjoinVacStk.where(myjoinVacStk["Ticker"]=="WELL")
    myjoinVacStkBNS=myjoinVacStk.where(myjoinVacStk["Ticker"]=="BNS")

    myjoinVacStkAC.coalesce(1).sort(myjoinCovStkAC["FridayWeekStock"]).write.json(output + "/AC_Vaccine", mode="overwrite")
    myjoinVacStkBMO.coalesce(1).sort(myjoinCovStkBMO["FridayWeekStock"]).write.json(output + "/BMO_Vaccine", mode="overwrite")
    myjoinVacStkSHOP.coalesce(1).sort(myjoinCovStkSHOP["FridayWeekStock"]).write.json(output + "/SHOP_Vaccine", mode="overwrite")
    myjoinVacStkWELL.coalesce(1).sort(myjoinCovStkWELL["FridayWeekStock"]).write.json(output + "/WELL_Vaccine", mode="overwrite")
    myjoinVacStkBNS.coalesce(1).sort(myjoinCovStkBNS["FridayWeekStock"]).write.json(output + "/BNS_Vaccine", mode="overwrite")

    records = spark.read.csv(unemGDPinputs,header=True).rdd
    myunempRDD=records.map(extract_data)
    #records.saveAsTextFile(output)
    myunempDF=spark.createDataFrame(myunempRDD,schema=myUnempDFSchema).cache()
    myunempDF=myunempDF.withColumn("unempdate",to_date(myunempDF["Date_unemp"],"yyyy-MM-dd")).drop(myunempDF["Date_unemp"])
    myunempjoinCases=myunempDF.join(myCovDFmonthTemp,((month(myunempDF["unempdate"])==myCovDFmonthTemp["Month"]) & (year(myunempDF["unempdate"])==myCovDFmonthTemp["Year"]))).drop(myCovDFmonthTemp["Month"]).drop(myCovDFmonthTemp["Year"]).withColumnRenamed("sum(Cases per Week)","Cases per Week")
    myunempjoinCases.coalesce(1).sort(myunempjoinCases["unempdate"]).write.json(output + "/Cases_vs_gdp", mode="overwrite")

if __name__ == '__main__':
    covidinputs = sys.argv[1]
    vaccineinputs = sys.argv[2]
    stockinputs = sys.argv[3]
    unemGDPinputs = sys.argv[4]
    output = sys.argv[5]

    conf = SparkConf().setAppName('Covid')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark=SparkSession(sc)
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    main(stockinputs,vaccineinputs,covidinputs,unemGDPinputs,output)
