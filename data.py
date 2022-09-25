from pyspark import SparkConf, SparkContext
import sys
import json
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,TimestampType,ArrayType,DateType






def get_api_url(i):
    url = 'https://api.covid19tracker.ca/reports/province/'+i
    return url


  
def fetch_data(i):
    response_json = {}
    json_arr = []
    try: 
        response = requests.get(url = get_api_url(i))
        response_json = response.json()
        json_arr.append(response_json)
    except requests.ConnectionError as err:
        print("**********************ERROR****************")
        print("API Fetch Failed for the province: " + i +  " with the error:")
        print(err)

    return json_arr

def main(output):
    rdd = sc.parallelize(['bc','nl','ns','nb','qc','pe','on','mb','sk','ab','yt','nt','nu'])
    api_data = rdd.map(fetch_data)
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
    df = spark.read.json(api_data, myCovDFSchema)
    df.coalesce(1).write.json(output,mode='overwrite')
    
        
if __name__ == '__main__':
    conf = SparkConf().setAppName('Covid Data')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    output = sys.argv[1]
    
    main( output) 
