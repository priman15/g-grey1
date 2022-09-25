from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import json
import requests
import json 
import datetime
import time
from pyspark.sql.functions import explode
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import lit
import random
from pyspark.sql.functions import dayofweek

#sample URL: https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=UBER&apikey=GO9YGIIB8IKGU37N

# defining a params dict for the parameters to be sent to the API
api_stock_price_schema = types.StructType([
    types.StructField('Meta Data', types.MapType(types.StringType(),types.StringType())),
    types.StructField('Time Series (Daily)', types.MapType(types.StringType(),types.MapType(types.StringType(),types.StringType()))) 
])

formatted_stock_price_schema = types.StructType([
    types.StructField('Ticker', types.StringType()),
    types.StructField('Observation Date', types.DateType()),
    types.StructField('Low', types.FloatType()),
    types.StructField('High', types.FloatType()),
    types.StructField('Open', types.FloatType()),
    types.StructField('Close', types.FloatType()),
    types.StructField('Volume', types.IntegerType())
])


def get_api_url():
    return "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&"

def get_random_api_key(stock_ticker):
    possible_api_keys = ["GO9YGIIB8IKGU37N", "61I5WJZM0YPISJN7", "P0YQOU4GHXSQIABL"]
    random.seed()
    api_key = random.choice(possible_api_keys)
    return api_key

def get_api_params(stock_ticker):
    parameters = {'symbol' : stock_ticker, 'apikey' : get_random_api_key(stock_ticker)}
    return parameters
  
def fetch_data(stock_ticker):
    response_json = {}
    try:
        response = requests.get(url = get_api_url(), params = get_api_params(stock_ticker))
        response_json = response.json()
    except requests.ConnectionError as err:
        print("**********************ERROR****************")
        print("API Fetch Failed for the stock symbol: " + str(stock_ticker) +  " with the error:")
        print(err)
    return response_json

def remove_api_failures(api_data):
    api_response_keys = api_data.keys()
    if "Meta Data" in api_response_keys and "Time Series (Daily)" in api_response_keys:
        return True
    return False    

def remove_extra_fields(filtered_data):
    del filtered_data["Meta Data"]["1. Information"]
    del filtered_data["Meta Data"]["3. Last Refreshed"]
    del filtered_data["Meta Data"]["4. Output Size"]
    del filtered_data["Meta Data"]["5. Time Zone"]
    stock_ticker = filtered_data["Meta Data"]["2. Symbol"]
    if stock_ticker.endswith(".TRT", len(stock_ticker)-4, len(stock_ticker)):
        filtered_data["Meta Data"]["2. Symbol"] = filtered_data["Meta Data"]["2. Symbol"][:-4]
    return filtered_data

def main(inputs, output):
    stock_tickers = sc.textFile(inputs)
    api_data = stock_tickers.map(fetch_data)
    api_data_filtered = api_data.filter(remove_api_failures)
    api_data_extra_keys_dropped = api_data_filtered.map(remove_extra_fields).cache()
    if not api_data_extra_keys_dropped.isEmpty():
        stock_price_df = spark.createDataFrame(api_data_extra_keys_dropped, schema = api_stock_price_schema)
        stock_price_details_date_formatted = stock_price_df.repartition(10) 
        stock_price_df_symbol_details = stock_price_df.select(explode(stock_price_df["Meta Data"]), stock_price_df["Time Series (Daily)"])
        stock_price_df_symbol_details = stock_price_df_symbol_details.drop(stock_price_df_symbol_details["key"])
        stock_price_df_symbol_details = stock_price_df_symbol_details.withColumnRenamed("value", "ticker")
        
        stock_price_df_date_exploded = stock_price_df_symbol_details.select(stock_price_df_symbol_details["ticker"], explode(stock_price_df_symbol_details["Time Series (Daily)"]))
        stock_price_df_date_exploded = stock_price_df_date_exploded.withColumnRenamed("key", "observation date")
        
        stock_price_df_price_exploded_rdd = stock_price_df_date_exploded.rdd.map(lambda x: (x["ticker"], x["observation date"], float(x["value"]["3. low"]), float(x["value"]["2. high"]), float(x["value"]["1. open"]), float(x["value"]["4. close"]), int(x["value"]["5. volume"])))
        stock_price_df_price_exploded_df = stock_price_df_price_exploded_rdd.toDF(["Ticker","Observation Date", "Low", "High", "Open", "Close" , "Volume"])
        
        stock_price_details_date_formatted = stock_price_df_price_exploded_df.withColumn("Observation Date", to_date(stock_price_df_price_exploded_df["Observation Date"], "yyyy-MM-dd"))
        
        stock_price_details_date_formatted = stock_price_details_date_formatted.sort(stock_price_details_date_formatted["Observation Date"])
        # stock_price_details_date_formatted.groupBy(stock_price_details_date_formatted["Ticker"]).count().show()
        stock_price_details_20_to_21 = stock_price_details_date_formatted.filter(stock_price_details_date_formatted["Observation Date"] >= lit("2019-01-01"))
        
        #keep weekly data from the daily data
        stock_price_details_20_to_21 = stock_price_details_20_to_21.withColumn('day_of_week', ((dayofweek(stock_price_details_20_to_21["Observation Date"])+5)%7)+1)
        stock_price_details_20_to_21_weekly_price = stock_price_details_20_to_21.where(stock_price_details_20_to_21["day_of_week"] == 5)
        stock_price_details_20_to_21_weekly_price.groupBy(stock_price_details_20_to_21_weekly_price["Ticker"]).count().show()
        
        stock_price_details_20_to_21.write.json(output, mode = "overwrite")
        prepared_data = spark.read.json(output, schema = formatted_stock_price_schema)
    else:
        print("NO DATA FOUND FROM THE ALPHAVANTAGE API. CHECK THE URL, KEY AND THE STOCK SYMBOLS BEING USED")

        
if __name__ == '__main__':
    conf = SparkConf().setAppName('Price Calculations')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output) 
