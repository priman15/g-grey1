
# The Impact of COVID-19 pandemic on businesses and economy in Canada

The Covid-19 outbreak has disrupted the socio-economic balance, not only it had disrupted the health systems but also had a massive impact on businesses and financial institutions. The pandemic also led to demographic transformations in Canada. The effects were quite evident on large as well small businesses. While covid is known to brought negative consequences but on the hindsight some organizations have either profited or prevented a financial crisis. The main goal of the project is to analyse the impacts and study the trends which covid has brought to the socio-demographics of Canada and its provinces.


## Tech Stack

**Programming Language:** Python3 \
**Cloud Service Provider:** Amazon Web Services (AWS)\
**Cloud Services:** AWS Elastic Map Reduce(EMR), Simple Storage Service(S3)\
**Big Data Tools:** Apache Spark \
**Visualization:** Microsoft Power BI



## Contributors

- [Divye Maheshwari](https://csil-git1.cs.surrey.sfu.ca/dma96)
- [Priyanka Manam](https://csil-git1.cs.surrey.sfu.ca/pma49)
- [Akshita Sharma](https://csil-git1.cs.surrey.sfu.ca/asa341)



## Demo

[YouTube](https://youtu.be/vFMSCBIvjpg)


## Dataset

#### Get Covid-19 Related Data by Province

```http
  GET https://api.covid19tracker.ca/reports/province/<province_name>
```

| Parameter | Type     | Description                | Example                    |
| :-------- | :------- | :------------------------- | :------------------------- |
| `province_name` | `string` | **Required**. The name of the Canadian Province | AB |

#### Get Stocks Data for Canadian Stocks 
- [Alpha Vantage API Documentation](https://www.alphavantage.co/documentation/)

```http
  GET https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&symbol=<stock_ticker_name>&apikey=<your_api_key>
```

| Parameter | Type     | Description                | Example                    |
| :-------- | :------- | :------------------------- | :------------------------- |
| `stock_ticker_name` | `string` | **Required**. The short name(ticker) of the stock| BMO.TRT |
| `your_api_key` | `string` | **Required**. Your API Key for API | |

#### Get GDP data from Statistics Canada 
- [Data](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610043402)

#### Get Unemployment data from Statistics Canada 
- [Data](https://www.stats.gov.nl.ca/Statistics/Topics/labour/PDF/UnempRate_Monthly.pdf)
## How to Run
The application is capable of running on both Amazon Web Services(AWS) and the Simon Fraser University's inhouse Spark Cluster. The only difference is the starter code that is used.


- Clone the branch to your local system. 

#### To run on SFU's inhouse Spark Cluster

- Copy code to the Cluster in a directory
- cd into the directory 
- Copy the 3 input files to HDFS namely: 
```bash    
ticker.txt 
Gdp.csv 
UnempRate_Monthly.csv 
```
- Run the follwing command to start the execution
```bash    
spark-submit starter_cluster.py 
```

#### To run on AWS EMR & S3 

- Create a new S3 bucket named
```bash    
c732-301427187-a5-dm
```
- Create a new EMR cluster. Use the bootstrap.sh file to add a bootstrap step
- Once the cluster is running add a new Step by clicking on the 'Add Step' button and use the following values:
```bash    
Step type (Dropdown): Spark Application
Name (Text): Team-Grey-Project-Run
Deploy mode (Dropdown): Client
Spark-submit options (Text): --conf spark.yarn.maxAppAttempts=1
Application location (Text): s3://c732-301427187-a5-dm/starter.py
Action on failure (Dropdown): Continue [Default]
Click Add (Lower right)
```

## Order of execution and the outputs created in between
The application runs in the order listed in the table below and the outputs are shared wherever necessary
 | File | Input (s)    | Output                | Responsibility                    |
| :-------- | :------- | :------------------------- | :------------------------- |
| `s3://c732-301427187-a5-dm/data.py` | | s3://c732-301427187-a5-dm/covid-api-data | Fetch Covid-19 related data province wise |
| `s3://c732-301427187-a5-dm/covid.py` | `s3://c732-301427187-a5-dm/covid-api-data` | s3://c732-301427187-a5-dm/covid-api-data-ETL | Generates weekly statistics for cases and vaccinations for each province and overall. Also provides data to the ML Model|
| `s3://c732-301427187-a5-dm/stock_ticker.py` | `s3://c732-301427187-a5-dm/ticker.txt` | s3://c732-301427187-a5-dm/stock-data-ETL | Fetches and generates required values for 5 Canadian Stocks namely Air Canada(AC), The Bank of Montreal(BMO), Shopify(SHOP), Well Health Technologies Corp(WELL), The Bank of Nova Scotia (BNS)|
| `s3://c732-301427187-a5-dm/Unemployment_gdp.py` | `s3://c732-301427187-a5-dm/Gdp.csv, s3://c732-301427187-a5-dm/UnempRate_Monthly.csv` | s3://c732-301427187-a5-dm/gdp-data-ETL | Generates statistics to compare the shift in GDP and Unemployment rate due to the pandemic |
| `s3://c732-301427187-a5-dm/covid_stock.py` | `s3://c732-301427187-a5-dm/covid-api-data-ETL/Per_Week_Cases, s3://c732-301427187-a5-dm/covid-api-data-ETL/Per_Week_Vaccinations, s3://c732-301427187-a5-dm/stock-data-ETL, s3://c732-301427187-a5-dm/gdp-data-ETL/unemployment_gdp` | s3://c732-301427187-a5-dm/covid-stock-gdp-unemployment-ETL | Correlates the Covid 19 VS Stock Closing Price; Vaccinations Administered VS Stock Closing Price;  Covid 19 VS Cases GDP and Unemployment|
| `s3://c732-301427187-a5-dm/Ml_model.py` | `s3://c732-301427187-a5-dm/covid-api-data-ETL/ml_data` | s3://c732-301427187-a5-dm/ml_output  | Saves a Trained Model ready to be used for prediction |
| `s3://c732-301427187-a5-dm/Ml_model_predict.py` | `s3://c732-301427187-a5-dm/ml_output ` | Predicted value of expected fatalities | Predicts the number of deaths by using cases, hospitalisations, critical cases, tests, vaccinations, recoveries as features. |


## Loading latest data from S3 bucket to Power BI 
 - Open Power BI 
 - Import the Financial-Impact-Of-Covid-Visualization.pbix file 
 - Select Get data > Other > Python script
 - Insert the code written in s3_connector.py
 - Update the AWS keys, bucket name
 - Press OK
    



    
    