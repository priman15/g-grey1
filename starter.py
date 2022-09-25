import subprocess
import time
error = False
try:
	print("the covid API data fetch has started")
	subprocess.run(args = ["spark-submit", "s3://c732-301427187-a5-dm/data.py", "s3://c732-301427187-a5-dm/covid-api-data"])
	print("the covid API data fetch has finished")
	print("************")
	print("the covid.py code is now running")
	subprocess.run(args= ["spark-submit", "s3://c732-301427187-a5-dm/covid.py", "s3://c732-301427187-a5-dm/covid-api-data", "s3://c732-301427187-a5-dm/covid-api-data-ETL"])
	print("the covid.py code is now completed")
	print("************")
	print("the stock-ticker.py code is now running")
	#spark-submit stock_ticker.py ticker.txt test-stock
	subprocess.run(args= ["spark-submit", "s3://c732-301427187-a5-dm/stock_ticker.py", "s3://c732-301427187-a5-dm/ticker.txt", "s3://c732-301427187-a5-dm/stock-data-ETL"])
	print("the stock-ticker.py code is now completed")
	print("************")
	print("the Unemployment_gdp.py code is now running")
	subprocess.run(args= ["spark-submit", "s3://c732-301427187-a5-dm/Unemployment_gdp.py", "s3://c732-301427187-a5-dm/Gdp.csv", "s3://c732-301427187-a5-dm/UnempRate_Monthly.csv", "s3://c732-301427187-a5-dm/gdp-data-ETL"])
	print("the Unemployment_gdp.py code is now completed")
	print("************")
	print("the covid-stock.py code is now running")
	# spark-submit covid_stock.py covid-api-data-ETL/Per_Week_Cases covid-api-data-ETL/Per_Week_Vaccinations stock-data-ETL gdp-data-ETL covid-stock-gdp-unemployment-ETL
	subprocess.run(args= ["spark-submit", "s3://c732-301427187-a5-dm/covid_stock.py", "s3://c732-301427187-a5-dm/covid-api-data-ETL/Per_Week_Cases", "s3://c732-301427187-a5-dm/covid-api-data-ETL/Per_Week_Vaccinations", "s3://c732-301427187-a5-dm/stock-data-ETL", "s3://c732-301427187-a5-dm/gdp-data-ETL/unemployment_gdp", "s3://c732-301427187-a5-dm/covid-stock-gdp-unemployment-ETL"])
	print("the covid-stock.py code is now completed")
	print("************")
	print("the Ml_Model.py code is now running")
	subprocess.run(args= ["spark-submit", "s3://c732-301427187-a5-dm/Ml_model.py", "s3://c732-301427187-a5-dm/covid-api-data-ETL/ml_data", "s3://c732-301427187-a5-dm/ml_output"])
	print("the Ml_Model.py code is now completed")
	print("************")
except:
	error = True
finally:
	message = ""
	if error == True:
		message = ". EXCEPTION HAPPENED PLEASE CHECK THE LOGS ABOVE."
	print("THE EXECUTION IS COMPLETE. " + message)	