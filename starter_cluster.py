import subprocess
import time
error = False
try:
	print("the covid API data fetch has started")
	subprocess.run(args = ["spark-submit", "data.py", "covid-api-data"])
	print("the covid API data fetch has finished")
	print("************")
	print("the covid.py code is now running")
	subprocess.run(args= ["spark-submit", "covid.py", "covid-api-data", "covid-api-data-ETL"])
	print("the covid.py code is now completed")
	print("************")
	print("the stock-ticker.py code is now running")
	#spark-submit stock_ticker.py ticker.txt test-stock
	subprocess.run(args= ["spark-submit", "stock_ticker.py", "ticker.txt", "stock-data-ETL"])
	print("the stock-ticker.py code is now completed")
	print("************")
	print("the Unemployment_gdp.py code is now running")
	subprocess.run(args= ["spark-submit", "Unemployment_gdp.py", "Gdp.csv", "UnempRate_Monthly.csv", "gdp-data-ETL"])
	print("the Unemployment_gdp.py code is now completed")
	print("************")
	print("the covid-stock.py code is now running")
	# spark-submit covid_stock.py covid-api-data-ETL/Per_Week_Cases covid-api-data-ETL/Per_Week_Vaccinations stock-data-ETL gdp-data-ETL covid-stock-gdp-unemployment-ETL
	subprocess.run(args= ["spark-submit", "covid_stock.py", "covid-api-data-ETL/Per_Week_Cases", "covid-api-data-ETL/Per_Week_Vaccinations", "stock-data-ETL", "gdp-data-ETL/unemployment_gdp", "covid-stock-gdp-unemployment-ETL"])
	print("the covid-stock.py code is now completed")
	print("************")
	print("the Ml_Model.py code is now running")
	subprocess.run(args= ["spark-submit", "Ml_model.py", "covid-api-data-ETL/ml_data", "ml_output"])
	print("the Ml_Model.py code is now completed")
	print("************")
except:
	error = True
finally:
	message = ""
	if error == True:
		message = ". EXCEPTION HAPPENED PLEASE CHECK THE LOGS ABOVE."
	print("THE EXECUTION IS COMPLETE. " + message)	
#TODO --> ADD EXCEPTION HANDLING AND EXIT AFTER IT. 
