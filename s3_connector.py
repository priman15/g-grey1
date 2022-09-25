import boto3, os, io
import pandas as pd 

my_key= '<YOUR-KEY>' 
my_secret= '<YOUR-SECRET-KEY>' 

my_bucket_name = 'c732-301427187-a5-dm' 
my_file_path = 'covid-api-data-ETL/prov/part-00000-a6ab4df2-f1be-4c1b-909f-8004467118f3-c000.csv' 

folders_list = ["covid-api-data-ETL/prov/", "gdp-data-ETL/unemployment_gdp/", "gdp-data-ETL/industry_gdp/", "covid-stock-gdp-unemployment-ETL/AC_Cases/",
"covid-stock-gdp-unemployment-ETL/AC_Vaccine/","covid-stock-gdp-unemployment-ETL/BMO_Cases/","covid-stock-gdp-unemployment-ETL/BMO_Vaccine/","covid-stock-gdp-unemployment-ETL/BNS_Cases/","covid-stock-gdp-unemployment-ETL/BNS_Vaccine/","covid-stock-gdp-unemployment-ETL/SHOP_Cases/",
"covid-stock-gdp-unemployment-ETL/SHOP_Vaccine/", "covid-stock-gdp-unemployment-ETL/WELL_Cases/","covid-stock-gdp-unemployment-ETL/WELL_Vaccine/","covid-stock-gdp-unemployment-ETL/Cases_vs_gdp/"]
session = boto3.Session(aws_access_key_id=my_key,aws_secret_access_key=my_secret) 
s3Client = session.client('s3') 
data = []
for folder in folders_list:
	prefix = folder +'part-'
	response = s3Client.list_objects_v2(Bucket=my_bucket_name, Prefix=prefix) 
	contents = response.get('Contents')[0]
	key = contents['Key']
	f = s3Client.get_object(Bucket=my_bucket_name, Key=key) 
	print(key)
	if key.endswith('.csv'):
		# if key in 
		if "WELL_Cases" in key:
			well_cases = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
		elif "SHOP_Cases" in key:
			shop_cases = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
		elif "AC_Cases" in key:
			ac_cases = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
		elif "BMO_Cases" in key:
			bmo_cases = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
		elif "BNS_Cases" in key:
			bns_cases = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
		elif "prov" in key:
			province_wise = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)	
		elif "industry_gdp" in key:
			industry_gdp = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
		elif "unemployment_gdp" in key:
			unemployment_gdp = pd.read_csv(io.BytesIO(f['Body'].read()), header=0)
	else:
		if "WELL_Vaccine" in key:
			well_vaccine = pd.read_json(io.BytesIO(f['Body'].read()), lines=True)
		elif "SHOP_Vaccine" in key:
			shop_vaccine = pd.read_json(io.BytesIO(f['Body'].read()), lines=True)
		elif "AC_Vaccine" in key:
			ac_vaccine = pd.read_json(io.BytesIO(f['Body'].read()), lines=True)
		elif "BMO_Vaccine" in key:
			bmo_vaccine = pd.read_json(io.BytesIO(f['Body'].read()), lines=True)
		elif "BNS_Vaccine" in key:
			bns_vaccine = pd.read_json(io.BytesIO(f['Body'].read()), lines=True)
		elif "Cases_vs_gdp" in key:	
			cases_vs_gdp = pd.read_json(io.BytesIO(f['Body'].read()), lines=True)