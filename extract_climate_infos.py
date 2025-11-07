import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv("API_KEY")
api_url = os.getenv("API_URL")
city = os.getenv("CITY")
file_path = f"/home/laryssa/Documents/repositories/apache-airflow/data" # seria o caminho de um db

# intervalo de datas
start_date = datetime.today()
end_date = start_date + timedelta(days=7)

# formatando as datas
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

# catch data
url = f'{api_url}/{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={api_key}&contentType=csv'
data = pd.read_csv(url)

# split data in folders
data.to_csv(file_path + f'/week/week_{start_date}.csv')
data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + f'/temperature/temperature_{start_date}.csv')
data[['datetime', 'description', 'icon']].to_csv(file_path + f'/condition/condition_{start_date}.csv')
