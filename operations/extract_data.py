import os
from dotenv import load_dotenv
import pandas as pd
from airflow.macros import ds_add

load_dotenv()
api_key = os.getenv("API_KEY")
api_url = os.getenv("API_URL")
city = os.getenv("CITY")

def extract_data(working_path, data_interval_end):

    # catch data
    url = f'{api_url}{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={api_key}&contentType=csv'
    data = pd.read_csv(url)

    # split data in folders
    # week data, temperature data and condition data
    data.to_csv(working_path + f'week_{data_interval_end}.csv')
    data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(working_path + f'temperature_{data_interval_end}.csv')
    data[['datetime', 'description', 'icon']].to_csv(working_path + f'condition_{data_interval_end}.csv')