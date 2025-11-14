import os
from dotenv import load_dotenv
import pandas as pd
from airflow.macros import ds_add

load_dotenv()
api_key = os.getenv("API_KEY")
api_url = os.getenv("API_URL")
city = os.getenv("CITY")
#file_path = f"/home/laryssa/Documents/repositories/apache-airflow/data" # seria o caminho de um db

def extract_data(working_path, data_interval_end):
    print('OIIIIIIIIIIIIIIIIIIIIIII!!!!!!!!!!!!!!!!!!!')

    # intervalo de datas
    # start_date = datetime.today()
    # end_date = start_date + timedelta(days=7)

    # formatando as datas
    # start_date = start_date.strftime('%Y-%m-%d')
    # end_date = end_date.strftime('%Y-%m-%d')

    print('data inteval end ---------------------', data_interval_end)

    # catch data
    url = f'{api_url}/{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={api_key}&contentType=csv'
    data = pd.read_csv(url)

    # split data in folders
    # week data, temperature data and condition data
    data.to_csv(working_path + f'/week/week_{data_interval_end}.csv')
    data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(working_path + f'/temperature={data_interval_end}.csv')
    data[['datetime', 'description', 'icon']].to_csv(working_path + f'/condition={data_interval_end}.csv')