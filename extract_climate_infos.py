import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv("API_KEY")
api_url = os.getenv("API_URL")
city = os.getenv("CITY")

# intervalo de datas
start_date = datetime.today()
end_date = start_date + timedelta(days=7)

# formatando as datas
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

city = os.get