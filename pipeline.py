import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

# Extract - API
res = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=59.3294&longitude=18.0687&start_date=2010-01-01&end_date=2023-12-11&hourly=temperature_2m,apparent_temperature,precipitation,rain,snowfall,pressure_msl,surface_pressure,wind_speed_10m")
data = res.json()

# Transform
hourly_data = data['hourly']
df = pd.DataFrame.from_dict(hourly_data)
df['time'] = pd.to_datetime(df['time'])
df['date'] = df['time'].dt.date
df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day

df = df.rename(columns={'temperature_2m': 'temperature', 'wind_speed_10m': 'wind_speed'})
df=df[['time', 'temperature', 'rain', 'snowfall', 'wind_speed', 'year', 'month', 'day','date']]


# Load - to Snowflake
load_dotenv("snowflake.env")

conn = snowflake.connector.connect(
user=os.getenv("SNOWFLAKE_USER"),
password=os.getenv("SNOWFLAKE_PASSWORD"),
account=os.getenv("SNOWFLAKE_ACCOUNT"),
database=os.getenv("SNOWFLAKE_DaTABASE"),
schema=os.getenv("SNOWFLAKE_SCHEMA")
)

# Drop table if exists
table_name = "hourly_data"
cur = conn.cursor()
cur.execute(f"DROP TABLE IF EXISTS {table_name}")
cur.close()

write_pandas(conn, df, "hourly_data", auto_create_table=True)

# Load table of hourly_data to snowflake and create daily_data , monthly_data , yearly_data tables in snowflake and do analysis


