import os
import requests
import datetime as dt
from dateutil import tz


from confluent_kafka import Producer
import pandas as pd

# Classify each fuel type as 'clean', 'fossil', or 'unknown'
ENERGY_TYPES = {
    'COL': 'Fossil',
    'NG': 'Fossil',
    'OIL': 'Fossil',
    'NUC': 'Clean',
    'WAT': 'Clean',
    'WND': 'Clean',
    'SUN': 'Clean',
    'BIO': 'Clean',
    'GEO': 'Clean',
    'OTH': 'Unknown',
}

# Threshold over which we'll notify Kafka that the energy mix is clean for the region
PCT_CLEAN_THRESHOLD = 0.5

# An EIA API key is required to access the data, and it is accessed from the environment.
# You can get one here: https://www.eia.gov/opendata/register.php
EIA_API_KEY = os.environ['EIA_API_KEY']
FUEL_TYPE_URL = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/'
# EIA expects the time format to be YYYY-MM-DDTHH in UTC
EIA_TIME_FORMAT = '%Y-%m-%dT%H'
TODAY_UTC = dt.datetime.now(dt.UTC).replace(hour=0, minute=0).strftime(EIA_TIME_FORMAT)

# Create the Kafka producer
PRODUCER = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    '''Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    '''
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_notification(topic, payload):
    '''Send a message to the specified Kafka topic with the given payload. This function will
    also print the message to the console for debugging purposes.'''
    PRODUCER.produce(topic, payload.encode('utf-8'), callback=delivery_report)
    PRODUCER.poll(0)

    print(f'Message sent to {topic}: {payload}')

def summarize_power_mix():
    '''Fetch the hourly power mix data from the EIA API and summarize it by respondent. This will
    write the results to Kafka for use by the consumer of your choice. If you want to automate this
    process, you can use a scheduler like cron or Airflow to run this function at regular intervals.
    '''
    session = requests.Session()

    # Query the EIA endpoint for the most recent hourly data
    params =  {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "start": TODAY_UTC,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 5000
    }
    response = session.get(FUEL_TYPE_URL, params=params)

    df = pd.DataFrame(response.json()['response']['data'])
    # Convert the value column to float so we can add it up
    df['value'] = df['value'].astype(float)
    # Classify each fuel type as 'clean', 'fossil', or 'unknown'
    df['energy_type'] = df['fueltype'].apply(lambda x: ENERGY_TYPES[x])

    for respondent in df['respondent'].unique():
        # Slice the most recent data for each respondent
        res_df = df[(df['respondent'] == respondent)]
        max_time = res_df['period'].max()
        res_df = res_df[res_df['period'] == max_time]

        # Calculate the total energy and the percentage of clean energy
        name = res_df['respondent-name'].iloc[0]
        total_energy = res_df['value'].sum()
        pct_clean = res_df[res_df['energy_type'] == 'Clean']['value'].sum() / total_energy

        payload = f'{{"entity": "{respondent}", "name": "{name}", "period": "{max_time}", "total_energy": {total_energy}, "pct_clean": "{pct_clean}"}}'

        # Split the clean and dirty power messages into separate topics for different consumers.
        topic = 'clean-power' if pct_clean > PCT_CLEAN_THRESHOLD else 'dirty-power'
        send_notification(topic, payload)

    PRODUCER.flush()

if __name__ == '__main__':
    summarize_power_mix()
