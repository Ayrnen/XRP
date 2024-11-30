import requests
import pandas as pd
import time
import datetime as dt
class GeckoCollector():
    def __init__(self):
        self.url = 'https://api.coingecko.com/api/v3/coins/ripple/market_chart/range'
        self.currency = 'usd'
        self.pause_duration = 60

    def get_day_data(self, start, end):
        params = {
            'vs_currency': self.currency,
            'from': self.date_to_unix(start),
            'to': self.date_to_unix(end)
        }

        while True:
            try:
                print('Collecting:', start)
                response = requests.get(self.url, params=params)
                response.raise_for_status()
                data = response.json()
                return data['prices']
            
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    print('Rate limit reached. Waiting 60 seconds...')
                    time.sleep(self.pause_duration)

                else:
                    print(f'Error fetching data: {e}')
                    exit()
    
    def parse_data(self, data):
        df = pd.DataFrame(data, columns=['TimeStamp', 'Price', 'Currency'])
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], unit='ms')
        return df

    
    def csv_filename(self, addition):
        today = dt.datetime.now().strftime('%Y-%m-%d')
        return f'{addition}_{today}.csv'

    def save_data(self, data, filename):
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)


# Fetch data
if __name__ == '__main__':
    collector = GeckoCollector()
    start_time = dt.datetime.now()

    start_date = int(dt.datetime(2024, 8, 7).timestamp())
    end_date = int(dt.datetime(2024, 11, 27).timestamp())

    print('Begin Data Pull')
    raw_data = []
    current_date = start_date
    while current_date <= end_date:

        next_date = current_date + dt.timedelta(days=1)
        
        daily_data = collector.get_day_data(current_date, next_date)
        raw_data.extend(daily_data)

        current_date = next_date


    final_data = collector.parse_data(raw_data)


    print('Save to CSV')
    filename = collector.csv_filename('Offer_Create')
    collector.save_data(final_data, filename)

    end_time = dt.datetime.now()
    print(f'Runtime: {end_time - start_time}')
    print('Data Collection Complete')