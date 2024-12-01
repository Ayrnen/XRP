import requests
import pandas as pd
import time
import datetime as dt


class GeckoCollector:
    def __init__(self):
        self.url = 'https://api.coingecko.com/api/v3/coins/ripple/market_chart/range'
        self.pause_duration = 60

    @staticmethod
    def date_to_unix(date):
        return int(date.timestamp())

    def get_day_data(self, start, end, currency):
        params = {
            'vs_currency': currency,
            'from': start,
            'to': end
        }

        while True:
            try:
                print(f'Currency: {currency}, Day: {dt.datetime.utcfromtimestamp(start)}')
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
                    
    @staticmethod
    def parse_data(data, currency):
        df = pd.DataFrame(data, columns=['TimeStamp', 'Price'])
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], unit='ms')
        df['Currency'] = currency
        return df

    @staticmethod
    def csv_filename(addition):
        today = dt.datetime.now().strftime('%Y-%m-%d')
        return f'/CSV_Files/{addition}_{today}.csv'

    @staticmethod
    def save_data(data, filename):
        data.to_csv(filename, index=False)


# Fetch data
if __name__ == '__main__':
    collector = GeckoCollector()
    start_time = dt.datetime.now()
    
    start_date = dt.datetime(2024, 8, 7)
    end_date = dt.datetime(2024, 11, 27)
    # currencies = ['usd', 'eur', 'cny']
    currencies = ['usd']

    print('Begin Data Pull')
    all_data = []

    for currency in currencies:
        current_date = start_date
        while current_date <= end_date:
            next_date = current_date + dt.timedelta(days=1)
            start_unix = collector.date_to_unix(current_date)
            end_unix = collector.date_to_unix(next_date)

            daily_data = collector.get_day_data(start_unix, end_unix, currency)
            parsed_data = collector.parse_data(daily_data, currency)
            all_data.append(parsed_data)

            current_date = next_date

    # Combine all data into one DataFrame
    final_data = pd.concat(all_data, ignore_index=True)

    print('Save to CSV')
    filename = collector.csv_filename('XRP_Prices')
    collector.save_data(final_data, filename)

    end_time = dt.datetime.now()
    print(f'Runtime: {end_time - start_time}')
    print('Data Collection Complete')