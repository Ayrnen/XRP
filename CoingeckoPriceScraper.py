import requests
import pandas as pd
import time
from datetime import datetime, timedelta


class PriceScraper():
    def __init__(self):
        self.url = 'https://api.coingecko.com/api/v3/coins/ripple/market_chart/range'
        self.currency = 'usd'
        self.pause_duration = 60

    def date_to_unix(self, date):
        return int(date.timestamp())
    
    def fetch_daily_data(self, start, end):
        params = {
            'vs_currency': self.currency,
            'from': self.date_to_unix(start),
            'to': self.date_to_unix(end)
        }
        while True:
            try:
                response = requests.get(self.url, params=params)
                response.raise_for_status()
                data = response.json()
                return data.get('prices', [])
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    time.sleep(self.pause_duration)
                    print(f'Retrying for date range: {start.strftime("%Y-%m-%d")} to {end.strftime("%Y-%m-%d")}')
                else:
                    print(f'Error fetching data: {e}')
                    return []




# Define constants
API_URL = "https://api.coingecko.com/api/v3/coins/ripple/market_chart/range'
CURRENCY = "usd"
PAUSE_DURATION = 60  # Pause duration in seconds after hitting rate limit

# Date range


# Convert datetime to UNIX timestamps (required by the API)
def date_to_unix(date):
    return int(date.timestamp())

# Main function to fetch data within the date range
def fetch_price_data(start_date, end_date):
    all_data = []
    current_date = start_date

    while current_date <= end_date:
        # Define the end of the current day's range
        next_date = current_date + timedelta(days=1)
        
        # Fetch data for the current day
        print(f"Fetching data for: {current_date.strftime('%Y-%m-%d')}")
        daily_data = fetch_daily_data(current_date, next_date)
        if daily_data:
            all_data.extend(daily_data)
        
        # Move to the next day
        current_date = next_date

    return all_data

# Fetch data
if __name__ == "__main__":

    start_date = datetime(2024, 8, 7)
    end_date = datetime(2024, 11, 27)

    
    price_data = fetch_price_data(start_date, end_date)

    if price_data:
        # Convert to a DataFrame
        df_prices = pd.DataFrame(price_data, columns=["timestamp", "price_usd"])
        df_prices["datetime"] = pd.to_datetime(df_prices["timestamp"], unit="ms")
        df_prices = df_prices[["datetime", "price_usd"]]  # Keep relevant columns

        # Save to a CSV file
        output_file = "xrp_usd_prices.csv"
        df_prices.to_csv(output_file, index=False)
        print(f"Data successfully saved to {output_file}")
    else:
        print("No data fetched.")
