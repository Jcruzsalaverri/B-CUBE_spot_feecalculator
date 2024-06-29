import os
import requests
from datetime import datetime, timedelta
import time

def download_files(ticker, start_date, end_date):
    """
    Download files for the specified ticker and date range.

    Args:
    - ticker: The ticker symbol.
    - start_date: The start date in the format 'YYYYMMDD'.
    - end_date: The end date in the format 'YYYYMMDD'.
    """
    # Create a directory named after the ticker
    if not os.path.exists(ticker):
        os.makedirs(ticker)

    # Convert the date strings to datetime objects
    start_dt = datetime.strptime(start_date, '%Y%m%d')
    end_dt = datetime.strptime(end_date, '%Y%m%d')

    current_dt = start_dt

    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y-%m-%d')
        url = f'https://data.binance.vision/data/spot/daily/trades/{ticker}/{ticker}-trades-{date_str}.zip'
        file_name = f'{ticker}/{ticker}-trades-{date_str}.zip'

        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_name, 'wb') as file:
                    file.write(response.content)
                print(f"Downloaded '{file_name}' successfully.")
            else:
                print(f"Failed to download file for date {date_str}. Status code: {response.status_code}")
        except Exception as e:
            print(f"An error occurred while downloading {date_str}: {e}")

        # Wait for half a second
        time.sleep(0.5)

        # Move to the next day
        current_dt += timedelta(days=1)

if __name__ == "__main__":
    # This block will not be executed when imported as a module
    # Inputs
    ticker = 'BNBUSDT'
    start_date = '20240528'  # Example start date
    end_date = '20240528'    # Example end date

    # Call the download function
    download_files(ticker, start_date, end_date)
