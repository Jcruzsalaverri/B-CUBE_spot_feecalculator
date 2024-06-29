import os
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import binance_market_data
import requests

def download_files_if_needed(ticker, start_date, end_date):
    """
    Download files for the specified ticker and date range if not already downloaded.

    Args:
    - ticker: The ticker symbol.
    - start_date: The start date in the format 'YYYYMMDD'.
    - end_date: The end date in the format 'YYYYMMDD'.
    """
    # Convert the date strings to datetime objects
    start_dt = datetime.strptime(start_date, '%Y%m%d')
    end_dt = datetime.strptime(end_date, '%Y%m%d')

    current_dt = start_dt

    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y-%m-%d')
        zip_file_path = f'{ticker}/{ticker}-trades-{date_str}.zip'

        if not os.path.exists(zip_file_path):
            # If the file doesn't exist, download it
            binance_market_data.download_files(ticker, start_date, end_date)
        
        # Move to the next day
        current_dt += timedelta(days=1)

def read_price_at_timestamp(ticker, datetime_str):
    try:
        # Convert datetime string to datetime object
        target_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError(f"Invalid datetime format: {datetime_str}. Expected format: 'YYYY-MM-DD HH:MM:SS'")

    try:
        # Check if the data for the specified day is downloaded, if not download it
        download_files_if_needed(ticker, target_dt.strftime('%Y%m%d'), target_dt.strftime('%Y%m%d'))
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error downloading data: {e}")

    # Find the corresponding CSV file
    file_path = f'{ticker}/{ticker}-trades-{target_dt.strftime("%Y-%m-%d")}.zip'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No data available for {target_dt.strftime('%Y-%m-%d')}")

    try:
        # Extract CSV from ZIP
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(ticker)
    except zipfile.BadZipFile:
        raise Exception(f"Error extracting ZIP file: {file_path}")

    # Read CSV into DataFrame
    csv_file = f'{ticker}/{ticker}-trades-{target_dt.strftime("%Y-%m-%d")}.csv'
    column_names = ['trade_id', 'price', 'quantity', 'quote', 'timestamp', 'taker', 'best_price']
    
    try:
        df = pd.read_csv(csv_file, names=column_names)
    except pd.errors.EmptyDataError:
        raise Exception(f"CSV file is empty: {csv_file}")
    except Exception as e:
        raise Exception(f"Error reading CSV file: {e}")

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Find nearest timestamp to target datetime
    nearest_timestamp = df['timestamp'].iloc[(df['timestamp'] - target_dt).abs().argsort()[:1]].values[0]

    # Get price at nearest timestamp
    price_at_timestamp = df.loc[df['timestamp'] == nearest_timestamp, 'price'].values[0]

    # Clean up extracted CSV file
    try:
        os.remove(csv_file)
    except Exception as e:
        print(f"Warning: Could not remove temporary CSV file: {e}")

    return price_at_timestamp

if __name__ == "__main__":
    # Inputs
    ticker = 'BNBUSDT'
    target_datetime = '2024-05-24 05:47:21'

    # Read the price at the specified timestamp
    price = read_price_at_timestamp(ticker, target_datetime)

    # Debugging print statement
    # print(f"The price of {ticker} at {target_datetime} was: {price}")
