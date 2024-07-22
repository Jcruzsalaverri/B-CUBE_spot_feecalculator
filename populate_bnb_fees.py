import pandas as pd
import sys
import argparse
from read_price_at_timestamp import read_price_at_timestamp
from tqdm import tqdm
import multiprocessing
from functools import partial
import numpy as np
from decimal import Decimal, getcontext
from datetime import datetime, timedelta

# Set a high precision for decimal calculations
getcontext().prec = 10

def process_chunk(chunk, current_date, verbose=False):
    bnb_prices = []
    fee_usdt_values = []

    for _, row in chunk.iterrows():
        fee_usdt = Decimal('0')
        bnb_price = None

        try:
            trade_date = datetime.strptime(row['Date(UTC)'], '%Y-%m-%d %H:%M:%S')
            if trade_date.date() >= current_date.date():
                bnb_prices.append(None)
                fee_usdt_values.append(None)
                continue

            if row['Fee Coin'] == 'BNB':
                datetime_str = row['Date(UTC)']
                try:
                    bnb_price = Decimal(str(read_price_at_timestamp('BNBUSDT', datetime_str)))
                    bnb_prices.append(float(bnb_price))
                    fee_usdt = Decimal(str(row['Fee'])) * bnb_price
                except Exception as e:
                    if verbose:
                        print(f"Warning: Error retrieving price for {datetime_str}: {e}")
                    bnb_prices.append(None)
                    fee_usdt = None
            else:
                bnb_prices.append(None)
                if row['Fee Coin'] == 'USDT':
                    fee_usdt = Decimal(str(row['Fee']))
                else:
                    fee_usdt = Decimal(str(row['Price'])) * Decimal(str(row['Fee']))

            fee_usdt_values.append(float(fee_usdt) if fee_usdt is not None else None)
        except Exception as e:
            if verbose:
                print(f"Error processing row: {e}")
            bnb_prices.append(None)
            fee_usdt_values.append(None)

    chunk['BNB Price'] = bnb_prices
    chunk['Fee USDT'] = fee_usdt_values
    return chunk

def populate_bnb_fees(input_file, output_csv, verbose=False):
    try:
        # Check file extension and read accordingly
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith('.xlsx'):
            df = pd.read_excel(input_file)
        else:
            raise ValueError("Unsupported file format. Please use CSV or Excel file.")
    except FileNotFoundError:
        print(f"Error: The input file '{input_file}' was not found.")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: The input file '{input_file}' is empty.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading the input file: {e}")
        sys.exit(1)

    # Check if required columns are present
    required_columns = ['Date(UTC)', 'Fee Coin', 'Fee', 'Price']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: The following required columns are missing from the input file: {', '.join(missing_columns)}")
        sys.exit(1)

    if verbose:
        print(f"Processing {len(df)} rows...")

    # Get the current date
    current_date = datetime.now()

    # Determine the number of CPU cores to use
    num_cores = multiprocessing.cpu_count()
    
    # Split the dataframe into chunks
    chunks = np.array_split(df, num_cores)

    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=num_cores)

    # Process the chunks in parallel
    processed_chunks = list(tqdm(
        pool.imap(partial(process_chunk, current_date=current_date, verbose=verbose), chunks),
        total=len(chunks),
        desc="Processing trades",
        disable=not verbose
    ))

    # Close the pool of worker processes
    pool.close()
    pool.join()

    # Combine the processed chunks
    df = pd.concat(processed_chunks, ignore_index=True)

    # Count rows omitted (current day trades)
    rows_omitted = df[(pd.isna(df['BNB Price'])) & (pd.isna(df['Fee USDT']))].shape[0]

    # Remove rows where BNB Price and Fee USDT are both None (current day trades)
    df = df.dropna(subset=['BNB Price', 'Fee USDT'], how='all')

    try:
        # Write the updated DataFrame to a CSV file
        df.to_csv(output_csv, index=False)
        if verbose:
            print(f"Successfully wrote output to {output_csv}")
    except PermissionError:
        print(f"Error: Permission denied when writing to {output_csv}. Make sure the file is not open in another program.")
        sys.exit(1)
    except Exception as e:
        print(f"Error writing the output file: {e}")
        sys.exit(1)

    # Calculate and print the total fees
    total_fees = Decimal('0')
    total_fees_without_bnb_discount = Decimal('0')
    fee_breakdown = {}
    rows_processed = 0
    rows_with_errors = 0

    for _, row in df.iterrows():
        rows_processed += 1
        if pd.notna(row['Fee USDT']):
            fee = Decimal(str(row['Fee USDT']))
            total_fees += fee
            fee_coin = row['Fee Coin']
            if fee_coin in fee_breakdown:
                fee_breakdown[fee_coin] += fee
            else:
                fee_breakdown[fee_coin] = fee
            
            # Calculate fees without BNB discount
            if fee_coin == 'BNB':
                total_fees_without_bnb_discount += fee / Decimal('0.75')
            else:
                total_fees_without_bnb_discount += fee
        else:
            rows_with_errors += 1

    print(f"\nTotal fees paid: ${total_fees:.2f}")
    print(f"Total fees if BNB was not used: ${total_fees_without_bnb_discount:.2f}")
    print("\nFee breakdown by coin:")
    for coin, fee in fee_breakdown.items():
        print(f"{coin}: ${fee:.2f}")
    print(f"\nTotal rows processed: {rows_processed}")
    print(f"Rows with errors: {rows_with_errors}")
    print(f"Rows omitted (current day trades): {rows_omitted}")

def main():
    parser = argparse.ArgumentParser(description="Calculate Binance spot trading fees")
    parser.add_argument("-i", "--input", default="trade_history.xlsx",
                        help="Input Excel file containing trade history (default: trade_history.xlsx)")
    parser.add_argument("-o", "--output", default="trade_history_with_bnb_prices_and_fees.csv",
                        help="Output CSV file name (default: trade_history_with_bnb_prices_and_fees.csv)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose output")
    
    args = parser.parse_args()

    # Populate BNB fees
    populate_bnb_fees(args.input, args.output, args.verbose)

if __name__ == "__main__":
    main()