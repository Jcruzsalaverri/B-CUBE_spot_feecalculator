# Binance Spot Trading Fee Calculator

This package helps calculate the total fees paid for spot trading on Binance over a specified period.

## Features

- Downloads historical trade data from Binance
- Calculates fees paid in BNB and converts them to USDT
- Outputs total fees paid in USD

## Prerequisites

- Python 3.6+
- Pandas
- Requests

## Installation

1. Clone this repository:

 ``` git clone https://github.com/Jcruzsalaverri/B-CUBE_spot_feecalculator.git ``` 

2. Install the required packages:

 ``` pip install -r requirements.txt ``` 

## Usage

1. Log in to your [Binance](https://www.binance.com/en) account and navigate to Orders > Spot Orders.
2. Select the date range for your trade history and export it as an Excel file.
3. Place the exported Excel file in the root folder of this project and rename it to `trade_history.xlsx`.
4. Run the fee calculation script: 

 ``` python .\populate_bnb_fees.py ``` 

5. The script will create a new file named `trade_history_with_bnb_prices_and_fees.csv` and display the total fees paid in the terminal.

### Command-line Arguments

The script supports the following command-line arguments:

- `-i` or `--input`: Specify the input Excel file (default: trade_history.xlsx)
- `-o` or `--output`: Specify the output CSV file (default: trade_history_with_bnb_prices_and_fees.csv)
- `-v` or `--verbose`: Enable verbose output, including a progress bar and additional information

Example usage with verbose output:

 ``` python .\populate_bnb_fees.py -v ``` 

Example usage:

 ``` python .\populate_bnb_fees.py -i my_trade_history.xlsx -o my_output.csv ``` 

## File Description

- `binance_market_data.py`: Downloads historical trade data from Binance.
- `read_price_at_timestamp.py`: Reads the price of a ticker at a specific timestamp.
- `populate_bnb_fees.py`: Main script that calculates fees and generates the output file.

## Contributing

Contributions, issues, and feature requests are welcome. Feel free to check [issues page](https://github.com/Jcruzsalaverri/B-CUBE_spot_feecalculator/issues) if you want to contribute.



## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
