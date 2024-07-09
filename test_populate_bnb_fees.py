import unittest
import pandas as pd
import os
from populate_bnb_fees import populate_bnb_fees
from unittest.mock import patch, MagicMock

class TestPopulateBNBFees(unittest.TestCase):

    def setUp(self):
        # Create a sample input DataFrame
        self.sample_data = pd.DataFrame({
            'Date(UTC)': ['2024-01-01 00:00:00', '2024-01-02 00:00:00'],
            'Fee Coin': ['BNB', 'USDT'],
            'Fee': [0.1, 1.0],
            'Price': [100, 1]
        })
        self.input_file = 'test_input.csv'
        self.output_file = 'test_output.csv'
        
        # Save sample data to CSV file
        self.sample_data.to_csv(self.input_file, index=False)

    def tearDown(self):
        # Clean up test files
        if os.path.exists(self.input_file):
            os.remove(self.input_file)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    @patch('populate_bnb_fees.read_price_at_timestamp')
    def test_populate_bnb_fees(self, mock_read_price):
        # Mock the read_price_at_timestamp function
        mock_read_price.return_value = 200

        # Run the function
        populate_bnb_fees(self.input_file, self.output_file)

        # Check if output file was created
        self.assertTrue(os.path.exists(self.output_file))

        # Read the output file
        result_df = pd.read_csv(self.output_file)

        # Check if new columns were added
        self.assertIn('BNB Price', result_df.columns)
        self.assertIn('Fee USDT', result_df.columns)

        # Check if BNB price was correctly added
        self.assertEqual(result_df.loc[0, 'BNB Price'], 200)

        # Check if fees were correctly calculated
        self.assertEqual(result_df.loc[0, 'Fee USDT'], 0.1 * 200)  # BNB fee
        self.assertEqual(result_df.loc[1, 'Fee USDT'], 1.0)  # USDT fee

    def test_missing_input_file(self):
        with self.assertRaises(SystemExit):
            populate_bnb_fees('non_existent_file.csv', self.output_file)

    def test_missing_columns(self):
        # Create a DataFrame with missing columns
        invalid_data = pd.DataFrame({
            'Date(UTC)': ['2024-01-01 00:00:00'],
            'Fee Coin': ['BNB']
        })
        invalid_input = 'invalid_input.csv'
        invalid_data.to_csv(invalid_input, index=False)

        with self.assertRaises(SystemExit):
            populate_bnb_fees(invalid_input, self.output_file)

        os.remove(invalid_input)

if __name__ == '__main__':
    unittest.main()
