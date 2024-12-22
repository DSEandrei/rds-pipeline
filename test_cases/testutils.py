import unittest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime
from snowflake.snowpark.types import *
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from utils import query_data, append_date, load_data  


class TestUtils(unittest.TestCase):

    @patch('utils.Session')  
    def test_query_data(self, MockSession):
        # Mock the database cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ('1', 'partner1', 'account1', '100', '200', '{"key": "value"}', '{"meta": "data"}', '2023-01-01', 'source_ip', 'details', 'short_id', 'ref_id', 'orig_ref_id', 'card_mask', 'source', 'type', 'code')
        ]
        mock_cursor.description = [
            ('ID',), ('PARTNER_ID',), ('ACCOUNT_ID',), ('POINTS',), ('BALANCE',), ('PROPERTIES',), ('METADATA',), ('TIMESTAMP',), ('SOURCE_IP_ADDRESS',), ('DETAILS',), ('SHORT_ID',), ('REFERENCE_ID',), ('ORIGINAL_REFERENCE_ID',), ('CARD_NUMBER_MASK',), ('SOURCE',), ('TYPE',), ('CODE',)
        ]

        # Mock the Snowflake session
        mock_session = MockSession.return_value
        mock_create_dataframe = mock_session.create_dataframe

        mock_df = MagicMock()
        mock_create_dataframe.return_value = mock_df
        mock_df.with_column.return_value = mock_df
        mock_df.columns = ['ID', 'PARTNER_ID', 'ACCOUNT_ID', 'POINTS', 'BALANCE', 'PROPERTIES', 'METADATA', 'TIMESTAMP', 'SOURCE_IP_ADDRESS', 'DETAILS', 'SHORT_ID', 'REFERENCE_ID', 'ORIGINAL_REFERENCE_ID', 'CARD_NUMBER_MASK', 'SOURCE', 'TYPE', 'CODE', 'INGESTED_DATE']

        # Define the schema
        dtype_dict = {
            'TRANSACTIONS': StructType([
                StructField('ID', StringType(), False),
                StructField('PARTNER_ID', StringType(), False),
                StructField('ACCOUNT_ID', StringType(), False),
                StructField('POINTS', StringType(), False),
                StructField('BALANCE', StringType(), False),
                StructField('PROPERTIES', MapType(StringType(), StringType()), True),
                StructField('METADATA', MapType(StringType(), StringType()), True),
                StructField('TIMESTAMP', StringType(), False),
                StructField('SOURCE_IP_ADDRESS', StringType(), True),
                StructField('DETAILS', StringType(), True),
                StructField('SHORT_ID', StringType(), False),
                StructField('REFERENCE_ID', StringType(), False),
                StructField('ORIGINAL_REFERENCE_ID', StringType(), True),
                StructField('CARD_NUMBER_MASK', StringType(), False),
                StructField('SOURCE', StringType(), False),
                StructField('TYPE', StringType(), False),
                StructField('CODE', StringType(), False),
                StructField('INGESTED_DATE', TimestampType(), False)
            ])
        }

        # Call the function
        result_df = query_data(
            pg_cursor=mock_cursor,
            query="SELECT * FROM transactions",
            rds_table="transactions",
            snowflake_session=mock_session,
            snowflake_table='TRANSACTIONS'
        )

        # Assertions
        mock_cursor.execute.assert_called_once_with("SELECT * FROM transactions")
        mock_cursor.fetchall.assert_called_once()
        mock_create_dataframe.assert_called_once()
        self.assertIsNotNone(result_df)
        self.assertIn("INGESTED_DATE", result_df.columns)

    def test_append_date(self):
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.with_column.return_value = mock_df
        mock_df.columns = ['ID', 'PARTNER_ID', 'ACCOUNT_ID', 'POINTS', 'BALANCE', 'PROPERTIES', 'METADATA', 'TIMESTAMP', 'SOURCE_IP_ADDRESS', 'DETAILS', 'SHORT_ID', 'REFERENCE_ID', 'ORIGINAL_REFERENCE_ID', 'CARD_NUMBER_MASK', 'SOURCE', 'TYPE', 'CODE', 'INGESTED_DATE']


        # Call the function
        result_df = append_date(mock_df)

        # Assertions
        mock_df.with_column.assert_called_once()
        self.assertIsNotNone(result_df)

    @patch('utils.logger')
    def test_load_data(self, mock_logger):
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_write = mock_df.write
        mock_write.save_as_table = MagicMock()

        # Call the function
        load_data(
            data=mock_df,
            snowflake_table='transactions'
        )

        # Assertions
        mock_df.write.save_as_table.assert_called_once_with('TRANSACTIONS', mode='overwrite', block=True)
        mock_logger.warning.assert_not_called()

    @patch('utils.logger')
    def test_load_data_no_data(self, mock_logger):
        # Mock DataFrame
        mock_df_empty = MagicMock()
        mock_df_empty.count.return_value = 0

        # Call the function
        load_data(
            data=mock_df_empty,
            snowflake_table='transactions'
        )

        # Assertions
        mock_df_empty.write.save_as_table.assert_not_called()
        mock_logger.warning.assert_called_once_with("No data to load to TRANSACTIONS.")

if __name__ == '__main__':
    unittest.main()