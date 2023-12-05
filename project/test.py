import os
import unittest
import pandas as pd
from pipeline import data_pipeline
from unittest.mock import Mock
import sqlite3
import gdown

class TestDataPipeline(unittest.TestCase):

    pipeline_instance = data_pipeline()

    def test_general_cleaning(self):
        data_df = pd.DataFrame([[1, 2, 3, None], [2, 3, 3, 4], [2, 3, 3, 4], [1, 2, 3, 4]])
        cleaned_df = self.pipeline_instance.General_Cleaning(data_df)
        print("!!!!")
        assert cleaned_df.shape == (2, 4)
        pass


    def test_create_sql_table(self):
        # Create a DataFrame for testing
        test_data = {'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']}
        test_dataframe = pd.DataFrame(test_data)
        pipeline_instance = data_pipeline()
        test_table_name = 'TestTable'
        pipeline_instance.Create_SQL_Table(test_table_name, test_dataframe, primary_key='Column1')
        db_path = os.path.join(os.path.dirname(os.getcwd()), 'data', test_table_name + '.sqlite')
        self.assertTrue(os.path.exists(db_path), f"SQLite file '{test_table_name}.sqlite' not found.")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{test_table_name}'")
        result = cursor.fetchone()
        self.assertIsNotNone(result, f"Table '{test_table_name}' not found in the SQLite database.")
        pass


if __name__ == '__main__':
    unittest.main()
