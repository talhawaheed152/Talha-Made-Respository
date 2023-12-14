import opendatasets as od
import os
import sqlite3
import gdown
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
import numpy as np
import pyarrow.parquet as pq


class data_pipeline():
    # Creates DataFrames for the both datasets and calls all the functions required
    def __init__(self):
        print("Starting The Pipepline... ")
        self.population_df=None
        self.flights_df=None
        self.Download_Token_Kaggle()  # Downloading the Kaggle Token
        self.Download_CSV_Files()
        #Cleaning Population DataSet
        self.population_df=self.population_df.drop(columns=['population_2010','absolute_change','percent_change'])
        self.population_df=self.General_Cleaning(self.population_df)
        # Cleaning Flights DataSet & Making SQLITE files
        self.Flights_Cleaning()
        self.Create_Dimention_Tables()
        print("All Tasks Completed! ")
        
    def Create_Dimention_Tables(self):
        #Creating Cities Table (Dimention)
        City_df= pd.DataFrame()
        City_df['city']=self.population_df['city'].unique()
        City_df = pd.merge(City_df, self.population_df[['city','state', 'latitude','longitude','rank_2020','largest_city_in_state','state_capital','federal_capital','population_2020','land_area_sqkm','pop_density_sqkm']], on='city', how='left')
        self.Create_SQL_Table('Cities_t',City_df,'city')
        del City_df
        #Creating Months Table (Dimention)
        months_full = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        months_df = pd.DataFrame({'Month': months_full})
        self.Create_SQL_Table('Months_t',months_df,'Month')
        del months_df
        #Creating States Table (Dimention)
        State_df= pd.DataFrame()
        State_df['state']=self.population_df['state'].unique()
        self.Create_SQL_Table('States_t',State_df)
        del State_df
        # State Origin Destination Flights Data Frame
        State_Origin_Dest_df = pd.DataFrame()
        State_Origin_Dest_df['OriginStateName']=self.flights_df['OriginStateName'].unique()
        State_Origin_Dest_df = pd.merge(State_Origin_Dest_df, self.flights_df[['OriginStateName','DestStateName','Distance']], on='OriginStateName', how='left')
        State_Origin_Dest_df = State_Origin_Dest_df.drop_duplicates(subset='OriginStateName')
        State_Origin_Dest_df= State_Origin_Dest_df[['OriginStateName','DestStateName','Distance']]
        State_Origin_Dest_df['Origin_Destination'] = State_Origin_Dest_df['OriginStateName'] + '_' + State_Origin_Dest_df['DestStateName']
        self.Create_SQL_Table('State_Origin_Dest_t',State_Origin_Dest_df)
        del State_Origin_Dest_df
        # City Origin Destination Flights Data Frame
        City_Origin_Dest_df = pd.DataFrame()
        City_Origin_Dest_df['OriginCityName']=self.flights_df['OriginCityName'].unique()
        City_Origin_Dest_df = pd.merge(City_Origin_Dest_df, self.flights_df[['OriginCityName','DestCityName','Distance']], on='OriginCityName', how='left')
        City_Origin_Dest_df = City_Origin_Dest_df.drop_duplicates(subset='OriginCityName')
        City_Origin_Dest_df= City_Origin_Dest_df[['OriginCityName','DestCityName','Distance']]
        City_Origin_Dest_df['Origin_Destination'] = City_Origin_Dest_df['OriginCityName'] + '_' + City_Origin_Dest_df['DestCityName']
        self.Create_SQL_Table('City_Origin_Dest_t',City_Origin_Dest_df)
        del City_Origin_Dest_df
        
    def Flights_Cleaning(self):
        print("Deep Cleaning Flight DataSet Please Wait...")
        self.flights_df=self.flights_df[['Year', 'FlightDate', 'Flight_Number_Operating_Airline', 'Operating_Airline', 'Origin', 'OriginCityName', 'OriginStateName', 'Dest', 'DestCityName', 'DestStateName', 'Cancelled', 'AirTime', 'Distance', 'DistanceGroup']]
        self.flights_df=self.General_Cleaning(self.flights_df)
        self.flights_df = self.flights_df[self.flights_df['OriginStateName'].isin(self.population_df['state'])]
        self.flights_df = self.flights_df[self.flights_df['DestStateName'].isin(self.population_df['state'])]
        self.flights_df = self.flights_df[self.flights_df['Cancelled']!=True]
        self.flights_df=self.flights_df.drop(columns=['Origin','Dest','DistanceGroup','Cancelled','Year'])
        self.flights_df['OriginCityName']=(self.flights_df['OriginCityName']).str.replace(r'\s*,.*', '', regex=True)
        self.flights_df['DestCityName']=(self.flights_df['DestCityName']).str.replace(r'\s*,.*', '', regex=True)
        self.flights_df = self.flights_df[self.flights_df['OriginCityName'].isin(self.population_df['city'])]
        self.flights_df = self.flights_df[self.flights_df['DestCityName'].isin(self.population_df['city'])]
        self.flights_df['Same_State'] =  self.flights_df['OriginStateName'] ==  self.flights_df['DestStateName']
        max_distance = self.flights_df['Distance'].max()
        self.flights_df['Distance'] = pd.cut(self.flights_df['Distance'], bins=[0, 0.33 * max_distance, 0.66 * max_distance, max_distance],labels=['low', 'medium', 'high'],include_lowest=True)
        print("Deep Cleaning Done")
        self.flights_df['FlightDate'] = self.flights_df['FlightDate'].astype(str)
        self.flights_df['FlightId'] = range(1, len(self.flights_df) + 1)
        # If you want 'x_id' to be the first column, you can rearrange the columns
        self.flights_df = self.flights_df[['FlightId'] + [col for col in self.flights_df.columns if col != 'FlightId']]
        self.Create_SQL_Table('Flights_t',self.flights_df,'FlightId') #Fact Table
        
    
    # Creates the SQL Table and places it in the /data directory
    def Create_SQL_Table(self,table_name,x_dataframe,primary_key=None):
        print(f"Creating an SQLite file for {table_name}, please wait...")
        new_df=pd.DataFrame()
        sql_type_mapping = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)', 'bool': 'BOOLEAN','datetime64[us]': 'TEXT','category': 'VARCHAR(255)'}
        current_dir = os.getcwd()
        data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, table_name + '.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if primary_key!= None:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({primary_key} {sql_type_mapping[x_dataframe[primary_key].dtype.name]} PRIMARY KEY "
            new_df = pd.concat([new_df, x_dataframe[primary_key]], axis=1)
            if len(x_dataframe.columns)>1:
                create_table_query+=' ,'
            create_table_query += ', '.join([f"{col} {sql_type_mapping[x_dataframe[col].dtype.name]}" for col in x_dataframe.columns if col != primary_key])
            for col in x_dataframe.columns:
                if col != primary_key:
                    new_df = pd.concat([new_df, x_dataframe[col]], axis=1)
            del x_dataframe
        else:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ( "
            create_table_query += ', '.join([f"{col} {sql_type_mapping[x_dataframe[col].dtype.name]}" for col in x_dataframe.columns])
        create_table_query += ")"
        print(create_table_query)
        cursor.execute(create_table_query)
        # Insert data into the table
        if primary_key != None:
            for index, row in new_df.iterrows():
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(row))])})"
                cursor.execute(insert_query, tuple(row))
            del new_df
        else:
            for index, row in x_dataframe.iterrows():
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(row))])})"
                cursor.execute(insert_query, tuple(row))
        conn.commit()
        conn.close()
    
    # Cleans data by removing none values and duplicates
    def General_Cleaning(self,x_dataframe):
        print(f"Cleaning DataFrame. Please wait...")
        x_dataframe=x_dataframe.dropna()
        x_dataframe=x_dataframe.drop_duplicates()
        print('DataFrame Cleaned!')
        return x_dataframe
        
    # Downloads the CSV files from their Respective URLs
    def Download_CSV_Files(self):
        # Population
        population_file_path = 'top-100-us-cities-by-population/top100cities (3).csv'
        if not os.path.exists(population_file_path):
            dataset_url = 'https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population/download?datasetVersionNumber=3'
            od.download(dataset_url)
            self.population_df = pd.read_csv(population_file_path)
            print("Population file downloaded and loaded successfully.")
        else:
            self.population_df = pd.read_csv(population_file_path)
            print("Population file already exists. Skipping download.")
        # Airlines
        parquet_file_path = 'Combined_Flights_2020.parquet'
        if not os.path.exists(parquet_file_path):
            zip_url = "https://storage.googleapis.com/kaggle-data-sets/2529204/4295427/compressed/Combined_Flights_2020.parquet.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20231130%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20231130T133516Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=5b01962f7e57b292e5ffcc92dd596558845aec97e93cdd35658e7a6b721ce5eabf24da52d7122c844363251d8da6524bbf96000b3e65efebb92bab97ee12298780ef1af36cbdada6836f60b8129f3b7f35e4e88109dd2b7c0b67a6d22777f5d54ee6cb6bc24ab27a72c31616c9d7f49a793fc0aa43e29bcce7910fe93c51e3f81754cde28c8921c43594ac677d199bb895397a072b9650e82c56effac7f2f007a6c8ad3e13d01a00cb1c60d0704806fd1a66103907090a87326d78521c95486650c47b8eb6e1a2c928b0dd30aa6cd18386c12e6451a1abb2d3a7d89119f823c474f6ae520545b3c0622fa8952cd77c2a06464a33ec5c24e539544e345f45a95c"
            response = requests.get(zip_url)
            zip_file = ZipFile(BytesIO(response.content))
            parquet_file = zip_file.extract(zip_file.namelist()[0])
            self.flights_df = pd.read_parquet(parquet_file)
            print("Flights file downloaded and loaded successfully.")
        else:
            self.flights_df = pd.read_parquet(parquet_file_path)
            print("Flights file already exists. Skipping download.")
    
    # Downloads token for the Kaggle to allow downloading the datasets 
    def Download_Token_Kaggle(self):
        file_url = 'https://drive.google.com/uc?id=10OPFuPot4xZNb1-JHEiK4oa7wegLfPgr'
        output_file_path = 'kaggle.json'
        if not os.path.exists(output_file_path):
            gdown.download(file_url, output_file_path, quiet=False)
            print(f"Token File downloaded successfully to {output_file_path}")

def main():
    test_run = data_pipeline()

if __name__ == "__main__":
    main()
