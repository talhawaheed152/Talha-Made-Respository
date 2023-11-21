import opendatasets as od
import os
import sqlite3
import pandas as pd
import gdown

class Data_Pipeline:
    def create_population(self):
            file_url = 'https://drive.google.com/uc?id=10OPFuPot4xZNb1-JHEiK4oa7wegLfPgr'
            output_file_path = 'kaggle.json'  # Change the file name if needed
            gdown.download(file_url, output_file_path, quiet=False)
            print(f"Token File downloaded successfully to {output_file_path}")
            dataset = 'https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population/download?datasetVersionNumber=3'
            od.download(dataset)
            file_path = 'top-100-us-cities-by-population/top100cities (3).csv'
            population_df = pd.read_csv(file_path)
            population_df = population_df.drop(columns=['latitude', 'longitude', 'population_2010', 'absolute_change', 'percent_change'])
            current_dir = os.getcwd()
            data_dir = os.path.join(os.path.dirname(current_dir), 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'population.sqlite')
            conn = sqlite3.connect(db_path)
            population_df.to_sql('population', conn, index=False, if_exists='replace')
            conn.close()

    def create_Airlines(self):
            dataset ='https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022/download?datasetVersionNumber=4'
            od.download(dataset)
            file_path = 'flight-delay-dataset-20182022/Airlines.csv'
            Airlines_df = pd.read_csv(file_path,nrows=1600)
            current_dir = os.getcwd()
            data_dir = os.path.join(os.path.dirname(current_dir), 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'Airlines.sqlite')
            conn = sqlite3.connect(db_path)
            Airlines_df.to_sql('Airlines', conn, index=False, if_exists='replace')
            conn.close()

    def create_combined_flights(self):
            dataset ='https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022/download?datasetVersionNumber=4'
            od.download(dataset)
            file_path = 'flight-delay-dataset-20182022/Combined_Flights_2020.csv'
            combined_flights_df = pd.read_csv(file_path,nrows=5050000)
            current_dir = os.getcwd()
            data_dir = os.path.join(os.path.dirname(current_dir), 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'combined_flights.sqlite')
            conn = sqlite3.connect(db_path)
            combined_flights_df.to_sql('combined_flights', conn, index=False, if_exists='replace')
            conn.close()


def main():
    x = Data_Pipeline()
    x.create_population()
    print("Population.sqlite Created!")
    x.create_Airlines()
    print("Airlines.sqlite Created!")
    x.create_combined_flights()
    print("Combined_Flights.sqlite Created!")

if __name__ == "__main__":
    main()
