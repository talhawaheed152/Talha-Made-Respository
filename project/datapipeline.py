import opendatasets as od
import os
import sqlite3
import pandas as pd
from github import Github
import gdown

class Data_Pipeline:
    def __init__(self):   
        file_url = 'https://drive.google.com/uc?id=10OPFuPot4xZNb1-JHEiK4oa7wegLfPgr'
        output_file_path = 'kaggle.json'  # Change the file name if needed
        gdown.download(file_url, output_file_path, quiet=False)
        print(f"Token File downloaded successfully to {output_file_path}")
        dataset ='https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population/download?datasetVersionNumber=3'
        od.download(dataset)
        file_path = 'top-100-us-cities-by-population/top100cities (3).csv'
        self.population_df = pd.read_csv(file_path)
        self.population_df=self.population_df.drop(columns=['latitude','longitude','population_2010','absolute_change','percent_change'])

    def Create_Connection(self):
        db_path = 'population.sqlite'
        conn = sqlite3.connect(db_path)
        self.population_df.to_sql('population', conn, index=False, if_exists='replace')
        conn.close()

    def Create_Data_In_Github(self):
        # GitHub credentials
        github_token = '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'
        repo_name = 'talhawaheed152/Talha-Made-Respository'  # Replace with your GitHub repository username/repo_name

        # Create a GitHub instance
        g = Github(github_token)

        # Get the repository
        repo = g.get_repo(repo_name)

        # Specify the file path in the repository
        file_path_2 = 'data/population.sqlite'

        # Commit and push the SQLite file to the repository
        with open(db_path, 'rb') as file:
            content = file.read()
            repo.create_file(file_path_2, "Committing SQLite file", content, branch="main")
            print(f"SQLite file successfully committed to {repo_name}/{file_path}")
def main():
    x= Data_Pipeline()
    x.Create_Connection()
    x.Create_Data_In_Github()

if __name__ == "__main__":
    main()
