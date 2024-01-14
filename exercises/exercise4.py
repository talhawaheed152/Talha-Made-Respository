import urllib.request
import pandas as pd
import zipfile
import os

# Download and unzip data
zip_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
zip_file_name = "mowesta-dataset.zip"
extracted_dir = "mowesta-dataset"
urllib.request.urlretrieve(zip_url, zip_file_name)
if os.path.exists(zip_file_name):
    if not os.path.exists(extracted_dir):
        os.makedirs(extracted_dir)
    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
        zip_ref.extractall(extracted_dir)
    csv_file_path = os.path.join(extracted_dir, "data.csv")
print("Downloaded and unziped data...")
    
# Reshaping Data
ex4_df = pd.read_csv(csv_file_path, sep=";", decimal=",", index_col=False,usecols=["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)","Batterietemperatur in °C", "Geraet aktiv"])
ex4_df.rename(columns={"Temperatur in °C (DWD)":"Temperatur","Batterietemperatur in °C":"Batterietemperatur"}, inplace=True)
ex4_df=ex4_df.loc[:, :'Geraet aktiv']
print("Reshaped data...")


# Transform Data 
temperature_columns = ['Temperatur', 'Batterietemperatur']
ex4_df[temperature_columns] = (ex4_df[temperature_columns] * 9/5) + 32
print("Transformed data...")


# Validation
ex4_df= ex4_df[ex4_df['Geraet'] > 0][ex4_df['Hersteller'].astype(str).str.strip() != ""][ex4_df['Model'].astype(str).str.strip() != ""][ex4_df['Monat'].between(1, 12)][pd.to_numeric(ex4_df['Temperatur'], errors='coerce').notnull()][pd.to_numeric(ex4_df['Batterietemperatur'], errors='coerce').notnull()][ex4_df['Geraet aktiv'].isin(['Ja', 'Nein'])]
print("Validated data...")


#Conversion to DB
conn = db.connect("temperatures.sqlite")
cursor = conn.cursor()
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS temperatures (
    Geraet BIGINT,
    Hersteller TEXT,
    Model TEXT,
    Monat TEXT,
    Temperatur FLOAT,
    Batterietemperatur FLOAT,
    Geraet_aktiv TEXT)"""
cursor.execute(create_table_query)
dataFrame.to_sql('temperatures', conn, if_exists='replace', index=False)
conn.commit()
conn.close()
print("Created DataBase...")


print("EXERCISE 4 COMPLETED")
#Code by M.Talha