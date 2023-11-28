import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Table, MetaData, Integer, BIGINT, TEXT, FLOAT, String

df=pd.read_csv('https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV',delimiter=';')
# Dropping the Status
df=df.drop(columns='Status')
# Valid "Verkehr" values are "FV", "RV", "nur DPN"
df=df[df['Verkehr'].isin(['FV','RV','nur DPN'])]
# Valid "Laenge", "Breite" values are geographic coordinate system values between and including -90 and 90
df['Laenge'] = pd.to_numeric(df['Laenge'].str.replace(',', '.'), errors='coerce')
df['Breite'] = pd.to_numeric(df['Breite'].str.replace(',', '.'), errors='coerce')
df = df[(df['Laenge'] >= -90) & (df['Laenge'] <= 90)]
#Converting to Numeric
df['Betreiber_Nr'] = pd.to_numeric(df['Betreiber_Nr'], errors='coerce')
df = df[(df['Breite'] >= -90) & (df['Breite'] <= 90)]
#Valid "IFOPT" values following pattern
pattern = r'^[a-zA-Z]{2}:\d+:\d+(?::\d+)?$'
df = df[df['IFOPT'].str.contains(pattern, na=False)]
df = df.dropna()
engine = create_engine('sqlite:///trainstops.sqlite', echo=True)
meta = MetaData()
trainstops = Table(
    'trainstops', meta,
    Column('EVA_NR', BIGINT),
    Column('TDSA', TEXT),
    Column('IFOPT', TEXT),
    Column('NAME', TEXT),
    Column('Verkehr', TEXT),
    Column('Laenge', FLOAT),
    Column('Breite', FLOAT),
    Column('Betreiber_Name', TEXT),
    Column('Betreiber_Nr', FLOAT)
)
meta.create_all(engine)
conn = engine.connect()
df.to_sql('trainstops', con=conn, if_exists='replace', index=False)
conn.close()
