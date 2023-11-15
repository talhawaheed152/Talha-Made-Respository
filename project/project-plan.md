# Project Plan

## Title
Analysis of Domestic Flights to USA Cities   

## Main Question

How does the number of flights coming to a USA city depend on the type of city and what are the top cities and routes? 

## Description


This project is about analyzing data of flights between major USA cities, in order to see how attributes related to cities relate to number of flights coming to that city, and what are the top destinations and routes. There can be further analytics related to time of flights and network analysis. The project would also involve Data Engineering through ETL (Extract, Transform, Load), Data Modeling and Data Warehousing concepts. Two datasets will be used including a CSV of flights in 2020 and a Parquet file of Cities information from 2020. 

## Datasources

### Datasource #1: Air Flight Dataset
* Metadata URL: https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population
* Data URL: https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2020.parquet
* Data Type: Parquet
* License: CC0: Public Domain

This dataset contains all flight information including cancellation and delays by airline for dates back to January 2018.The data contained in the compressed file has been extracted from the Marketing Carrier On-Time Performance (Beginning January 2018) data table of the "On-Time" database from the TranStats data library. The time period is indicated in the name of the compressed file; for example, XXX_XXXXX_2001_1 contains data of the first month of the year 2001.

### Datasource #2: Top 100 US Cities by Population

* Metadata URL: https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population
* Data URL: https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population
* Data Type: CSV
* License: CC0: Public Domain

Data was pulled from a table in the following Wikipedia article: https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population.Microsoft Excel's PowerQuery function was used to pull the table from Wikipedia.
Lists each city, its rank (based on 2020 population), some data on its area, and population in both 2020 and 2010.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Data Collection and Understanding [#1][i1]
2. ETL, Data Cleaning and Data Wrangling
3. Data Modeling and Data Warehousing
4. Data Analysis and Data Visualization
5. Conclusion

[i1]: https://github.com/jvalue/made-template/issues/1