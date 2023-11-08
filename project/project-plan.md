# Project Plan

## Title
Air Traffic Analysis in Top US Cities 

## Main Question

Can we plan better decision-making, policy development, and urban planning from public datasets?

## Description


In this project we will analyze the air traffic in the top US cities. We will use multiple datasets for this which are of public domain. Analyzing air traffic data for the top US cities can help us provide valuable insights and help solve several issues and various questions related to air travel and urban planning. Some of the key issues we look forward to answer are:
###	Traffic Congestion and Delays
-	What are the major factors contributing to flight delays and congestion in these cities?
-	Are there specific routes or time periods with consistently higher delays?
###	Demands and Trends
-	What are the trends in air travel demand for these cities? Are there specific times of the year when demand surges?
-	Are there routes that are growing or declining in popularity?
###	Urban Planning and Transportation
-	How does air traffic affect the overall transportation network within the cities?
-	How can cities improve connectivity between airports and other modes of transportation?
Therefore, By analyzing air traffic data in top US cities, we can provide valuable insights to inform decision-making, policy development, and urban planning.



## Datasources

### Datasource #1: Air Flight Dataset
* Metadata URL: https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2020.parquet
* Data URL: https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr
* Data Type: CSV

This dataset contains all flight information including cancellation and delays by airline for dates back to January 2018.The data contained in the compressed file has been extracted from the Marketing Carrier On-Time Performance (Beginning January 2018) data table of the "On-Time" database from the TranStats data library. The time period is indicated in the name of the compressed file; for example, XXX_XXXXX_2001_1 contains data of the first month of the year 2001.

### Datasource #2: Top 100 US Cities by Population

* Metadata URL: https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population
* Data URL: https://www.kaggle.com/datasets/brandonconrady/top-100-us-cities-by-population
* Data Type: CSV

Data was pulled from a table in the following Wikipedia article: https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population.Microsoft Excel's PowerQuery function was used ti pull the table from Wikipedia.
Lists each city, its rank (based on 2020 population), some data on its area, and population in both 2020 and 2010.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Data Collection and Understanding [#1][i1]
2. Data Cleaning and Feature Engineering
3. Data modeling and ETL (data warehouse)
4. Data Analysis
5. Conclusion

[i1]: https://github.com/jvalue/made-template/issues/1