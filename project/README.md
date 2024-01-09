# Important Information

## Title
Analysis of Domestic Flights to USA Cities

## Flight dataset update
The Flight dataset consists of 4 GB of storage and takes alot of time to donwload. The way round this problem is that we download the required _Combined_Flights_2020.parquet_ from kaggle which is of only 183.13 MB. to do that follow the following step:

__1-__ go to this link: https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2020.parquet

__2-__ Scroll down and click the download button to download the Combined_Flights_2020.parquet file but instead of downloading it, go to the downloads manager of your browser, and cancel it from downloading.

__3-__ right click the canceled download and copy the link address and past it at line 157 of the pipeline.py

*The link is not consatant, therefore this method has to be applied for the code to run the pipeline*