

# Youtube Data Harvesting and Warehousing

YouTube Data Harvesting and Warehousing is a project that aims to create a user-friendly Streamlit application that uses the Google API to extract useful information from YouTube channels. This application helps us to understand the channels’ performance better by analyzing their queries.






## Goal

The goal of the project is to setup ETL pipeline by python scripting which extract data from youtubeAPI and store it in MongoDB database as unstructured data then migrating it to MySQL database as structured.

## Installation

Packages to be installed to run this project.
```bash
  pip install google-api-python-client
```
```bash
  pip install pymongo
```
```bash
  pip install mysql.connector
```
```bash
  pip install pandas
```
```bash
  pip install streamlit
```

## Guide

Importing the packages installed

```bash
import pymongo
import pandas as pd
import streamlit as st
import mysql.connector
from googleapiclient.discovery import build
```

## Roadmap

- Extracting Data - The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.


```bash
api_key = 'AIzaSyBGy_dk6_7Uwa1aZxmQuyn2lZc4RxeDJ4M'
youtube = build('youtube', 'v3', developerKey=api_key)
```
- MongoDB - The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.
```bash
api_key = client = pymongo.MongoClient("mongodb+srv://Hemachandar:hema1234@cluster0.8rfch9i.mongodb.net/?retryWrites=true&w=majority")
db = client.Youtube_Data
```

- MySQL - The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.
```bash
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database='youtube_data'  
)
mycursor = mydb.cursor(buffered=True)
```

## Screenshots

This is the web app's preview
![App_preview](https://github.com/HemachandarAravamuthan/Youtube_Data_harvesting_and_warehousing/assets/141393571/51e98bd6-dd73-4a1e-ad6d-cec94e97c53b)

Operations can be done with this app
![Options](https://github.com/HemachandarAravamuthan/Youtube_Data_harvesting_and_warehousing/assets/141393571/6b8cb4a0-c98c-44c3-9dfc-c99e32f1644a)

Migrating data to MySQL database
![SQL migration](https://github.com/HemachandarAravamuthan/Youtube_Data_harvesting_and_warehousing/assets/141393571/c2911a61-b773-4a9e-a4f5-1c3641510ae8)

Analysis based on 10 queries
![Analysis Query](https://github.com/HemachandarAravamuthan/Youtube_Data_harvesting_and_warehousing/assets/141393571/df8b5412-3859-4568-bd26-b5aa0db99a7b)


## Appendix

Sentimental analysis of the video or channel can be done with this project as the commennt data and views data are already available.


## Contact

email : hemachandar11@gmail.com
Linkedin : https://www.linkedin.com/in/hemachandar-aravamuthan-1594b1194/
