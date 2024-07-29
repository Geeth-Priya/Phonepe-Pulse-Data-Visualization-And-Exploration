# Phonepe-Pulse-Data-Visualization-And-Exploration
## ****Domain**: Fintech**
### **Introduction**
  PhonePe Pulse Data Visualization and Exploration project is designed to transform and visualize data from the PhonePe Pulse GitHub repository in an engaging and intuitive way.It provides insights into various metrics and statistics related to PhonePe's transactions,insurance and user data.
### Table of Contents
* Technologies Used
* Installation
* Import Libraries from Modules
* Usage
* Features
#### Technologies Used:
* Python
* Pandas
* MySQL
* Streamlit
* Plotly
* GitHub 
#### Installation
* pip install pandas
* pip install mysql-connector-python
* pip install stramlit
* pip install plotly
##### Import Libraries from Modules
* import streamlit as st
* from streamlit_option_menu import option_menu

* import os 
* import json
* import pandas as pd
* import mysql.connector
* import requests

* import plotly.express as px
* from PIL import Image
* from streamlit_lottie import st_lottie
* import base64
#### Usage
Steps to be followed for effectively using the application:
1. Access the Streamlit App in the web browser
2. Select the Analysis method from the Navigation Menu such as Aggregated Analysis,Map Analysis, Insurance Analysis, Data Analysis.
3. After selecting the Analysis method and apply filters such as year,quarter and state.
4. Now the Streamlit will display the details using visualizations.
5. Use the input box provided to select from a variety of queries for data analysis.
6. By selecting the Query, streamlit will process the request and display the answers.
#### Features
- **Data Extraction:** Data is fetched from the PhonePe Pulse GitHub respository and cloned into the local environment.
- **Data Transformation:** Clean and Structure the data using Python and Pandas.
- **Database Insertion:** Storing the cleaned and structured data in MySQL Database.
- **Dashboard Creation:** Create an interactive dashboard using Streamlit and Plotly
- **Data Retrieval:** Retrieve data from the MySQL database to ensure the dashboard updates dynamically.
