import streamlit as st
import pandas as pd
import pandas_gbq
import pandas 
import os
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from scipy.stats import chi2_contingency
from PIL import Image
from git import Repo
import base64
import requests
import json
from google.cloud import storage

credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
client = bigquery.Client(credentials=credentials)

Account = "Equelle"
correct_hashed_password = "Equelle1234"

st.set_page_config(page_title= f"{Account} Creative Ad Testing Dash",page_icon="🧑‍🚀",layout="wide")

def initialize_storage_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    storage_client = storage.Client(credentials=credentials)
    return storage_client

# Use this client for GCS operations
storage_client = initialize_storage_client()

def password_protection():
  if 'authenticated' not in st.session_state:
      st.session_state.authenticated = False
      
  if not st.session_state.authenticated:
      password = st.text_input("Enter Password:", type="password")
      
      if st.button("Login"):
          if password == correct_hashed_password:
              st.session_state.authenticated = True
              main_dashboard()
          else:
              st.error("Incorrect Password. Please try again or contact the administrator.")
  else:
      main_dashboard()

def main_dashboard():
  st.markdown(f"<h1 style='text-align: center;'>{Account} Brand Lift Tracking</h1>", unsafe_allow_html=True)

  # Calculate the date one year ago from today
  one_year_ago = (datetime.now() - timedelta(days=365)).date()
  two_weeks_ago = (datetime.now() - timedelta(days=14)).date()
  one_week_ago = (datetime.now() - timedelta(days=7)).date() 

  if 'full_data' not in st.session_state:
      credentials = service_account.Credentials.from_service_account_info(
          st.secrets["gcp_service_account"]
      )
      client = bigquery.Client(credentials=credentials)
      # Modify the query
      query = f"""
      SELECT * FROM `equelle.Equelle_Segments.Platform_Raw` 
      WHERE Date BETWEEN '{one_year_ago}' AND CURRENT_DATE() """
      
      st.session_state.full_data = pandas.read_gbq(query, credentials=credentials)

  full_data = st.session_state.full_data

  col1, col2, _ = st.columns(3)
          
  with col1:
    st.write("Select Date Range for Period 1")
    start_date_1 = st.date_input("Start date", value=two_weeks_ago, key='start1')
    end_date_1 = st.date_input("End date", value=one_week_ago, key='end1')

  with col2:
    st.write("Select Date Range for Period 2")
    start_date_2 = st.date_input("Start date", value=one_week_ago, key='start2')
    end_date_2 = st.date_input("End date", value=datetime.now(), key='end2')

  # Filtering the dataset for the selected date ranges
  filtered_df1 = full_data[(full_data['Date'] >= start_date_1) & (full_data['Date'] <= end_date_1)]
  agg_data1 = filtered_df1.select_dtypes(include='number').sum().to_frame('Period 1 Sum')
  
          
  filtered_df2 = full_data[(full_data['Date'] >= start_date_2) & (full_data['Date'] <= end_date_2)]

  # Displaying the filtered dataframes
  col1, col2, _ = st.columns(3)

  with col1:
    st.write("Data for Period 1")
    st.dataframe(agg_data1)

  with col2:
    st.write("Data for Period 2")
    st.dataframe(filtered_df2)

if __name__ == '__main__':
    password_protection()
