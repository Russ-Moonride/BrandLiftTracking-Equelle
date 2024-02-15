import streamlit as st
import pandas as pd
import pandas_gbq
import pandas 
import os
import matplotlib.pyplot as plt
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
correct_hashed_password = "CFEquelleBL1940$"

st.set_page_config(page_title= f"{Account} Creative Ad Testing Dash",page_icon="🧑‍🚀",layout = 'wide')

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

# Function to color code the percentage
def color_code(val):
    color = 'red' if float(val.strip('%')) > 0 else 'green'
    return f'<p style="color:{color};">{val}</p>'

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

  col1, col2, col3, _  = st.columns(4)
          
  with col1:
    st.write("Select Date Range for Period 1")
    start_date_1 = st.date_input("Start date", value=two_weeks_ago, key='start1')
    end_date_1 = st.date_input("End date", value=one_week_ago, key='end1')

  with col2:
    st.write("Select Date Range for Period 2")
    start_date_2 = st.date_input("Start date", value=one_week_ago, key='start2')
    end_date_2 = st.date_input("End date", value=datetime.now(), key='end2')
  
  with col3:
    for i in range(10):
              st.write("")
    metric = st.selectbox(
    'Select Metric for Bar Chart',
    ('Impressions', 'Clicks', 'Cost', 'Conversions', 'Revenue'))
    

  #Creating two groups for bar chart
  start_date_1 = pd.Timestamp(start_date_1)
  end_date_1 = pd.Timestamp(end_date_1)
  start_date_2 = pd.Timestamp(start_date_2)
  end_date_2 = pd.Timestamp(end_date_2)

  full_data['Date'] = pd.to_datetime(full_data['Date'])
  data_copy = full_data
  
  # Create Groups for viz
  data_copy['range'] = pd.cut(data_copy['Date'], bins=[start_date_1, end_date_1, end_date_2], labels=['Range 1', 'Range 2'])
  
  data_copy = data_copy[((data_copy['Date'] >= start_date_1) & (data_copy['Date'] <= end_date_1)) | 
                 ((data_copy['Date'] >= start_date_2) & (data_copy['Date'] <= end_date_2))]

          
  # Aggregate data by day and range
  daily_data = data_copy.groupby([data_copy['Date'].dt.date, 'range'])[metric].sum().unstack(fill_value=0)
      
  # Plot
  fig, ax = plt.subplots(figsize=(10, 6))
  daily_data.plot(kind='bar', ax=ax, color=['blue', 'orange'])
  ax.set_title(f'Sum of {metric} Period Comparison')
  ax.set_xlabel('Date')
  ax.set_ylabel('Value')
  ax.legend(title='Date Range')
 
  # Filtering the dataset for the selected date ranges
  filtered_df1 = full_data[(full_data['Date'] >= start_date_1) & (full_data['Date'] <= end_date_1)]
  agg_data1 = filtered_df1.select_dtypes(include='number').sum().to_frame('Period 1      ').T

  #Add calc cols
  agg_data1['CPC'] = agg_data1['Cost']/agg_data1['Clicks']
  agg_data1['CTR'] = agg_data1['Clicks']/agg_data1['Impressions']
  agg_data1['CVR'] = agg_data1['Conversions']/agg_data1['Clicks']
  agg_data1['CAC'] = agg_data1['Cost']/agg_data1['Conversions']

  filtered_df2 = full_data[(full_data['Date'] >= start_date_2) & (full_data['Date'] <= end_date_2)]
  agg_data2 = filtered_df2.select_dtypes(include='number').sum().to_frame('Period 2      ').T

  #Addcacl cols
  agg_data2['CPC'] = agg_data2['Cost']/agg_data2['Clicks']
  agg_data2['CTR'] = agg_data2['Clicks']/agg_data2['Impressions']
  agg_data2['CVR'] = agg_data2['Conversions']/agg_data2['Clicks']
  agg_data2['CAC'] = agg_data2['Cost']/agg_data2['Conversions']  

  #Creating diff df
  sum_df1 = agg_data1
  sum_df2 = agg_data2
          
  sum_df1 = sum_df1.reset_index(drop=True)
  sum_df2 = sum_df2.reset_index(drop=True)
  percentage_diff = ((sum_df1 - sum_df2) / sum_df1) * 100
          
  #Format agg_data1 correctly
  agg_data1['Impressions'] = agg_data1['Impressions'].map(lambda x: "{:,}".format(int(x))) 
  agg_data1['Clicks'] = agg_data1['Clicks'].map(lambda x: "{:,}".format(int(x)))   
  agg_data1['Cost'] = agg_data1['Cost'].map(lambda x: "{:,}".format(int(x)))
  agg_data1['Conversions'] = agg_data1['Conversions'].map(lambda x: "{:,}".format(int(x)))
  agg_data1['Revenue'] = agg_data1['Revenue'].map(lambda x: "{:,}".format(int(x)))
          
  agg_data1['Cost'] = agg_data1['Cost'].apply(lambda x: f"${x}")
  agg_data1['Revenue'] = agg_data1['Revenue'].apply(lambda x: f"${x}")
          
  agg_data1['CPC'] = round(agg_data1['CPC'], 2).astype(int)
  agg_data1['CPC'] = agg_data1['CPC'].apply(lambda x: '' if abs(x) > 10000 else f"${x}")
          
  agg_data1['CAC'] = round(agg_data1['CAC'], 2)
  agg_data1['CAC'] = agg_data1['CAC'].apply(lambda x: f"${x}")
          
  agg_data1['CTR'] = agg_data1['CTR'].apply(lambda x: f"{x*100:.2f}%")
  agg_data1['CVR'] = agg_data1['CVR'].apply(lambda x: f"{x*100:.2f}%")   

  #Format agg_data1 correctly
  agg_data2['Impressions'] = agg_data2['Impressions'].map(lambda x: "{:,}".format(int(x))) 
  agg_data2['Clicks'] = agg_data2['Clicks'].map(lambda x: "{:,}".format(int(x)))   
  agg_data2['Cost'] = agg_data2['Cost'].map(lambda x: "{:,}".format(int(x)))
  agg_data2['Conversions'] = agg_data2['Conversions'].map(lambda x: "{:,}".format(int(x)))
  agg_data2['Revenue'] = agg_data2['Revenue'].map(lambda x: "{:,}".format(int(x)))
          
  agg_data2['Cost'] = agg_data2['Cost'].apply(lambda x: f"${x}")
  agg_data2['Revenue'] = agg_data2['Revenue'].apply(lambda x: f"${x}")
          
  agg_data2['CPC'] = round(agg_data2['CPC'], 2).astype(int)
  agg_data2['CPC'] = agg_data2['CPC'].apply(lambda x: '' if abs(x) > 10000 else f"${x}")
          
  agg_data2['CAC'] = round(agg_data2['CAC'], 2)
  agg_data2['CAC'] = agg_data2['CAC'].apply(lambda x: f"${x}")
          
  agg_data2['CTR'] = agg_data2['CTR'].apply(lambda x: f"{x*100:.2f}%")
  agg_data2['CVR'] = agg_data2['CVR'].apply(lambda x: f"{x*100:.2f}%")    

  #format diff df
  percentage_diff = percentage_diff.applymap(lambda x: f"{x:.2f}%")

  df_styled = percentage_diff.T.applymap(color_code)
  combined_df = pd.concat([agg_data1.T, agg_data2.T, df_styled], axis=1)
  combined_df.columns.values[-1] = "Percent Difference"
  html = combined_df.to_html(escape=False)
          
  # Custom CSS to inject width and possibly overflow handling
  custom_css = """
  <style>
      table { 
          width: 100%; 
          border-collapse: collapse; 
      } 
      th, td { 
          text-align: left; 
          padding: 8px; 
      } 
      tr:nth-child(even) {background-color: #f2f2f2;}
  </style>
  """

  # Combine custom CSS with DataFrame HTML
  html_with_css = custom_css + html

  col1, col2 = st.columns(2)
          
  with col1:        
    st.markdown(html_with_css, unsafe_allow_html=True)

  with col2:
    st.pyplot(fig)

if __name__ == '__main__':
    password_protection()
