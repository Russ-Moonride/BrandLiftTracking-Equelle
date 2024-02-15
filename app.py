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

st.set_page_config(page_title= f"{Account} Creative Ad Testing Dash",page_icon="🧑‍🚀")

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

  col1, col2, _, _  = st.columns(4)
          
  with col1:
    st.write("Select Date Range for Period 1")
    start_date_1 = st.date_input("Start date", value=two_weeks_ago, key='start1')
    end_date_1 = st.date_input("End date", value=one_week_ago, key='end1')

  with col2:
    st.write("Select Date Range for Period 2")
    start_date_2 = st.date_input("Start date", value=one_week_ago, key='start2')
    end_date_2 = st.date_input("End date", value=datetime.now(), key='end2')

  #Creating two groups for bar chart
  
  # Filter data by date range
  full_data['range'] = pd.cut(full_data['Date'], bins=[start_date_1, end_date_1, end_date_2], labels=['Range 1', 'Range 2'])

  # Aggregate data by day and range
  daily_data = full_data.groupby([full_data['date'].dt.date, 'range'])['value'].sum().unstack(fill_value=0)

  # Plot
  fig, ax = plt.subplots(figsize=(10, 6))
  daily_data.plot(kind='bar', ax=ax, color=['blue', 'orange'])
  ax.set_title('Daily Data Comparison')
  ax.set_xlabel('Date')
  ax.set_ylabel('Value')
  ax.legend(title='Date Range')
 
  # Filtering the dataset for the selected date ranges
  filtered_df1 = full_data[(full_data['Date'] >= start_date_1) & (full_data['Date'] <= end_date_1)]
  agg_data1 = filtered_df1.select_dtypes(include='number').sum().to_frame('Period 1      ').T

  #Add calc cols
  agg_data1['CPC'] = agg_data1['Cost']/agg_data1['Clicks']
  agg_data1['CPM'] = (agg_data1['Cost']/agg_data1['Impressions'])*1000
  agg_data1['CTR'] = agg_data1['Clicks']/agg_data1['Impressions']
  agg_data1['CVR'] = agg_data1['Conversions']/agg_data1['Clicks']
  agg_data1['CPA'] = agg_data1['Cost']/agg_data1['Conversions']

  filtered_df2 = full_data[(full_data['Date'] >= start_date_2) & (full_data['Date'] <= end_date_2)]
  agg_data2 = filtered_df2.select_dtypes(include='number').sum().to_frame('Period 2      ').T

  #Addcacl cols
  agg_data2['CPC'] = agg_data2['Cost']/agg_data2['Clicks']
  agg_data2['CPM'] = (agg_data2['Cost']/agg_data2['Impressions'])*1000
  agg_data2['CTR'] = agg_data2['Clicks']/agg_data2['Impressions']
  agg_data2['CVR'] = agg_data2['Conversions']/agg_data2['Clicks']
  agg_data2['CPA'] = agg_data2['Cost']/agg_data2['Conversions']  

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
          
  agg_data1['CPC'] = round(agg_data1['CPC'], 0).astype(int)
  agg_data1['CPC'] = agg_data1['CPC'].apply(lambda x: '' if abs(x) > 10000 else f"${x}")
          
  agg_data1['CPA'] = round(agg_data1['CPA'], 2)
  agg_data1['CPA'] = agg_data1['CPA'].apply(lambda x: f"${x}")
          
  agg_data1['CPM'] = round(agg_data1['CPM'], 0).astype(int)
  agg_data1['CPM'] = agg_data1['CPM'].apply(lambda x: f"${x}")
          
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
          
  agg_data2['CPC'] = round(agg_data2['CPC'], 0).astype(int)
  agg_data2['CPC'] = agg_data2['CPC'].apply(lambda x: '' if abs(x) > 10000 else f"${x}")
          
  agg_data2['CPA'] = round(agg_data2['CPA'], 2)
  agg_data2['CPA'] = agg_data2['CPA'].apply(lambda x: f"${x}")
          
  agg_data2['CPM'] = round(agg_data2['CPM'], 0).astype(int)
  agg_data2['CPM'] = agg_data2['CPM'].apply(lambda x: f"${x}")
          
  agg_data2['CTR'] = agg_data2['CTR'].apply(lambda x: f"{x*100:.2f}%")
  agg_data2['CVR'] = agg_data2['CVR'].apply(lambda x: f"{x*100:.2f}%")    

  #format diff df
  percentage_diff = percentage_diff.applymap(lambda x: f"{x:.2f}%")

  col1, col2 = st.columns(2)
          
  with col1:        
    df_styled = percentage_diff.T.applymap(color_code)
    combined_df = pd.concat([agg_data1.T, agg_data2.T, df_styled], axis=1)
    combined_df.columns.values[-1] = "Percent Difference"
    html = combined_df.to_html(escape=False)
    st.markdown(html, unsafe_allow_html=True)

  with col2:
    st.pyplot(fig)

if __name__ == '__main__':
    password_protection()
