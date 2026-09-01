from turtle import color
import streamlit as st
import pandas as pd
from PIL import Image
import awswrangler as wr
import numpy as np
import plotly.express as px


# copy data from S3 bucket to a pandas dataframe using awswrangler
# Define your S3 bucket and file key
bucket_name = "global-partners-oseikwadwo"
key_clv = "gold/customer_lifetime_value/run-1787777471574-part-r-00000"
key_rfm = "gold/customer-RFM-Metrics/run-1787847137955-part-r-00000"
key_churn = "gold/customer-churn-indicators/run-1787856505516-part-r-00000"
key_weekly = "gold/customer-weekly-rev/run-1787862938026-part-r-00000"
key_monthly = "gold/customer-monthly-rev/run-1787864594930-part-r-00000"
key_location = "gold/weekday-top-performing-locations/run-1787928234565-part-r-00000"

# Construct the S3 URI
s3_uri_clv = f"s3://{bucket_name}/{key_clv}"
s3_uri_rfm = f"s3://{bucket_name}/{key_rfm}"
s3_uri_churn = f"s3://{bucket_name}/{key_churn}"
s3_uri_weekly = f"s3://{bucket_name}/{key_weekly}"
s3_uri_monthly = f"s3://{bucket_name}/{key_monthly}"
s3_uri_location = f"s3://{bucket_name}/{key_location}"

# Read the CSV file from S3 into a pandas DataFrame
df_clv = wr.s3.read_csv(s3_uri_clv)

df_rfm = wr.s3.read_csv(s3_uri_rfm)
df_rfm = df_rfm.sort_values(by=['recency_days', 'frequency_count', 'monetary_value'], ascending=[True, False, False])
df_rfm = df_rfm.head(100)  # Limit to the first 100 rows for display purposes

df_churn = wr.s3.read_csv(s3_uri_churn)
df_churn.dropna(inplace=True)
df_churn = df_churn.head(100)  # Limit to the first 100 rows for display purposes

df_weekly = wr.s3.read_csv(s3_uri_weekly)
df_weekly = df_weekly.head(100)  # Limit to the first 100 rows for display purposes

df_monthly = wr.s3.read_csv(s3_uri_monthly)
df_monthly = df_monthly.head(100)  # Limit to the first 100 rows for display purposes

df_location = wr.s3.read_csv(s3_uri_location)
df_location = df_location.groupby('RESTAURANT_ID')['total_rev_per_day'].sum().reset_index(name='total_rev_per_restaurant')
df_location = df_location.head(100)  # Limit to the first 100 rows for display purposes 

# print(df_location.head())




# st.title("Customer Lifetime Value (CLV) Analysis")
# st.bar_chart(df_clv['CLV_Category'].value_counts(), use_container_width=True)


# st.title("RFM Analysis")
# st.bar_chart(df_rfm, x='user_id', y=['recency_days'], use_container_width=True)
# st.bar_chart(df_rfm, x='user_id', y=['frequency_count'], use_container_width=True)
# st.bar_chart(df_rfm, x='user_id', y=['monetary_value'], use_container_width=True)
# st.line_chart(df_rfm, x='user_id', y=['recency_days', 'frequency_count', 'monetary_value'])
# fig = px.bar(df_rfm, x='user_id', y=['recency_days', 'frequency_count', 'monetary_value'], barmode='group', title="RFM Analysis")
# st.plotly_chart(fig)


# st.title("Customer Churn Analysis")
# st.subheader("average order interval")
# st.bar_chart(df_churn, x='user_id', y=['avg_gap_days'], use_container_width=True)

# st.subheader("days since last order")
# st.line_chart(df_churn, x='user_id', y=['days_since_last_order', 'inactivity_tag'], use_container_width=True)
# fig1 = px.line(df_churn, x='user_id', y=['days_since_last_order'], color='inactivity_tag', title="Days Since Last Order and Inactivity Tag")
# fig2 = px.line(df_churn, x='user_id', y=['pct_change_spend'], color='inactivity_tag', title="percentage change in spend and inactivity tag")
# st.plotly_chart(fig1)
# st.plotly_chart(fig2)


# st.title("Customer Weekly Revenue Analysis")
# st.subheader("Weekly Revenue")
# fig1 = px.bar(df_weekly, x='week', y=['weekly_rev'], color='ITEM_CATEGORY', title="Weekly Revenue")
# st.plotly_chart(fig1)

# st.title("Customer Monthly Revenue Analysis")
# st.subheader("Monthly Revenue")
# fig2 = px.bar(df_monthly, x='month', y=['monthly_rev'], color='ITEM_CATEGORY', title="Monthly Revenue")
# st.plotly_chart(fig2)

st.title("Top Performing Locations Analysis")
st.subheader("Top Performing Locations")    
st.bar_chart(df_location, x='RESTAURANT_ID', y=['total_rev_per_restaurant'], use_container_width=True)