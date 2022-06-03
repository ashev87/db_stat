# %%
import pandas as pd
import streamlit as st
import psycopg2
import sys
import numpy as np
# get version of library
# print(psycopg2.__version__)
# 2.9.3
# %%
# Initialize connection.
# Uses st.experimental_singleton to only run once.
#@st.experimental_singleton
def connect(secrets):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**secrets)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

# %%
# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
#@st.experimental_memo(ttl=60)
conn = connect(st.secrets["postgres"])
query = """
        SELECT *
        FROM statistics.get_status_stat
        WHERE created_at >= '2022-06-1 00:00:00';
        """
query1 = """
        SELECT tablename, schemaname, tableowner
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog'
        AND schemaname != 'information_schema'
        ORDER BY tablename ASC;
        """
df = pd.read_sql(query, conn)
df = df.sort_values(by=['created_at'], ascending=False)
# %%
# st.write("Errors")
# errors = df.error.value_counts()
# st.write(errors)
# 
# st.write("Service Errors")
# service_error = df.service_error.value_counts()
# st.write(service_error)
# %%
df["error1"] = df["error"].combine_first(df["service_error"])
df['error_combined'] = np.where(df['error1'].str.contains('arrow-left', na=False), 'arrow-left', df['error1'])
df['error_combined'] = np.where(df['error1'].str.contains('timeout', na=False), 'sms timeout', df['error_combined'])
df['error_combined'] = np.where(df['error1'].str.contains('Rahmenvertragskunden', na=False), 'Rahmenvertragskunden', df['error_combined'])
df['error_combined'] = np.where(df['error1'].str.contains('Kennwort ', na=False), 'Kennwort ', df['error_combined'])
df['error_combined'] = np.where(df['error1'].str.contains('Der Kontostatus des Kunden ist nicht in Ordnung', na=False), 'Kontostatus nicht in Ordnung', df['error_combined'])
df['status'] = df['status'].replace('', np.NaN)
dates = df.created_at.dt.date.unique()
filter_date = st.selectbox("Select Date", dates)
mask = (df['created_at'].dt.date == filter_date)
today = df[mask]
col1, col2 = st.columns(2)
with col1:
    st.write("Combined Errors")
    st.dataframe(today['error_combined'].value_counts())
with col2:
    st.metric(label="Success", value=today[today['status'].notna()].shape[0])
    st.metric(label="Failed", value=today[today['error'].isna() & today['service_error'].isna() & today['status'].isna()].shape[0])

# st.write("Errors")
# st.write(df['error'].value_counts())
# st.write("Service Errors")
# st.write(df['service_error'].value_counts())

# %%
# get number of arrow left errors
# arrow_left_errors = df[df['service_error'].str.contains("Der Kontostatus des Kunden ist nicht in Ordnung", na=False)]
# %%