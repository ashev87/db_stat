# %%
import pandas as pd
import streamlit as st
import psycopg2
import sys
import numpy as np
from datetime import datetime, timedelta
import time
import plotly.express as px

st.set_page_config(layout="wide")
# get version of library
# print(psycopg2.__version__)
# 2.9.3
# %%
# Initialize connection.
# Uses st.experimental_singleton to only run once.
# @st.experimental_singleton
@st.cache(allow_output_mutation=True)
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
# Uses st.experimental_memo to only rerun when the query changes or after 10 min. example for 10 min ttl=600
# @st.experimental_memo(ttl=600)
# def query_db(_conn, _query):
    # cursor = conn.cursor()
    # cursor.execute(query)
    # rows_raw = cursor.fetchall()
    # cursor.close()
    # conn.close()
    # Convert to list of dicts. Required for st.experimental_memo to hash the return value.
    # rows = [dict(row) for row in rows_raw]
    # return pd.DataFrame(rows)
# cache for 10 minutes
@st.cache(ttl=60*10, hash_funcs={psycopg2.extensions.connection: id})
def load_from_db(query):
    with st.spinner('Loading Data...'):
        time.sleep(0.5)
        df = pd.read_sql_query(query, conn)
    return df
conn = connect(st.secrets["postgres"])
query = """
        SELECT *
        FROM statistics.get_status_stat
        WHERE created_at >= '2022-05-1 00:00:00';
        """
query1 = """
        SELECT tablename, schemaname, tableowner
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog'
        AND schemaname != 'information_schema'
        ORDER BY tablename ASC;
        """
df = load_from_db(query)
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
df['error_combined'] = np.where(df['error1'].str.contains('Kennwort', na=False), 'Kennwort', df['error_combined'])
df['error_combined'] = np.where(df['error1'].str.contains("Problem beim Speichern eines Auftrags", na=False), "Problem beim Speichern eines Auftrags", df['error_combined'])
df['error_combined'] = np.where(df['error1'].str.contains('Der Kontostatus des Kunden ist nicht in Ordnung', na=False), 'Kontostatus nicht in Ordnung', df['error_combined'])
df['status'] = df['status'].replace('', np.NaN)
df['Date'] = df['created_at'].dt.date
dates = df.created_at.dt.date.unique()
max_date = dates.max()
try:
    min_date = max_date - timedelta(days=7)
except:
    min_date = dates.min()

st.title("Status Statistic")
a_date = st.date_input("Pick a date", (min_date, max_date))
st.write("The date selected:", a_date)
# filter_date = st.selectbox("Select Date", dates)
# mask = (df['created_at'].dt.date == filter_date)
mask = (df['created_at'].dt.date >= a_date[0]) & (df['created_at'].dt.date <= a_date[1])
today = df[mask]
col1, col2 = st.columns(2)
with col1:
    st.write("Combined Errors")
    st.table(today['error_combined'].value_counts())
with col2:
    st.metric(label="Success", value=today[today['status'].notna()].shape[0])
    st.metric(label="Failed", value=today[today['error'].isna() & today['service_error'].isna() & today['status'].isna()].shape[0])

# %% plot a chart
plot_data = today.groupby(['Date', 'error_combined']).size().unstack(fill_value=0)
# bar plot
# fig = px.bar(plot_data, x=plot_data.index, y=plot_data.columns, color='error_combined', barmode='group')
# line plot
fig = px.line(plot_data, x=plot_data.index, y=plot_data.columns, color='error_combined')
#fig.update_layout(showlegend=False)
fig.update_layout(legend=dict(
    orientation="h",
))
st.plotly_chart(fig,use_container_width=True)
# %%

# st.write("Errors")
# st.write(df['error'].value_counts())
# st.write("Service Errors")
# st.write(df['service_error'].value_counts())

# %%
# get number of arrow left errors
# arrow_left_errors = df[df['service_error'].str.contains("Der Kontostatus des Kunden ist nicht in Ordnung", na=False)]
# %%