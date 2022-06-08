# %%
import contextlib
import time
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
#@st.cache(allow_output_mutation=True)
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

@contextlib.contextmanager
def profile(name):
    start_time = time.time()
    yield  # <-- your code will execute here
    total_time = time.time() - start_time
    print("%s: %.4f ms" % (name, total_time * 1000.0))
# %%
# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or af
# @st.experimental_memo(ttl=600)
# def query_db(_conn, _query):
    # cursor = conn.cursor()
    # cursor.execute(query)
    # rows_raw = cursor.fetchall()
    # cursor.close()
    # conn.close()
    # Convert to list of dicts. Required for st.experimental_memo to h
    # rows = [dict(row) for row in rows_raw]
    # return pd.DataFrame(rows)
# cache for 10 minutes
#@st.cache(ttl=6*10, hash_funcs={psycopg2.extensions.connection: id})
# def load_from_db(query, conn):
    # with st.spinner('Loading Data...'):
        # time.sleep(0.5)
        # df = pd.read_sql_query(sql=query, con=conn)
    # return df

def load_from_db(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    print(cursor.description)
    column_names = [i for i in cursor.description]
    cursor.close()
    df = pd.DataFrame(rows, columns=column_names) 
    return df

with profile("connect"):
    conn = connect(st.secrets["postgres"])

query = """
        SELECT client_phone, created_at, error, status, user_id
        FROM statistics.get_status_stat
        WHERE created_at >= '2022-06-1 00:00:00';
        """
query1 = """
        SELECT *
        from
        (
        select count(error), error from statistics.get_status_stat 
        where created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        and error not like '%arrow%'
        group by error
        union all
        select count(service_error), service_error from statistics.get_status_stat
        where created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        and service_error not like '%sms timeout%'
        and service_error not like '%Benutzer Id%'
        and service_error not like '%Der Kontostatus%'
        group by service_error
        union all
        select count(error), right(error, 16) as result from statistics.get_status_stat
        where error like '%arrow%'
        and created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        group by result
        union all
        select count(service_error), right(service_error, 11) as result from statistics.get_status_stat
        where service_error like '%sms%'
        and created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        group by result
        union all
        select count(service_error), left(service_error, 15) as result from statistics.get_status_stat
        where service_error like '%Benutzer%'
        and created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        group by result
        union all 
        select count(service_error), left(service_error, 15) as result from statistics.get_status_stat
        where service_error like '%Der Kontostatus%'
        and created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        group by result
        union all
        select count(*), error from statistics.get_status_stat
        where error is null
        and service_error  is null
        and status is null
        and created_at >= '2022-05-3 00:00:00'
        and created_at <  '2022-06-3 24:00:00'
        group by error
        union all 
        select count(status), left(status, 6) as result from statistics.get_status_stat
        where status is not null 
        and status != ''
        and created_at >= '2022-05-3 00:00:00'
        and  created_at <  '2022-06-3 24:00:00'
        group by result
        )result
        order by result desc;
        """
with profile("load_data"):
    df = load_from_db(query, conn)
# %%
