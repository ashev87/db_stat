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
from sqlalchemy import create_engine

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
        conn = create_engine("postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(secrets['user'], secrets['password'], secrets['host'], secrets['port'], secrets['dbname']))
        #conn = psycopg2.connect(**secrets)
    except (Exception) as error:
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
def load_from_db(query, conn):
    chunks = []
    for chunk in pd.read_sql_query(query, conn, chunksize=10000):
        chunks.append(chunk)
    df = pd.concat(list(chunks))
    return df

with profile("connect"):
    # stream results for avoiding memory issues
    conn = connect(st.secrets["postgres"]).execution_options(stream_results=True)

query = """
        SELECT client_phone, created_at, error, status, user_id
        FROM statistics.get_status_stat
        WHERE created_at >= '2022-05-1 00:00:00';
        """

with profile("load_data"):
    df = load_from_db(query, conn)