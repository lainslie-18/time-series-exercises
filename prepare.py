import pandas as pd
from datetime import timedelta, datetime
import numpy as np
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings("ignore")

import acquire


def prepare_store_data():
    # use a function to pull in data
    df = acquire.get_store_data()
    # convert sale_data column to datetime
    df.sale_date = pd.to_datetime(df.sale_date)
    # set sale data as index and sort
    df = df.set_index("sale_date").sort_index()
    
    # use assign to create additional columns
    df = df.assign(month = df.index.strftime('%B'), 
                   day_of_week = df.index.strftime('%A'),
                  sales_total = df.sale_amount * df.item_price)
    
    return df



def prepare_ops_data():
    
    # use function to pull in data
    df = acquire.acquire_open_power_systems_data()
    # change date column format
    df.Date = pd.to_datetime(df.Date)
    # set sale data as index and sort
    df = df.set_index("Date").sort_index()
    
    # use assign to create new columns
    df = df.assign(month = df.index.strftime('%B'), year = df.index.strftime('%Y'))
    # fill wind/solar nulls with 0 since there could be days of no wind or sun
    df = df.fillna(0)
    
    return df