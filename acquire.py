import pandas as pd
import numpy as np
import requests
import os


def get_zgulde_data(base_url, end_point):
    '''
    This function takes in a base url and end point, checks and returns a dataframe, if the file is 
    saved locally, otherwise makes an http request and returns all pages requested from the API as a 
    dataframe and saved the file locally as a csv
    '''
    if os.path.isfile(end_point + '.csv'):
        return pd.read_csv(end_point + '.csv', index_col=0)
        
    else:     
        # use requests.get to make an http request
        response = requests.get(base_url)
        # convert response to json and assign to a variable
        data = response.json()
        # convert items within json format to a dataframe
        df = pd.DataFrame(data['payload'][end_point])
    
        while data['payload']['next_page'] != None:
            # pull items from next page
            response = requests.get('https://python.zgulde.net' + data['payload']['next_page'])
            # convert items to json
            data = response.json()
            # convert json to dataframe and concatenate onto original 
            df = pd.concat([df, pd.DataFrame(data['payload'][end_point])]).reset_index(drop=True)
        
    # Cache data
    df.to_csv(end_point + '.csv')
    
    return df


def combine_items_stores_sales(sales, items, stores):
    '''
    This function takes in three dataframes, merges them, and returns a combined dataframe
    '''
    # rename columns in sales to match columns in other dataframes
    sales = sales.rename(columns={'item':'item_id', 'store':'store_id'})
    # merge on column name found in both dataframes
    df = sales.merge(items, how='left', on='item_id')
    # merge on column name found in both dataframes
    df = df.merge(stores, how='left', on='store_id')
    
    return df


def acquire_open_power_systems_data():
    '''
    This function reads in csv data and returns a dataframe
    '''
    # convert response to json and assign to a variable
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    
    return df


def read_csv(str_file_path):
    '''
    This function takes in a csv file path as a string, reads in the data, and returns a dataframe
    '''
    df = pd.read_csv(str_file_path)
    
    return df