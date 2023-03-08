import datetime
import logging
import requests
import pandas as pd
import re

import azure.functions as func
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import xlrd

def main(mytimer: func.TimerRequest): 
    
    ## This function saves the pickled dataframe minute by minute. Dummy.pkl is the roving data file
    ## Meaning it's constantly being updated and compared to the static.
    url = 'https://www.bchydro.com/bctc/system_cms/outages/forced/Annual_Plan_Extract.xls'
    df = pd.read_excel(url,index_col=False)
    date_LastUpdate=df.iloc[0:1,0:1]
    df.columns = df.iloc[0]
    df1=df.iloc[1:]
    df = df1
    df = df.dropna(axis=1,how = 'all')
    df666=date_LastUpdate.to_string()
    df666 = df666[df666.find("(")+1:df666.find(")")]
    column_names = ['Outage #','Outage Status','AP Outage Title','Outage Purpose','Requested Equipment','Start Date','Complete Date','Recall Time','Outage Type','Path 3 N->S ING - CUS','Path 3 N->S BC - US','Path 3 S -> N CUS ING','Path 3 S -> N US - BC','Path 1 W -> E BC - AB','Path 1 E -> W AB - BC','Resource Impact','Load Impact','Posted to Plan','Under Review AOP','Revision Date']
    keywords = ["Completed"]
    df.columns.values[0:20] = column_names[0:20]
    df= df[~df['Outage Status'].str.contains('|'.join(keywords))]
    df['Complete Date']= pd.to_datetime(df['Complete Date'])
    df['Start Date']= pd.to_datetime(df['Start Date'])
    df['Revision Date']= pd.to_datetime(df['Revision Date'])
    df['Start Date'] = df['Start Date'].astype(str)
    
    df['Revision Date'] = df['Revision Date'].astype(str)

    # push todays date and filter by flows into Alberta.
    
    df= df[df['Path 1 W -> E BC - AB'] | df['Path 1 E -> W AB - BC'] > 0]
    df= df[df['Complete Date'] >= pd.Timestamp.today() - pd.Timedelta('1D')]
    df['Complete Date'] = df['Complete Date'].astype(str)

    
    df = df.drop(['AP Outage Title','Path 3 N->S BC - US','Path 3 S -> N CUS ING', 'Load Impact','Path 3 N->S ING - CUS','Outage Purpose','Recall Time','Under Review AOP','Resource Impact','Posted to Plan','Requested Equipment','Path 3 S -> N US - BC','Revision Date'], axis=1)

    df = df.to_csv()
    
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientTransmission = blob_service_client.get_container_client("transmissiondrone")
    blob_client = container_clientTransmission.get_blob_client("transmissiondrone.csv")
    container_clientTransmission= blob_client.upload_blob(df,overwrite=True)

