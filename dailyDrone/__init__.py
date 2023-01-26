from datetime import datetime
import logging
import requests
import pandas as pd
import re
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:


    ## Load Daily outage table and save results. Timer is set for every two minutes, starting at the top of the 
    ## hour. 
    url2 = "http://ets.aeso.ca/ets_web/ip/Market/Reports/DailyOutageReportServlet?contentType=html"
    df2 = pd.read_html(url2)
    df2 = df2[1]
    df2 = df2.drop(df2.index [ [ 0,1,2,3 ] ])
    ## Index's from HTML do not play nice. Re-index and rename columns.
    df2.columns = ['Date', 'Coal', 'Gas', 'DualFuel','Hydro','Wind','Solar','Energy','Other','Load']
    df2 = df2.set_index('Date')
    
    ## Convert into comma seperated file for easy upload to blob storage.
    df2 = df2.round(0)
    df2 = df2.to_csv()
    
    ##Upload Drone file to 90daydrone.csv in Sevendaypremium blob storage. Overwrite == true as file is replaced 
    ## with every new trigger
    eventName = "90daydrone.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("90daydrone")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(df2,overwrite=True)

    
