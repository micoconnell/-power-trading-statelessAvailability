import datetime
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:
    
    ## Read the site, push into dataframe
    URLMONTH = 'http://ets.aeso.ca/ets_web/ip/Market/Reports/MonthlyOutageForecastReportServlet?contentType=html'
    DFMONTHLY = pd.read_html(URLMONTH)
    DFMONTHLY = DFMONTHLY[2]
    DFMONTHLY = DFMONTHLY.set_index('Month')
    print(DFMONTHLY)
    DFMONTHLY = DFMONTHLY.to_csv()
    ##Set month as index and push to drone file in monthlydrone
    eventName = "monthlydrone.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("monthlydrone")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(DFMONTHLY,overwrite=True)

