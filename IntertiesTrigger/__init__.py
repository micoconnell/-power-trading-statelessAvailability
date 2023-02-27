from datetime import datetime
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    now = datetime.now().strftime("%Y%m%d-%H%M%S")

    eventName = "transmissiondrone.csv"
    
    ## Start Azure BlobServiceClient with connection string from sevendaypremium blob storage. Load drone file.
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("transmissiondrone")
    blob_client = container_client.get_blob_client('transmissiondrone.csv')
    
    #Load drone file and place into dataframe.
    df = blob_client.download_blob()
    df = pd.read_csv(df)
    
    ## dfMasterDrone set from previously loaded drone file after comparions
    ## Next timer will cycle and compare against this file and so on. 

