import datetime
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:


    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("aesoinfo")
    blob_client = container_client.get_blob_client("monthlyinfo.csv")
    container_client = blob_client.download_blob()
    dfdrone = pd.read_csv(container_client,index_col=None)
    dfdrone = dfdrone.set_index("ID Number")

    dfdrone['Posting Date'] = pd.to_datetime(dfdrone['Posting Date'])
    print(dfdrone)
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("aesoinfomaster")
    blob_client = container_client.get_blob_client("monthlyinfoMaster.csv")
    container_client = blob_client.download_blob()
    dfMaster = pd.read_csv(container_client,index_col=None)

    dfMaster = dfMaster.set_index("ID Number")
    dfMaster['Posting Date'] = pd.to_datetime(dfMaster['Posting Date'])
    print(dfMaster)
    
    diff_df = dfdrone.compare(dfMaster)
    print(diff_df)
    if diff_df.empty == False:
        diff_df = diff_df.to_html()
        eventName2 = "newdiff.html"
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
        container_client = blob_service_client.get_container_client("aesoinfodiff")
        blob_client = container_client.get_blob_client(eventName2)
        container_client = blob_client.upload_blob(diff_df,overwrite=True)
        
        
        dfdrone = dfdrone.to_csv(index=True)
        eventName = "monthlyinfoMaster.csv"
        blob_service_client1 = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
        container_client1 = blob_service_client1.get_container_client("monthlyinfomaster")
        blob_client1 = container_client1.get_blob_client(eventName)
        container_client1 = blob_client1.upload_blob(dfdrone,overwrite=True)