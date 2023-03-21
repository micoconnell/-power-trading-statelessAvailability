from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from pretty_html_table import build_table
def main(mytimer: func.TimerRequest) -> None:


    startDateDASHED = datetime.now().strftime("%Y-%m-%d %H")
    timing = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M")
    #timing = timing.replace(tzinfo=None) 
    print(timing)
    endDate = pd.to_datetime(startDateDASHED) + pd.DateOffset(hours=-7)

    endDateHTML= datetime.now().strftime("%Y-%m-%d %H")
    endDate = endDateHTML
    eventName = ".html"
    endDate = endDate + eventName
    #endDateLast = endDateLast + eventName      
    #endDateLast = pd.to_datetime(endDate) + pd.DateOffset(hours=-1)
# #adjust dates with the function below
#     startDateDASHED = pd.to_datetime(startDateDASHED) + pd.DateOffset(hours=-7)
    
#     startDateDASHED = startDateDASHED.strftime("%Y-%m-%d")


    def aeso_AILForecast():
        headers = {
            'accept': 'application/json',
            'X-API-Key': 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdWFsN2giLCJpYXQiOjE2NzQ2NjkwNTd9.nRWzrcZuJDkSfcaobYjzjxR6WpzgvvbRl09vmJNH7f4',
        }


        response = requests.get('https://api.aeso.ca/report/v1.1/price/systemMarginalPrice/current',headers=headers)
        dd=json.loads(response.text)
        df_SMP=pd.DataFrame(dd['return']['System Marginal Price Report'])
        AILForecast = df_SMP

        AILForecast = AILForecast.drop('begin_datetime_utc',axis =1)

        AILForecast.set_index("begin_datetime_mpt",inplace = True)

        AILForecast = AILForecast.apply(pd.to_numeric)

        #

        return AILForecast

    AILForecast = aeso_AILForecast()

    
    timing = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M")
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("checksmp")
    eventlistGAS = []
    blobs_listGAS = container_client.list_blobs()
    for blob in blobs_listGAS:
        blobs = blob.last_modified
        eventlistGAS.append(blobs)
        
    lastModified = eventlistGAS[-1]
    secondLastModified = eventlistGAS[-2]
    lastModified = pd.to_datetime(lastModified)
    lastModified = lastModified.replace(tzinfo=None)
    secondLastModified = pd.to_datetime(secondLastModified)
    secondLastModified = secondLastModified.replace(tzinfo=None)
    timing = pd.to_datetime(timing)
    print(lastModified,secondLastModified,timing)


    truthy = (AILForecast['system_marginal_price'] >= 150).any()
    AILForecast = AILForecast.reset_index()
    time_diff = timing - lastModified
    print(time_diff)
    if time_diff > timedelta(hours=3):
        #AILForecast = AILForecast.loc[AILForecast[AILForecast >= 150].any(axis=1)]
        #AILForecast=AILForecast.drop(["begin_datetime_utc","end_datetime_utc","end_datetime_mpt"],axis=1)
        if truthy:
            try:

                html_tableSMP = build_table(AILForecast
                    , 'blue_light'
                    , font_size='medium'
                    , font_family='Open Sans sans-serif'
                    , text_align='justify'
                    , width='200px'
                    , index=False
                    ,conditions={
                        'SMP': {
                            'min': -1,
                            'max': 1,
                            'min_color': 'green',
                            'max_color': 'black',
                        }
                    }) 
                AIL = html_tableSMP
                blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
                container_client = blob_service_client.get_container_client("checksmp")
                blob_client = container_client.get_blob_client(endDate)
                container_client = blob_client.upload_blob(AIL,overwrite=False) 
            except:
                print("An exception occurred")
    else:
        print(True)
    




       


        
        

    