from datetime import datetime
import pandas as pd
import numpy as np
import requests
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:


    startDateDASHED = datetime.now().strftime("%Y-%m-%d %H")
    endDate = pd.to_datetime(startDateDASHED) + pd.DateOffset(hours=-7)
    endDateHTML= datetime.now().strftime("%Y-%m-%d %H")
#adjust dates with the function below
    startDateDASHED = pd.to_datetime(startDateDASHED) + pd.DateOffset(hours=-7)
    
    startDateDASHED = startDateDASHED.strftime("%Y-%m-%d")
    print(startDateDASHED)

    def aeso_AILForecast(startDateDASHED,endDateDASHED):
        startDateDASHED = startDateDASHED
        endDateDASHED = endDateDASHED
        headers = {
            'accept': 'application/json',
            'X-API-Key': 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdWFsN2giLCJpYXQiOjE2NzQ2NjkwNTd9.nRWzrcZuJDkSfcaobYjzjxR6WpzgvvbRl09vmJNH7f4',
        }

        params = {
            'startDate': startDateDASHED,
            'endDate': endDateDASHED,
        }

        response = requests.get('https://api.aeso.ca/report/v1.1/price/poolPrice', params=params, headers=headers)
        dd=json.loads(response.text)
        df_SMP=pd.DataFrame(dd['return']['Pool Price Report'])
        AILForecast = df_SMP

        AILForecast = AILForecast.drop('begin_datetime_utc',axis =1)
        AILForecast['begin_datetime_mpt'] = pd.to_datetime(AILForecast['begin_datetime_mpt'])+ pd.DateOffset(hours=+1)
        AILForecast['begin_datetime_mpt'] = AILForecast['begin_datetime_mpt'].dt.hour
        AILForecast['begin_datetime_mpt'] = AILForecast['begin_datetime_mpt'].astype(str)
        AILForecast['HourEnding'] = "HE " + AILForecast['begin_datetime_mpt']
        AILForecast=AILForecast.drop(["begin_datetime_mpt","pool_price","rolling_30day_avg"],axis=1)
        #AILForecast['begin_datetime_mpt'] = AILForecast['begin_datetime_mpt'].strftime("%H")
        AILForecast.set_index("HourEnding",inplace = True)

        AILForecast = AILForecast.apply(pd.to_numeric)
        
        #
        
        return AILForecast

    AILForecast = aeso_AILForecast(startDateDASHED,startDateDASHED)
    #AILForecast = AILForecast[AILForecast.index >= endDate]
    #AIL = AILForecast['forecast_pool_price']
    #AILForecast = AILForecast[AILForecast.index >= endDate]
    AIL = AILForecast.dropna(how='all')

    AIL = AIL.tail(n=6)
    AIL = AIL.fillna('')

    #AIL = AIL.reset_index()
    print(AIL)
    truthy = (AIL['forecast_pool_price'] >= 150).any()
    truthy500 = (AIL['forecast_pool_price'] >= 500).any()
    #AILForecast = AILForecast.loc[AILForecast[AILForecast >= 150].any(axis=1)]
    #AILForecast=AILForecast.drop(["pool_price","rolling_30day_avg"],axis=1)
    if truthy:
        try:
            endDate = endDateHTML
            eventName = ".html"
            endDate = endDate + eventName
            
            AIL = AIL.to_html()
            blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
            container_client = blob_service_client.get_container_client("checktext")
            blob_client = container_client.get_blob_client(endDate)
            container_client = blob_client.upload_blob(AIL,overwrite=False) 
        except:
            print("An exception occurred")
            
    if truthy500:
        try:
            endDate = endDateHTML
            eventName = ".html"
            endDate = endDate + eventName
            
            AIL = AIL.to_html()
            blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
            container_client = blob_service_client.get_container_client("checktext500")
            blob_client = container_client.get_blob_client(endDate)
            container_client = blob_client.upload_blob(AIL,overwrite=False) 
        except:
            print("An exception occurred")

        
        

    