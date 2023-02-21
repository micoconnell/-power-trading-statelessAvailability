from datetime import datetime
import pandas as pd
import numpy as np
import requests
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:


    global BCIM
    global startDateDASHED
    global endDateDASHED
    global startDateTGT
    global endDateTGT

    endDateTGT = datetime.now().strftime("%Y%m%d")
    startDateTGT = datetime.now().strftime("%Y%m%d")
    startDateDASHED = datetime.now().strftime("%Y-%m-%d %H")
    endDate = datetime.now().strftime("%Y-%m-%d %H:%M")
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
        
        AILForecast.set_index("begin_datetime_mpt",inplace = True)

        AILForecast = AILForecast.apply(pd.to_numeric)
        
        AILForecast = AILForecast[AILForecast.index > endDate]
        
        return AILForecast

    AILForecast = aeso_AILForecast(startDateDASHED,startDateDASHED)
    # AIL = AILForecast['forecast_pool_price']
    # AIL = AIL.dropna()
    AILForecast = AILForecast.loc[AILForecast[AILForecast >= 60].any(axis=1)]
    AILForecast=AILForecast.drop(["pool_price","rolling_30day_avg"],axis=1)
    print(AILForecast)
    if AILForecast.empty == False:
        try:
            endDate = endDateHTML
            eventName = ".html"
            endDate = endDate + eventName
            
            AIL = AILForecast.to_html()
            print(AIL)
            blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
            container_client = blob_service_client.get_container_client("checktext")
            blob_client = container_client.get_blob_client(endDate)
            container_client = blob_client.upload_blob(AIL,overwrite=False) 
        except:
            print("An exception occurred")

    
    

 