from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import requests
import azure.functions as func
import pandas as pd
import lxml
import pyodbc
import time
import re
from azure.storage.blob import BlobServiceClient
def main(mytimer: func.TimerRequest) -> None:
    #Function below gets the dates needed. Will update the AESO column in all Databases.
    
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("transmissionmaster")
    blob_client = container_client.get_blob_client('transmissionmaster.csv')
    dfTransmission = blob_client.download_blob()
    #Load drone file and place into dataframe.
    dfTransmission = pd.read_csv(dfTransmission,index_col=None)

    filtered_df = dfTransmission[~dfTransmission['Outage Status'].str.contains('Denied')]
    dfTransmission = filtered_df.drop(['Unnamed: 0','Outage Status', 'Outage #','Outage Type'],axis=1)
    dfTransmission['Start Date']= pd.to_datetime(dfTransmission['Start Date'])
    dfTransmission['Complete Date']= pd.to_datetime(dfTransmission['Complete Date'])
    dfTransmission['Start Date'] = dfTransmission['Start Date'].dt.strftime('%Y%m%d')
    dfTransmission['Complete Date'] = dfTransmission['Complete Date'].dt.strftime('%Y%m%d')
    #dfTransmission= dfTransmission.set_index(['Start Date'],drop=True)
    print(dfTransmission)
    def init_connection():

        server =r'tcp:supowerdatabase.database.windows.net' 
        database =r'SUpowerFinancials' 
        username =r'micoconnell1' 
        password =r'anisoTropical+308'
        driver= r'{ODBC Driver 17 for SQL Server}'

        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' +
        server + ';PORT=1433;DATABASE=' + database +
        ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        return cursor

    cursor = init_connection()
    today = date.today()
    
    
    def outageAESO(DF):
        DF=DF
        i=0
        for ind in DF.index:
            first_day_of_month = ind
            first_day_of_month = str(first_day_of_month)
            first_day_of_month=first_day_of_month.replace(".","")
            importintertie = "BCMATLIM"
            exportintertie = "BCMATLEX"
            last_date_of_month = first_day_of_month
            tempVal = DF.iloc[i]
            importValues = tempVal['Path 1 W -> E BC - AB']
            exportValues = tempVal['Path 1 E -> W AB - BC']
            startDate = tempVal['Start Date'] 
            endDate = tempVal['Complete Date']

            i=i+1
            response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype=intertieAB&asset={0}&datestart={1}&dateend={2}&volume={3}".format(importintertie,startDate,endDate,importValues))
            # print(response)
            response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype=intertieAB&asset={0}&datestart={1}&dateend={2}&volume={3}".format(exportintertie,startDate,endDate,exportValues))

    # # # Lame Timestamp [NS] Conversion. 
    
    def clean():


        today = date.today()
        first_day_of_month = today
        first_day_of_month = str(first_day_of_month)
        first_day_of_month=first_day_of_month.replace(".","")
        importintertie = "BCMATLIM"
        exportintertie = "BCMATLEX"
        value = 0
        
        last_date_of_month = first_day_of_month

        importValues = 0
        exportValues = 0
        startDate = first_day_of_month
        today = date.today().replace(day=1)
        twentyfour_months = today + relativedelta(months=+25, days=-1)
        twentyfour_months = str(twentyfour_months)
        twentyfour_months= twentyfour_months.replace("-","")

        response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype=intertieAB&asset={0}&datestart={1}&dateend={2}&volume={3}".format(importintertie,first_day_of_month,twentyfour_months,importValues))
        # print(response)
        response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype=intertieAB&asset={0}&datestart={1}&dateend={2}&volume={3}".format(exportintertie,first_day_of_month,twentyfour_months,exportValues))

        
    
    
    
    
    
    
    
    
    
    
    clean()
    outageAESO(dfTransmission)
