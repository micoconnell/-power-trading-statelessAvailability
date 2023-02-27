import datetime
import logging
import requests
import pandas as pd
import re
import numpy as np
import azure.functions as func
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import xlrd
import dataframe_image as dfi


def main(mytimer: func.TimerRequest) :

    ## Read Drone
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientDrone = blob_service_client.get_container_client("transmissiondrone")    
    blob_client = container_clientDrone.get_blob_client("transmissiondrone.csv")
    dfDrone = blob_client.download_blob()
    dfDrone = pd.read_csv(dfDrone,index_col=0)
    dfDroneStagnant = dfDrone
    dfDroneStagnant['Complete Date']= pd.to_datetime(dfDroneStagnant['Complete Date'])
    dfDroneStagnant['Start Date']= pd.to_datetime(dfDroneStagnant['Start Date'])
    dfDroneStagnant['Revision Date']= pd.to_datetime(dfDroneStagnant['Revision Date'])
    dfDroneStagnant = dfDroneStagnant.to_csv()
 
    ## Read Master file
    container_clientDrone = blob_service_client.get_container_client("transmissionmaster")    
    blob_client = container_clientDrone.get_blob_client("transmissionmaster.csv")
    dfMaster = blob_client.download_blob()
    dfMaster = pd.read_csv(dfMaster,index_col=0)



    dfDrone['Complete Date']= pd.to_datetime(dfDrone['Complete Date'])
    dfDrone['Start Date']= pd.to_datetime(dfDrone['Start Date'])
    dfDrone['Revision Date']= pd.to_datetime(dfDrone['Revision Date'])

    dfMaster['Complete Date']= pd.to_datetime(dfMaster['Complete Date'])
    dfMaster['Start Date']= pd.to_datetime(dfMaster['Start Date'])
    dfMaster['Revision Date']= pd.to_datetime(dfMaster['Revision Date'])

    ##Concat both files
    df_diffNEW = pd.concat([dfDrone,dfMaster]).reset_index(drop=True)
    df_diffNEWMASTER = df_diffNEW.sort_values('Revision Date').drop_duplicates('Outage #',keep='first')
    df_diffCHANGELOG = df_diffNEW.sort_values('Revision Date').drop_duplicates('Outage #',keep='last')
    
    df = pd.merge(df_diffNEWMASTER, dfMaster, how='left', indicator='Exist')
    df['Exist'] = np.where(df.Exist == 'both', True, False)

    ##False makes all the newer ones appear
    ## True keeps all the old files
    dfNEW = df[df['Exist']==False].drop(['Exist','Outage #'], axis=1)
    dfOLD = df[df['Exist']==True].drop(['Exist','Outage #'], axis=1)
    ## df is the change log. 
    ## df_diffNEWMASTER is the new Master file and the complete schedule

    if dfNEW.empty == False :
        ## If new drone is different, upload and the replace master file
        container_clientMaster = blob_service_client.get_container_client("transmissionmaster")
        blob_client = container_clientMaster.get_blob_client("transmissionmaster.csv")
        container_clientMaster= blob_client.upload_blob(dfDroneStagnant,overwrite=True)
        
        # Upload an html of the changes (for now, this is just an old master file.)
        dfNEW = dfNEW.to_html()
        container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdate")
        blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdate.html")
        container_clientHTMLupdate= blob_client.upload_blob(dfNEW,overwrite=True)

        dfOLD = dfOLD.to_html()
        container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdateold")
        blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdateold.html")
        container_clientHTMLupdate= blob_client.upload_blob(dfOLD,overwrite=True)

        ## Upload an html of the new *complete* schedule
        dfDrone = dfDrone.to_html()
        container_clientHTML = blob_service_client.get_container_client("transmissionhtml")
        blob_client = container_clientHTML.get_blob_client("transmission.html")
        container_clientHTML= blob_client.upload_blob(dfDrone,overwrite=True)