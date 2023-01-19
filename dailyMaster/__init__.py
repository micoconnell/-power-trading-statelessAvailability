from datetime import datetime
import logging
import requests
import pandas as pd
import re
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:



    ## Start with datetime now as this will be the file name.
    now = datetime.now().strftime("%Y%m%d-%H%M%S")

    eventName = "90daydrone.csv"
    
    ## Start Azure BlobServiceClient with connection string from sevendaypremium blob storage. Load drone file.
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("90daydrone")
    blob_client = container_client.get_blob_client('90daydrone.csv')
    
    #Load drone file and place into dataframe.
    dfAlldrone = blob_client.download_blob()
    dfAlldrone = pd.read_csv(dfAlldrone)
    
    ## dfMasterDrone set from previously loaded drone file after comparions
    ## Next timer will cycle and compare against this file and so on. 
    dfMasterDrone = dfAlldrone.to_csv()
    
    #Set dfAllDrone columns to numeric. Set index on Date after.
    dfAlldrone['Coal'] = pd.to_numeric(dfAlldrone['Coal'])
    dfAlldrone['Gas'] = pd.to_numeric(dfAlldrone['Gas'])
    dfAlldrone['DualFuel'] = pd.to_numeric(dfAlldrone['DualFuel'])
    dfAlldrone['Hydro'] = pd.to_numeric(dfAlldrone['Hydro'])
    dfAlldrone['Wind'] = pd.to_numeric(dfAlldrone['Wind'])
    dfAlldrone['Solar'] = pd.to_numeric(dfAlldrone['Solar'])
    dfAlldrone['Energy'] = pd.to_numeric(dfAlldrone['Energy'])
    dfAlldrone['Other'] = pd.to_numeric(dfAlldrone['Other'])
    dfAlldrone.Date = pd.to_datetime(dfAlldrone.Date)
    dfAlldrone= dfAlldrone.set_index('Date',drop=True)

    ## Load up the master file. Rinse and repeat the same process.
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientMaster = blob_service_client.get_container_client("90daymaster")
    blob_client = container_clientMaster.get_blob_client('90dayMaster.csv')
    dfMaster = blob_client.download_blob()
    dfMaster = pd.read_csv(dfMaster)
    dfMaster['Coal'] = pd.to_numeric(dfMaster['Coal'])
    dfMaster['Gas'] = pd.to_numeric(dfMaster['Gas'])
    dfMaster['DualFuel'] = pd.to_numeric(dfMaster['DualFuel'])
    dfMaster['Hydro'] = pd.to_numeric(dfMaster['Hydro'])
    dfMaster['Wind'] = pd.to_numeric(dfMaster['Wind'])
    dfMaster['Solar'] = pd.to_numeric(dfMaster['Solar'])
    dfMaster['Energy'] = pd.to_numeric(dfMaster['Energy'])
    dfMaster['Other'] = pd.to_numeric(dfMaster['Other'])
    dfMaster.Date = pd.to_datetime(dfMaster.Date)
    dfMaster= dfMaster.set_index('Date',drop=True)
    
    ## Minus columns between both dataframes. Drone - Master so that new outages are positive, units 
    ## that are returning are displayed as negative.
    DfChanges = dfAlldrone - dfMaster

    ## Naming Convention for blobs.
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    eventName = now + "b.csv"
    eventNameb = now + "b.csv"
    
    ## Next section is dedicated to detection along each of the type columns
    ## Make a dataframe that is primarily dedicated to Coal, then Gas etc etc.
    ## These outages are then compared against a filter (currently >100MW or <-100MW)
    ## and any outages that satsify this criteria are uploaded to specific blob storage.
    
    ## If empty is True, program proceeds checking.
    DFCOAL = DfChanges[["Coal"]]
    DFCOAL = DFCOAL.loc[DFCOAL[DFCOAL >= 100].any(axis=1)]
    if DFCOAL.empty == False:
        DFCOAL = DFCOAL.to_csv()
        container_clientCOAL = blob_service_client.get_container_client("90coalevent")
        blob_client = container_clientCOAL.get_blob_client(eventName)
        container_clientCOAL= blob_client.upload_blob(DFCOAL,overwrite=True)
    DFCOALb = DfChanges[["Coal"]]
    DFCOALb = DFCOALb.loc[DFCOALb[DFCOALb <= -100].any(axis=1)]
    if DFCOALb.empty == False:
        DFCOALb = DFCOALb.to_csv()
        container_clientCOAL = blob_service_client.get_container_client("90coalevent")
        blob_client = container_clientCOAL.get_blob_client(eventNameb)
        container_clientCOAL= blob_client.upload_blob(DFCOALb,overwrite=True)
###########################################################################################
    DFGAS = DfChanges[["Gas"]]
    DFGAS = DFGAS.loc[DFGAS[DFGAS >= 100].any(axis=1)]
    if DFGAS.empty == False:
        DFGAS = DFGAS.to_csv()
        container_clientGAS = blob_service_client.get_container_client("90gasevent")
        blob_client = container_clientGAS.get_blob_client(eventName)
        container_clientGAS= blob_client.upload_blob(DFGAS,overwrite=True)
    DFGASb = DfChanges[["Gas"]]
    DFGASb = DFGASb.loc[DFGASb[DFGASb <= -100].any(axis=1)]
    if DFGASb.empty == False:
        DFGASb = DFGASb.to_csv()
        container_clientGAS = blob_service_client.get_container_client("90gasevent")
        blob_client = container_clientGAS.get_blob_client(eventNameb)
        container_clientGAS= blob_client.upload_blob(DFGASb,overwrite=True)
###########################################################################################
    DFDUAL = DfChanges[["DualFuel"]]
    DFDUAL = DFDUAL.loc[DFDUAL[DFDUAL >= 100].any(axis=1)]
    if DFDUAL.empty == False:
        DFDUAL = DFDUAL.to_csv()
        container_clientDUAL = blob_service_client.get_container_client("90dualevent")
        blob_client = container_clientDUAL.get_blob_client(eventName)
        container_clientDUAL= blob_client.upload_blob(DFDUAL,overwrite=True)
    DFDUALb = DfChanges[["DualFuel"]]
    DFDUALb = DFDUALb.loc[DFDUALb[DFDUALb <= -100].any(axis=1)]
    if DFDUALb.empty == False:
        DFDUALb = DFDUALb.to_csv()
        container_clientDUAL = blob_service_client.get_container_client("90dualevent")
        blob_client = container_clientDUAL.get_blob_client(eventNameb)
        container_clientDUAL= blob_client.upload_blob(DFDUALb,overwrite=True)
###########################################################################################

    DFHYDRO = DfChanges[["Hydro"]]
    DFHYDRO = DFHYDRO.loc[DFHYDRO[DFHYDRO >= 100].any(axis=1)]
    if DFHYDRO.empty == False:
        DFHYDRO = DFHYDRO.to_csv()
        container_clientHYDRO = blob_service_client.get_container_client("90hydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventName)
        container_clientHYDRO= blob_client.upload_blob(DFHYDRO,overwrite=True)
    DFHYDROb = DfChanges[["Hydro"]]
    DFHYDROb = DFHYDROb.loc[DFHYDROb[DFHYDROb <= -100].any(axis=1)]
    if DFHYDROb.empty == False:
        DFHYDROb = DFHYDROb.to_csv()
        container_clientHYDRO = blob_service_client.get_container_client("90hydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventName)
        container_clientHYDRO= blob_client.upload_blob(DFHYDROb,overwrite=True)

###########################################################################################
    DFOTHER = DfChanges[["Other"]]
    DFOTHER = DFOTHER.loc[DFOTHER[DFOTHER >= 100].any(axis=1)]
    if DFOTHER.empty == False:
        DFOTHER = DFOTHER.to_csv()
        container_clientOTHER = blob_service_client.get_container_client("90otherevent")
        blob_client = container_clientOTHER.get_blob_client(eventName)
        container_clientOTHER= blob_client.upload_blob(DFOTHER,overwrite=True)
    DFOTHERb = DfChanges[["Other"]]
    DFOTHERb = DFOTHERb.loc[DFOTHERb[DFOTHERb <= -100].any(axis=1)]
    if DFOTHERb.empty == False:
        DFOTHERb = DFOTHERb.to_csv()
        container_clientOTHER = blob_service_client.get_container_client("90otherevent")
        blob_client = container_clientOTHER.get_blob_client(eventNameb)
        container_clientOTHER= blob_client.upload_blob(DFOTHERb,overwrite=True)

###########################################################################################

    DFSOLAR = DfChanges[["Solar"]]
    DFSOLAR = DFSOLAR.loc[DFSOLAR[DFSOLAR >= 100].any(axis=1)]
    if DFSOLAR.empty == False:
        DFSOLAR = DFSOLAR.to_csv()
        container_clientSOLAR = blob_service_client.get_container_client("90solarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventName)
        container_clientSOLAR= blob_client.upload_blob(DFSOLAR,overwrite=True)
    DFSOLARb = DfChanges[["Solar"]]
    DFSOLARb = DFSOLARb.loc[DFSOLARb[DFSOLARb <= -100].any(axis=1)]
    if DFSOLARb.empty == False:
        DFSOLARb = DFSOLARb.to_csv()
        container_clientSOLAR = blob_service_client.get_container_client("90solarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventName)
        container_clientSOLAR= blob_client.upload_blob(DFSOLARb,overwrite=True)
###########################################################################################

    DFWIND = DfChanges[["Wind"]]
    DFWIND = DFWIND.loc[DFWIND[DFWIND >= 100].any(axis=1)]
    if DFWIND.empty == False:
        DFWIND = DFWIND.to_csv()
        container_clientWIND = blob_service_client.get_container_client("90windevent")
        blob_client = container_clientWIND.get_blob_client(eventName)
        container_clientWIND= blob_client.upload_blob(DFWIND,overwrite=True)
    DFWINDb = DfChanges[["Wind"]]
    DFWINDb = DFWINDb.loc[DFWINDb[DFWINDb <= -100].any(axis=1)]
    if DFWINDb.empty == False:
        DFWINDb = DFWINDb.to_csv()
        container_clientWIND = blob_service_client.get_container_client("90windevent")
        blob_client = container_clientWIND.get_blob_client(eventNameb)
        container_clientWIND= blob_client.upload_blob(DFWINDb,overwrite=True)
###########################################################################################

    DFSTORAGE = DfChanges[["Energy"]]
    DFSTORAGE = DFSTORAGE.loc[DFSTORAGE[DFSTORAGE >= 100].any(axis=1)]
    if DFSTORAGE.empty == False:
        DFSTORAGE = DFSTORAGE.to_csv()
        container_clientSTORAGE = blob_service_client.get_container_client("90storageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventName)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGE,overwrite=True)

    DFSTORAGEb = DfChanges[["Energy"]]
    DFSTORAGEb = DFSTORAGEb.loc[DFSTORAGEb[DFSTORAGEb <= -100].any(axis=1)]
    if DFSTORAGEb.empty == False:
        DFSTORAGEb = DFSTORAGEb.to_csv()
        container_clientSTORAGE = blob_service_client.get_container_client("90storageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventName)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGEb,overwrite=True)
###########################################################################################
    
    ##Upload the drone file and replace the master file.
    eventName = "90dayMaster.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("90daymaster")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(dfMasterDrone,overwrite=True)

    