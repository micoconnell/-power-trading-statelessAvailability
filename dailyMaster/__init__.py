from datetime import datetime
import logging
import requests
import pandas as pd
import re
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import json
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
    blob_service_client1 = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=outageblobspremium;AccountKey=hYBKzIYX0cNoB//eU5OqR1m3AFiB3dEOqnI+BDUv94CQZ5Ep1fHEeMsczoJPQuIXRnkOIgxKSAXO+AStyiep+A==;EndpointSuffix=core.windows.net")
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
    DfChanges = DfChanges.reset_index()
    DfChanges['Date'] = DfChanges['Date'].astype("string")
    DfChanges= DfChanges.set_index('Date',drop=True)
    print(DfChanges)
    ## Naming Convention for blobs.
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    eventName = now + ".json"

    eventnameHTML = now +".html"






    eventNameCoal = now + "Coal" + ".json"
    eventNameGas = now + "Gas" + ".json"
    eventNameDual = now + "Dual" + ".json"
    eventNameHydro = now + "Hydro" + ".json"
    eventNameSolar = now + "Solar" + ".json"
    eventNameWind = now + "Wind" + ".json"
    eventNameStorage = now + "Storage" + ".json"
    eventNameOther = now + "Other" + ".json"

    eventNameCoalHTML = now + "Coal" + ".html"
    eventNameGasHTML = now + "Gas" + ".html"
    eventNameDualHTML = now + "Dual" + ".html"
    eventNameHydroHTML = now + "Hydro" + ".html"
    eventNameSolarHTML = now + "Solar"+ ".html"
    eventNameWindHTML = now + "Wind" + ".html"
    eventNameStorageHTML = now+  "Storage" + ".html"
    eventNameOtherHTML = now + "Other" + ".html"
    
    
    
    
    
    
    
    
    ## Next section is dedicated to detection along each of the type columns
    ## Make a dataframe that is primarily dedicated to Coal, then Gas etc etc.
    ## These outages are then compared against a filter (currently >100MW or <-100MW)
    ## and any outages that satsify this criteria are uploaded to specific blob storage.
    ## If empty is True, program proceeds checking.
    DFCOAL = DfChanges[["Coal"]]
    DFCOAL = DFCOAL.loc[DFCOAL[DFCOAL >= 5].any(axis=1) | (DFCOAL[DFCOAL <= -5].any(axis=1))]

    DFCOALJSON = DFCOAL.reset_index()
    type = DFCOALJSON.columns[1]
    datacoal = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFCOALJSON.iterrows()}
    }
    datacoal = pd.DataFrame(data=datacoal)
    datacoal = datacoal.to_json()
    
    
    
    
    DFCOALHTML = DFCOAL.to_html()
    if DFCOAL.empty == False:  
        DFCOAL = DFCOAL.to_json(orient="index",date_format="iso")
        container_clientCOAL = blob_service_client.get_container_client("90coalevent")
        blob_client = container_clientCOAL.get_blob_client(eventName)
        container_clientCOAL= blob_client.upload_blob(DFCOAL,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameCoal)
        container_clientCOAL1= blob_client1.upload_blob(datacoal,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameCoalHTML)
        container_clientHTML= blob_client.upload_blob(DFCOALHTML,overwrite=True)
        
        

# ###########################################################################################
    DFGAS = DfChanges[["Gas"]]
    
    DFGAS = DFGAS.loc[DFGAS[DFGAS >= 100].any(axis=1) | (DFGAS[DFGAS <= -100].any(axis=1))]
    
        
    DFGASJSON = DFGAS.reset_index()
    type = DFGASJSON.columns[1]
    DFGASJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFGASJSON.iterrows()}
    }
    DFGASJSON = pd.DataFrame(data=DFGASJSON)
    DFGASJSON = DFGASJSON.to_json()
    
    
    
    DFGASHTML = DFGAS.to_html()
    if DFGAS.empty == False:
        DFGAS = DFGAS.to_json(orient="index",date_format="iso")
        container_clientGAS = blob_service_client.get_container_client("90gasevent")
        blob_client = container_clientGAS.get_blob_client(eventName)
        container_clientGAS= blob_client.upload_blob(DFGAS,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameGas)
        container_clientCOAL1= blob_client1.upload_blob(DFGASJSON,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameGasHTML)
        container_clientHTML= blob_client.upload_blob(DFGASHTML,overwrite=True)
        
        
 
        
        
###########################################################################################
    DFDUAL = DfChanges[["DualFuel"]]
    
    DFDUAL = DFDUAL.loc[DFDUAL[DFDUAL >= 100].any(axis=1) | (DFDUAL[DFDUAL <= -100].any(axis=1))]
    
        
    DFDUALJSON = DFDUAL.reset_index()
    type = DFDUALJSON.columns[1]
    DFDUALJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFDUALJSON.iterrows()}
    }
    DFDUALJSON = pd.DataFrame(data=DFDUALJSON)
    DFDUALJSON = DFDUALJSON.to_json()
    
    

    DFDUALHTML = DFDUAL.to_html()
    if DFDUAL.empty == False:
        DFDUAL = DFDUAL.to_json(orient="index",date_format="iso")
        container_clientDUAL = blob_service_client.get_container_client("90dualevent")
        blob_client = container_clientDUAL.get_blob_client(eventName)
        container_clientDUAL= blob_client.upload_blob(DFDUAL,overwrite=True)
                
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameGas)
        container_clientCOAL1= blob_client1.upload_blob(DFDUALJSON,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameGasHTML)
        container_clientHTML= blob_client.upload_blob(DFDUALHTML,overwrite=True)   
        
        
        
 
###########################################################################################

    DFHYDRO = DfChanges[["Hydro"]]
    
    DFHYDRO = DFHYDRO.loc[DFHYDRO[DFHYDRO >= 100].any(axis=1) | (DFHYDRO[DFHYDRO <= -100].any(axis=1))]
    
        
    DFHYDROJSON = DFHYDRO.reset_index()
    type = DFHYDROJSON.columns[1]
    DFHYDROJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFHYDROJSON.iterrows()}
    }
    DFHYDROJSON = pd.DataFrame(data=DFHYDROJSON)
    DFHYDROJSON = DFHYDROJSON.to_json()
    

    DFHYDROHTML = DFHYDRO.to_html()
    if DFHYDRO.empty == False:
        DFHYDRO = DFHYDRO.to_json(orient="index",date_format="iso")
        container_clientHYDRO = blob_service_client.get_container_client("90hydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventName)
        container_clientHYDRO= blob_client.upload_blob(DFHYDRO,overwrite=True)
    
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameHydro)
        container_clientCOAL1= blob_client1.upload_blob(DFHYDROJSON,overwrite=True)     
                
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameHydroHTML)
        container_clientHTML= blob_client.upload_blob(DFHYDROHTML,overwrite=True)   


###########################################################################################
    DFOTHER = DfChanges[["Other"]]
    
    DFOTHER = DFOTHER.loc[DFOTHER[DFOTHER >= 100].any(axis=1) | (DFOTHER[DFOTHER <= -100].any(axis=1))]
    
        
    DFOTHERJSON = DFOTHER.reset_index()
    type = DFOTHERJSON.columns[1]
    DFOTHERJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFOTHERJSON.iterrows()}
    }
    DFOTHERJSON = pd.DataFrame(data=DFOTHERJSON)
    DFOTHERJSON = DFOTHERJSON.to_json()

    DFOTHERHTML = DFOTHER.to_html()
    if DFOTHER.empty == False:
        DFOTHER = DFOTHER.to_json(orient="index",date_format="iso")
        container_clientOTHER = blob_service_client.get_container_client("90otherevent")
        blob_client = container_clientOTHER.get_blob_client(eventName)
        container_clientOTHER= blob_client.upload_blob(DFOTHER,overwrite=True)
        
        
                    
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameOther)
        container_clientCOAL1= blob_client1.upload_blob(DFOTHERJSON,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameOtherHTML)
        container_clientHTML= blob_client.upload_blob(DFOTHERHTML,overwrite=True) 
        
        
 
###########################################################################################

    DFSOLAR = DfChanges[["Solar"]]
    
    DFSOLAR = DFSOLAR.loc[DFSOLAR[DFSOLAR >= 100].any(axis=1) | (DFSOLAR[DFSOLAR <= -100].any(axis=1))]
    
        
    DFSOLARJSON = DFSOLAR.reset_index()
    type = DFSOLARJSON.columns[1]
    DFSOLARJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFSOLARJSON.iterrows()}
    }
    DFSOLARJSON = pd.DataFrame(data=DFSOLARJSON)
    DFSOLARJSON = DFSOLARJSON.to_json()


    DFSOLARHTML = DFSOLAR.to_html()
    if DFSOLAR.empty == False:
        DFSOLAR = DFSOLAR.to_json(orient="index",date_format="iso")
        container_clientSOLAR = blob_service_client.get_container_client("90solarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventName)
        container_clientSOLAR= blob_client.upload_blob(DFSOLAR,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameSolar)
        container_clientCOAL1= blob_client1.upload_blob(DFSOLARJSON,overwrite=True) 
            
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameSolarHTML)
        container_clientHTML= blob_client.upload_blob(DFSOLARHTML,overwrite=True) 
        
        

###########################################################################################

    DFWIND = DfChanges[["Wind"]]
    DFWIND = DFWIND.loc[DFWIND[DFWIND >= 100].any(axis=1) | (DFWIND[DFWIND <= -100].any(axis=1))]
    
        
    DFWINDJSON = DFWIND.reset_index()
    type = DFWINDJSON.columns[1]
    DFWINDJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFWINDJSON.iterrows()}
    }
    DFWINDJSON = pd.DataFrame(data=DFWINDJSON)
    DFWINDJSON = DFWINDJSON.to_json()


    DFWINDHTML = DFWIND.to_html()
    if DFWIND.empty == False:
        DFWIND = DFWIND.to_json(orient="index",date_format="iso")
        container_clientWIND = blob_service_client.get_container_client("90windevent")
        blob_client = container_clientWIND.get_blob_client(eventName)
        container_clientWIND= blob_client.upload_blob(DFWIND,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameWind)
        container_clientCOAL1= blob_client1.upload_blob(DFWINDJSON,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameWindHTML)
        container_clientHTML= blob_client.upload_blob(DFWINDHTML,overwrite=True) 
        

        
###########################################################################################

    DFSTORAGE = DfChanges[["Energy"]]
    
    DFSTORAGE = DFSTORAGE.loc[DFSTORAGE[DFSTORAGE >= 100].any(axis=1) | (DFSTORAGE[DFSTORAGE <= -100].any(axis=1))]
    
        
    DFSTORAGEJSON = DFSTORAGE.reset_index()
    type = DFSTORAGEJSON.columns[1]
    DFSTORAGEJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFSTORAGEJSON.iterrows()}
    }
    DFSTORAGEJSON = pd.DataFrame(data=DFSTORAGEJSON)
    DFSTORAGEJSON = DFSTORAGEJSON.to_json()
    

    DFSTORAGEHTML = DFSTORAGE.to_html()
    if DFSTORAGE.empty == False:
        DFSTORAGE = DFSTORAGE.to_json(orient="index",date_format="iso")
        container_clientSTORAGE = blob_service_client.get_container_client("90storageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventName)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGE,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventNameStorage)
        container_clientCOAL1= blob_client1.upload_blob(DFSTORAGEJSON,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventNameStorageHTML)
        container_clientHTML= blob_client.upload_blob(DFSTORAGEHTML,overwrite=True) 
        


        
# ###########################################################################################
    
    ##Upload the drone file and replace the master file.
    eventName = "90dayMaster.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("90daymaster")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(dfMasterDrone,overwrite=True)

    