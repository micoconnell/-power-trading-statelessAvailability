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

    ## Naming Convention for blobs.
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    eventName = now + ".json"
    eventNameb = now + "b.json"
    eventnameHTML = now +".html"
    eventnameHTMLb = now +"b.html"
    ## Next section is dedicated to detection along each of the type columns
    ## Make a dataframe that is primarily dedicated to Coal, then Gas etc etc.
    ## These outages are then compared against a filter (currently >100MW or <-100MW)
    ## and any outages that satsify this criteria are uploaded to specific blob storage.
    ## If empty is True, program proceeds checking.
    DFCOAL = DfChanges[["Coal"]]
    DFCOAL = DFCOAL.loc[DFCOAL[DFCOAL >= 100].any(axis=1)]
    DFCOALHTML = DFCOAL.to_html()
    if DFCOAL.empty == False:  
        DFCOAL = DFCOAL.to_json(orient="index",date_format="iso")
        container_clientCOAL = blob_service_client.get_container_client("90coalevent")
        blob_client = container_clientCOAL.get_blob_client(eventName)
        container_clientCOAL= blob_client.upload_blob(DFCOAL,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFCOAL,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFCOALHTML,overwrite=True)
        
        
    DFCOALb = DfChanges[["Coal"]]
    DFCOALb = DFCOALb.loc[DFCOALb[DFCOALb <= -100].any(axis=1)]
    DFCOALbHTML = DFCOALb.to_html()
    if DFCOALb.empty == False:
        DFCOALb = DFCOALb.to_json(orient="index",date_format="iso")
        container_clientCOAL = blob_service_client.get_container_client("90coalevent")
        blob_client = container_clientCOAL.get_blob_client(eventNameb)
        container_clientCOAL= blob_client.upload_blob(DFCOALb,overwrite=True)
        
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFCOALb,overwrite=True)
        
        
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFCOALbHTML,overwrite=True)
###########################################################################################
    DFGAS = DfChanges[["Gas"]]
    
    DFGAS = DFGAS.loc[DFGAS[DFGAS >= 100].any(axis=1)]
    DFGASHTML = DFGAS.to_html()
    if DFGAS.empty == False:
        DFGAS = DFGAS.to_json(orient="index",date_format="iso")
        container_clientGAS = blob_service_client.get_container_client("90gasevent")
        blob_client = container_clientGAS.get_blob_client(eventName)
        container_clientGAS= blob_client.upload_blob(DFGAS,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFGAS,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFGASHTML,overwrite=True)
        
        
    DFGASb = DfChanges[["Gas"]]
    
    DFGASb = DFGASb.loc[DFGASb[DFGASb <= -100].any(axis=1)]
    DFGASBHTML = DFGASb.to_html()
    if DFGASb.empty == False:
        DFGASb = DFGASb.to_json(orient="index",date_format="iso")
        container_clientGAS = blob_service_client.get_container_client("90gasevent")
        blob_client = container_clientGAS.get_blob_client(eventNameb)
        container_clientGAS= blob_client.upload_blob(DFGASb,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFGASb,overwrite=True)    
        

        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFGASBHTML,overwrite=True)
        
        
        
###########################################################################################
    DFDUAL = DfChanges[["DualFuel"]]
    
    DFDUAL = DFDUAL.loc[DFDUAL[DFDUAL >= 100].any(axis=1)]
    DFDUALHTML = DFDUAL.to_html()
    if DFDUAL.empty == False:
        DFDUAL = DFDUAL.to_json(orient="index",date_format="iso")
        container_clientDUAL = blob_service_client.get_container_client("90dualevent")
        blob_client = container_clientDUAL.get_blob_client(eventName)
        container_clientDUAL= blob_client.upload_blob(DFDUAL,overwrite=True)
                
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFDUAL,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFDUALHTML,overwrite=True)   
        
        
        
    DFDUALb = DfChanges[["DualFuel"]]
    
    DFDUALb = DFDUALb.loc[DFDUALb[DFDUALb <= -100].any(axis=1)]
    DFDUALBHTML = DFDUALb.to_html()
    if DFDUALb.empty == False:
        DFDUALb = DFDUALb.to_json(orient="index",date_format="iso")
        container_clientDUAL = blob_service_client.get_container_client("90dualevent")
        blob_client = container_clientDUAL.get_blob_client(eventNameb)
        container_clientDUAL= blob_client.upload_blob(DFDUALb,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFDUALb,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFDUALBHTML,overwrite=True)  
###########################################################################################

    DFHYDRO = DfChanges[["Hydro"]]
    
    DFHYDRO = DFHYDRO.loc[DFHYDRO[DFHYDRO >= 100].any(axis=1)]
    DFHYDROHTML = DFHYDRO.to_html()
    if DFHYDRO.empty == False:
        DFHYDRO = DFHYDRO.to_json(orient="index",date_format="iso")
        container_clientHYDRO = blob_service_client.get_container_client("90hydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventName)
        container_clientHYDRO= blob_client.upload_blob(DFHYDRO,overwrite=True)
    
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFHYDRO,overwrite=True)     
                
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFHYDROHTML,overwrite=True)   

    DFHYDROb = DfChanges[["Hydro"]]
    
    DFHYDROb = DFHYDROb.loc[DFHYDROb[DFHYDROb <= -100].any(axis=1)]
    DFHYDRObHTML = DFHYDROb.to_html()
    if DFHYDROb.empty == False:
        DFHYDROb = DFHYDROb.to_json(orient="index",date_format="iso")
        container_clientHYDRO = blob_service_client.get_container_client("90hydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventNameb)
        container_clientHYDRO= blob_client.upload_blob(DFHYDROb,overwrite=True)
        
            
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFHYDROb,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFHYDRObHTML,overwrite=True) 
###########################################################################################
    DFOTHER = DfChanges[["Other"]]
    DFOTHER = DFOTHER.loc[DFOTHER[DFOTHER >= 100].any(axis=1)]
    DFOTHERHTML = DFOTHER.to_html()
    if DFOTHER.empty == False:
        DFOTHER = DFOTHER.to_json(orient="index",date_format="iso")
        container_clientOTHER = blob_service_client.get_container_client("90otherevent")
        blob_client = container_clientOTHER.get_blob_client(eventName)
        container_clientOTHER= blob_client.upload_blob(DFOTHER,overwrite=True)
        
        
                    
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFOTHER,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFOTHERHTML,overwrite=True) 
        
        
    DFOTHERb = DfChanges[["Other"]]
    DFOTHERb = DFOTHERb.loc[DFOTHERb[DFOTHERb <= -100].any(axis=1)]
    DFOTHERbHTML = DFOTHERb.to_html()
    if DFOTHERb.empty == False:
        DFOTHERb = DFOTHERb.to_json(orient="index",date_format="iso")
        container_clientOTHER = blob_service_client.get_container_client("90otherevent")
        blob_client = container_clientOTHER.get_blob_client(eventNameb)
        container_clientOTHER= blob_client.upload_blob(DFOTHERb,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFOTHERb,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFOTHERbHTML,overwrite=True) 
        

###########################################################################################

    DFSOLAR = DfChanges[["Solar"]]
    DFSOLAR = DFSOLAR.loc[DFSOLAR[DFSOLAR >= 100].any(axis=1)]
    DFSOLARHTML = DFSOLAR.to_html()
    if DFSOLAR.empty == False:
        DFSOLAR = DFSOLAR.to_json(orient="index",date_format="iso")
        container_clientSOLAR = blob_service_client.get_container_client("90solarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventName)
        container_clientSOLAR= blob_client.upload_blob(DFSOLAR,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSOLAR,overwrite=True) 
            
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFSOLARHTML,overwrite=True) 
        
        
    DFSOLARb = DfChanges[["Solar"]]
    DFSOLARb = DFSOLARb.loc[DFSOLARb[DFSOLARb <= -100].any(axis=1)]
    DFSOLARbHTML = DFSOLARb.to_html()
    if DFSOLARb.empty == False:
        DFSOLARb = DFSOLARb.to_json(orient="index",date_format="iso")
        container_clientSOLAR = blob_service_client.get_container_client("90solarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventNameb)
        container_clientSOLAR= blob_client.upload_blob(DFSOLARb,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSOLARb,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFSOLARbHTML,overwrite=True) 
        
###########################################################################################

    DFWIND = DfChanges[["Wind"]]
    DFWIND = DFWIND.loc[DFWIND[DFWIND >= 100].any(axis=1)]
    DFWINDHTML = DFWIND.to_html()
    if DFWIND.empty == False:
        DFWIND = DFWIND.to_json(orient="index",date_format="iso")
        container_clientWIND = blob_service_client.get_container_client("90windevent")
        blob_client = container_clientWIND.get_blob_client(eventName)
        container_clientWIND= blob_client.upload_blob(DFWIND,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFWIND,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFWINDHTML,overwrite=True) 
        
    DFWINDb = DfChanges[["Wind"]]
    DFWINDb = DFWINDb.loc[DFWINDb[DFWINDb <= -100].any(axis=1)]
    DFWINDbHTML = DFWINDb.to_html()
    if DFWINDb.empty == False:
        DFWINDb = DFWINDb.to_json(orient="index",date_format="iso")
        container_clientWIND = blob_service_client.get_container_client("90windevent")
        blob_client = container_clientWIND.get_blob_client(eventNameb)
        container_clientWIND= blob_client.upload_blob(DFWINDb,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFWINDb,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFWINDbHTML,overwrite=True) 
        
###########################################################################################

    DFSTORAGE = DfChanges[["Energy"]]
    DFSTORAGE = DFSTORAGE.loc[DFSTORAGE[DFSTORAGE >= 100].any(axis=1)]
    DFSTORAGEHTML = DFSTORAGE.to_html()
    if DFSTORAGE.empty == False:
        DFSTORAGE = DFSTORAGE.to_json(orient="index",date_format="iso")
        container_clientSTORAGE = blob_service_client.get_container_client("90storageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventName)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGE,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSTORAGE,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTML)
        container_clientHTML= blob_client.upload_blob(DFSTORAGEHTML,overwrite=True) 
        

    DFSTORAGEb = DfChanges[["Energy"]]
    DFSTORAGEb = DFSTORAGEb.loc[DFSTORAGEb[DFSTORAGEb <= -100].any(axis=1)]
    DFSTORAGEbHTML = DFSTORAGEb.to_html()
    if DFSTORAGEb.empty == False:
        DFSTORAGEb = DFSTORAGEb.to_json(orient="index",date_format="iso")
        container_clientSTORAGE = blob_service_client.get_container_client("90storageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventNameb)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGEb,overwrite=True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("dailyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSTORAGEb,overwrite=True) 
        
        container_clientHTML = blob_service_client.get_container_client("90html")
        blob_client = container_clientHTML.get_blob_client(eventnameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFSTORAGEbHTML,overwrite=True)
        
# ###########################################################################################
    
    ##Upload the drone file and replace the master file.
    eventName = "90dayMaster.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("90daymaster")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(dfMasterDrone,overwrite=True)

    