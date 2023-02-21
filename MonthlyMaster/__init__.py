from datetime import datetime
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient


def main(mytimer: func.TimerRequest) -> None:
    now = datetime.now().strftime("%Y%m%d-%H%M%S")

    #eventName = now + ".csv"
    
    ##Load monthlydrone file for change detection. Push same version of drone into dfAlldrone
    ##for loading into master file after change analysis is finished.
    eventName = "monthlydrone.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("monthlydrone")
    blob_client = container_client.get_blob_client('monthlydrone.csv')
    dfAlldrone = blob_client.download_blob()
    dfAlldrone = pd.read_csv(dfAlldrone)
    dfMasterDrone = dfAlldrone.to_csv()
    blob_service_client1 = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=outageblobspremium;AccountKey=hYBKzIYX0cNoB//eU5OqR1m3AFiB3dEOqnI+BDUv94CQZ5Ep1fHEeMsczoJPQuIXRnkOIgxKSAXO+AStyiep+A==;EndpointSuffix=core.windows.net")
    ## All into numeric as it's strings. Except 'Month'. Push back into
    ## datetime and set as index
    dfAlldrone['Coal'] = pd.to_numeric(dfAlldrone['Coal'])
    dfAlldrone['Gas'] = pd.to_numeric(dfAlldrone['Gas'])
    dfAlldrone['DualFuel'] = pd.to_numeric(dfAlldrone['Dual Fuel'])
    dfAlldrone['Hydro'] = pd.to_numeric(dfAlldrone['Hydro'])
    dfAlldrone['Wind'] = pd.to_numeric(dfAlldrone['Wind'])
    dfAlldrone['Solar'] = pd.to_numeric(dfAlldrone['Solar'])
    dfAlldrone['Energy'] = pd.to_numeric(dfAlldrone['Energy Storage'])
    dfAlldrone['Other'] = pd.to_numeric(dfAlldrone['Biomass and Other'])
    dfAlldrone['Month'] = dfAlldrone['Month'].astype('string')
    dfAlldrone['Month'] = pd.to_datetime(dfAlldrone['Month'])
    
    dfAlldrone= dfAlldrone.set_index('Month',drop=True)
    print(dfAlldrone)
#     ## Do the same for the master file so comparison is apples to apples.
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientMaster = blob_service_client.get_container_client("monthlymaster")
    blob_client = container_clientMaster.get_blob_client('monthlymaster.csv')
    dfMaster = blob_client.download_blob()
    dfMaster = pd.read_csv(dfMaster)
    dfMaster['Coal'] = pd.to_numeric(dfMaster['Coal'])
    dfMaster['Gas'] = pd.to_numeric(dfMaster['Gas'])
    dfMaster['DualFuel'] = pd.to_numeric(dfMaster['Dual Fuel'])
    dfMaster['Hydro'] = pd.to_numeric(dfMaster['Hydro'])
    dfMaster['Wind'] = pd.to_numeric(dfMaster['Wind'])
    dfMaster['Solar'] = pd.to_numeric(dfMaster['Solar'])
    dfMaster['Energy'] = pd.to_numeric(dfMaster['Energy Storage'])
    dfMaster['Other'] = pd.to_numeric(dfMaster['Biomass and Other'])
    dfMaster['Month'] = dfMaster['Month'].astype('string')
    dfMaster['Month'] = pd.to_datetime(dfMaster['Month'])
    dfMaster= dfMaster.set_index('Month',drop=True)

    print(dfMaster)
    ## Subtract files (drone - master) so that files are negative for returning outages, postive 
    ## for new outages 
    DfChanges = dfAlldrone - dfMaster
    print(DfChanges)
    now = datetime.now().strftime("%Y%m%d-%H%M%S")

    eventName = now + ".json"

    eventNameHTML = now + ".html"


    ##Rinse and Repeat for monthly outages. 
    DFCOAL = DfChanges[["Coal"]]
    DFCOAL = DFCOAL.loc[DFCOAL[DFCOAL >= 30].any(axis=1) | (DFCOAL[DFCOAL <= -30].any(axis=1))]
    
    
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
        container_clientCOAL = blob_service_client.get_container_client("monthlycoalevent")
        blob_client = container_clientCOAL.get_blob_client(eventName)
        container_clientCOAL= blob_client.upload_blob(DFCOAL)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(datacoal,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFCOALHTML,overwrite=True) 
        

        
###########################################################################################
    DFGAS = DfChanges[["Gas"]]
    DFGAS = DFGAS.loc[DFGAS[DFGAS >= 50].any(axis=1) | (DFGAS[DFGAS <= -50].any(axis=1))]
    
        
    DFGASJSON = DFGAS.reset_index()
    type = DFGASJSON.columns[1]
    DFGASJSON = {
        'type' : type,
        'dates' : {row['Date']: row[type] for _, row in DFGAS.iterrows()}
    }
    DFGASJSON = pd.DataFrame(data=DFGASJSON)
    DFGASJSON = DFGASJSON.to_json()
    DFGASHTML = DFGAS.to_html()
    if DFGAS.empty == False:
        DFGAS = DFGAS.to_json(orient="index",date_format="iso")
        container_clientGAS = blob_service_client.get_container_client("monthlygasevent")
        blob_client = container_clientGAS.get_blob_client(eventName)
        container_clientGAS= blob_client.upload_blob(DFGAS)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFGASJSON,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
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
        container_clientDUAL = blob_service_client.get_container_client("monthlydualevent")
        blob_client = container_clientDUAL.get_blob_client(eventName)
        container_clientDUAL= blob_client.upload_blob(DFDUAL)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFDUALJSON,overwrite=True)
        
                
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventName)
        container_clientHTML= blob_client.upload_blob(DFDUALHTML,overwrite=True) 
        
        
 
###########################################################################################
    DFHYDRO = DfChanges[["Hydro"]]
    
    DFHYDRO = DFHYDRO.loc[DFHYDRO[DFHYDRO >= 50].any(axis=1) | (DFHYDRO[DFHYDRO <= -50].any(axis=1))]
    
        
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
        container_clientHYDRO = blob_service_client.get_container_client("monthlyhydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventName)
        container_clientHYDRO= blob_client.upload_blob(DFHYDRO)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFHYDROJSON,overwrite=True)
        
                
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFHYDROHTML,overwrite=True)
             


###########################################################################################
    DFOTHER = DfChanges[["Other"]]
    DFOTHER = DFOTHER.loc[DFOTHER[DFOTHER >= 50].any(axis=1) | (DFOTHER[DFOTHER <= -50].any(axis=1))]
    
        
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
        container_clientOTHER = blob_service_client.get_container_client("monthlyotherevent")
        blob_client = container_clientOTHER.get_blob_client(eventName)
        container_clientOTHER= blob_client.upload_blob(DFOTHER)
        
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFOTHERJSON,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
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
        container_clientSOLAR = blob_service_client.get_container_client("monthlysolarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventName)
        container_clientSOLAR= blob_client.upload_blob(DFSOLAR)
                        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSOLARJSON,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
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
        container_clientWIND = blob_service_client.get_container_client("monthlywindevent")
        blob_client = container_clientWIND.get_blob_client(eventName)
        container_clientWIND= blob_client.upload_blob(DFWIND)
        
        
                                        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFWINDJSON,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
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
        container_clientSTORAGE = blob_service_client.get_container_client("monthlystorageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventName)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGE)
        
                
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSTORAGEJSON,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFSTORAGEHTML,overwrite=True)         
        
        

# ###########################################################################################
#     ## Overwrite master file with drone.
    eventName = "monthlymaster.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("monthlymaster")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(dfMasterDrone,overwrite=True)
    
    
    