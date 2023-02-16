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
    eventNameb = now + "b.json"
    eventNameHTML = now + ".html"
    eventNameHTMLb = now + "b.html"

    ##Rinse and Repeat for monthly outages. 
    DFCOAL = DfChanges[["Coal"]]
    DFCOAL = DFCOAL.loc[DFCOAL[DFCOAL >= 30].any(axis=1)]
    DFCOALHTML = DFCOAL.to_html()

    if DFCOAL.empty == False:
        
        DFCOAL = DFCOAL.to_json(orient="index",date_format="iso")
        container_clientCOAL = blob_service_client.get_container_client("monthlycoalevent")
        blob_client = container_clientCOAL.get_blob_client(eventName)
        container_clientCOAL= blob_client.upload_blob(DFCOAL)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFCOAL,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFCOALHTML,overwrite=True) 
        
    DFCOALa = DfChanges[["Coal"]]
    DFCOALa = DFCOALa.loc[DFCOALa[DFCOALa <= -30].any(axis=1)]
    DFCOALaHTML = DFCOALa.to_html()
    if DFCOALa.empty == False:
        DFCOALa = DFCOALa.to_json(orient="index",date_format="iso")
        container_clientCOAL = blob_service_client.get_container_client("monthlycoalevent")
        blob_client = container_clientCOAL.get_blob_client(eventNameb)
        container_clientCOAL= blob_client.upload_blob(DFCOALa)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFCOALa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFCOALaHTML,overwrite=True) 
        
###########################################################################################
    DFGAS = DfChanges[["Gas"]]
    DFGAS = DFGAS.loc[DFGAS[DFGAS >= 30].any(axis=1)]
    DFGASHTML = DFGAS.to_html()
    if DFGAS.empty == False:
        DFGAS = DFGAS.to_json(orient="index",date_format="iso")
        container_clientGAS = blob_service_client.get_container_client("monthlygasevent")
        blob_client = container_clientGAS.get_blob_client(eventName)
        container_clientGAS= blob_client.upload_blob(DFGAS)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFGAS,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFGASHTML,overwrite=True) 
        
    DFGASa = DfChanges[["Gas"]]
    DFGASa = DFGASa.loc[DFGASa[DFGASa <= -30].any(axis=1)]
    DFGASaHTML = DFGASa.to_html()
    if DFGASa.empty == False:
        DFGASa = DFGASa.to_json(orient="index",date_format="iso")
        container_clientGAS = blob_service_client.get_container_client("monthlygasevent")
        blob_client = container_clientGAS.get_blob_client(eventNameb)
        container_clientGAS= blob_client.upload_blob(DFGASa)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFGASa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFGASaHTML,overwrite=True)     

###########################################################################################
    DFDUAL = DfChanges[["DualFuel"]]
    DFDUAL = DFDUAL.loc[DFDUAL[DFDUAL >= 30].any(axis=1)]
    DFDUALHTML = DFDUAL.to_html()
    if DFDUAL.empty == False:
        DFDUAL = DFDUAL.to_json(orient="index",date_format="iso")
        container_clientDUAL = blob_service_client.get_container_client("monthlydualevent")
        blob_client = container_clientDUAL.get_blob_client(eventName)
        container_clientDUAL= blob_client.upload_blob(DFDUAL)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFDUAL,overwrite=True)
        
                
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameb)
        container_clientHTML= blob_client.upload_blob(DFDUALHTML,overwrite=True) 
        
        
    DFDUALa = DfChanges[["DualFuel"]]
    DFDUALa = DFDUALa.loc[DFDUALa[DFDUALa <= -30].any(axis=1)]
    DFDUALHTML = DFDUALa.to_html()
    if DFDUALa.empty == False:
        DFDUALa = DFDUALa.to_json(orient="index",date_format="iso")
        container_clientDUAL = blob_service_client.get_container_client("monthlydualevent")
        blob_client = container_clientDUAL.get_blob_client(eventNameb)
        container_clientDUAL= blob_client.upload_blob(DFDUALa)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFDUALa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFDUALHTML,overwrite=True) 
###########################################################################################

    DFHYDRO = DfChanges[["Hydro"]]
    DFHYDRO = DFHYDRO.loc[DFHYDRO[DFHYDRO >= 50].any(axis=1)]
    DFHYDROHTML = DFHYDRO.to_html()
    if DFHYDRO.empty == False:
        
        DFHYDRO = DFHYDRO.to_json(orient="index",date_format="iso")
        container_clientHYDRO = blob_service_client.get_container_client("monthlyhydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventName)
        container_clientHYDRO= blob_client.upload_blob(DFHYDRO)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFHYDRO,overwrite=True)
        
                
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFHYDROHTML,overwrite=True)
             
    DFHYDROa = DfChanges[["Hydro"]]
    DFHYDROa = DFHYDROa.loc[DFHYDROa[DFHYDROa <= -50].any(axis=1)]
    DFHYDROaHTML = DFHYDROa.to_html()
    if DFHYDROa.empty == False:
        DFHYDROa = DFHYDROa.to_json(orient="index",date_format="iso")
        container_clientHYDRO = blob_service_client.get_container_client("monthlyhydroevent")
        blob_client = container_clientHYDRO.get_blob_client(eventNameb)
        container_clientHYDRO= blob_client.upload_blob(DFHYDROa)
                
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFHYDROa,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFHYDROaHTML,overwrite=True)     

###########################################################################################
    DFOTHER = DfChanges[["Other"]]
    DFOTHER = DFOTHER.loc[DFOTHER[DFOTHER >= 50].any(axis=1)]
    DFOTHERHTML = DFOTHER.to_html()
    if DFOTHER.empty == False:
        DFOTHER = DFOTHER.to_json(orient="index",date_format="iso")
        container_clientOTHER = blob_service_client.get_container_client("monthlyotherevent")
        blob_client = container_clientOTHER.get_blob_client(eventName)
        container_clientOTHER= blob_client.upload_blob(DFOTHER)
        
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFOTHER,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFOTHERHTML,overwrite=True)     
        
    DFOTHERa = DfChanges[["Other"]]
    DFOTHERa = DFOTHERa.loc[DFOTHERa[DFOTHERa <= -50].any(axis=1)]
    DFOTHERHTMLa = DFOTHERa.to_html()
    if DFOTHERa.empty == False:
        DFOTHERa = DFOTHERa.to_json(orient="index",date_format="iso")
        container_clientOTHER = blob_service_client.get_container_client("monthlyotherevent")
        blob_client = container_clientOTHER.get_blob_client(eventNameb)
        container_clientOTHER= blob_client.upload_blob(DFOTHERa)
        
                
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFOTHERa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFOTHERHTMLa,overwrite=True)     
        

###########################################################################################

    DFSOLAR = DfChanges[["Solar"]]
    DFSOLAR = DFSOLAR.loc[DFSOLAR[DFSOLAR >= 50].any(axis=1)]
    DFSOLARHTML = DFSOLAR.to_html()
    if DFSOLAR.empty == False:
        DFSOLAR = DFSOLAR.to_json(orient="index",date_format="iso")
        container_clientSOLAR = blob_service_client.get_container_client("monthlysolarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventName)
        container_clientSOLAR= blob_client.upload_blob(DFSOLAR)
                        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSOLAR,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFSOLARHTML,overwrite=True)     
       
    DFSOLARa = DfChanges[["Solar"]]
    DFSOLARa = DFSOLARa.loc[DFSOLARa[DFSOLARa <= -50].any(axis=1)]
    DFSOLARaHTML = DFSOLARa.to_html()
    if DFSOLARa.empty == False:
        DFSOLARa = DFSOLARa.to_json(orient="index",date_format="iso")
        container_clientSOLAR = blob_service_client.get_container_client("monthlysolarevent")
        blob_client = container_clientSOLAR.get_blob_client(eventNameb)
        container_clientSOLAR= blob_client.upload_blob(DFSOLARa)
        
                                
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSOLARa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFSOLARaHTML,overwrite=True)     
        
###########################################################################################

    DFWIND = DfChanges[["Wind"]]
    DFWIND = DFWIND.loc[DFWIND[DFWIND >= 50].any(axis=1)]
    DFWINDHTML = DFWIND.to_html()
    if DFWIND.empty == False:
        DFWIND = DFWIND.to_json(orient="index",date_format="iso")
        container_clientWIND = blob_service_client.get_container_client("monthlywindevent")
        blob_client = container_clientWIND.get_blob_client(eventName)
        container_clientWIND= blob_client.upload_blob(DFWIND)
        
        
                                        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFWIND,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFWINDHTML,overwrite=True)     

    DFWINDa = DfChanges[["Wind"]]
    DFWINDa = DFWINDa.loc[DFWINDa[DFWINDa <= -50].any(axis=1)]
    DFWINDHTMLa = DFWINDa.to_html()
    if DFWINDa.empty == False:
        DFWINDa = DFWINDa.to_json(orient="index",date_format="iso")
        container_clientWIND = blob_service_client.get_container_client("monthlywindevent")
        blob_client = container_clientWIND.get_blob_client(eventNameb)
        container_clientWIND= blob_client.upload_blob(DFWINDa)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFWINDa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFWINDHTMLa,overwrite=True)      
###########################################################################################

    DFSTORAGE = DfChanges[["Energy"]]
    DFSTORAGE = DFSTORAGE.loc[DFSTORAGE[DFSTORAGE >= 50].any(axis=1)]
    DFSTORAGEHTML = DFSTORAGE.to_html()
    if DFSTORAGE.empty == False:
        DFSTORAGE = DFSTORAGE.to_json(orient="index",date_format="iso")
        container_clientSTORAGE = blob_service_client.get_container_client("monthlystorageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventName)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGE)
        
                
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSTORAGE,overwrite=True)
        
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(DFSTORAGEHTML,overwrite=True)         
        
        
    DFSTORAGEa = DfChanges[["Energy"]]
    DFSTORAGEa = DFSTORAGEa.loc[DFSTORAGEa[DFSTORAGEa <= -50].any(axis=1)]
    DFSTORAGEHTMLa = DFSTORAGEa.to_html()
    if DFSTORAGEa.empty == False:
        DFSTORAGEa = DFSTORAGEa.to_json(orient="index",date_format="iso")
        container_clientSTORAGE = blob_service_client.get_container_client("monthlystorageevent")
        blob_client = container_clientSTORAGE.get_blob_client(eventNameb)
        container_clientSTORAGE= blob_client.upload_blob(DFSTORAGEa)
        
                        
        container_clientCOAL1 = blob_service_client1.get_container_client("monthlyoutage")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(DFSTORAGEa,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("monthhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(DFSTORAGEHTMLa,overwrite=True)     
# ###########################################################################################
#     ## Overwrite master file with drone.
    eventName = "monthlymaster.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("monthlymaster")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(dfMasterDrone,overwrite=True)
    
    
    