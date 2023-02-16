
import logging
from datetime import datetime
import logging
import requests
import pandas as pd
import re

from io import StringIO
from typing import BinaryIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient

async def main(mytimer: func.TimerRequest):
    
    ##A little unwieldly due to the formatting issues from 7 day web scrape
    ##This file will just take each blob one at a time.
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientCoal = blob_service_client.get_container_client("outcoal7day")
    container_clientGas = blob_service_client.get_container_client("outgas7day")
    container_clientDual = blob_service_client.get_container_client("outdual7day")
    container_clientHydro = blob_service_client.get_container_client("outhydro7day")
    container_clientSolar = blob_service_client.get_container_client("outsolar7day")
    container_clientWind = blob_service_client.get_container_client("outwind7day")
    container_clientStorage = blob_service_client.get_container_client("outstorage7day")
    container_clientOther = blob_service_client.get_container_client("outother7day")

    container_clientCoalMaster = blob_service_client.get_container_client("mastercoal")
    container_clientGasMaster = blob_service_client.get_container_client("mastergas")
    container_clientDualMaster = blob_service_client.get_container_client("masterdual")
    container_clientHydroMaster = blob_service_client.get_container_client("masterhydro")
    container_clientSolarMaster = blob_service_client.get_container_client("mastersolar")
    container_clientWindMaster = blob_service_client.get_container_client("masterwind")
    container_clientStorageMaster = blob_service_client.get_container_client("masterstorage")
    container_clientOtherMaster = blob_service_client.get_container_client("masterother")  

    blob_service_client1 = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=outageblobspremium;AccountKey=hYBKzIYX0cNoB//eU5OqR1m3AFiB3dEOqnI+BDUv94CQZ5Ep1fHEeMsczoJPQuIXRnkOIgxKSAXO+AStyiep+A==;EndpointSuffix=core.windows.net")

    #Starting with coal, download drone blob, read csv, re-index and reset data types.
    ## Also get rid of MC and the Unnamed column. (Results from not importing an index)
    blob_client = container_clientCoal.get_blob_client('coal.csv')
    dfCoal = blob_client.download_blob()
    dfCoal = pd.read_csv(dfCoal)

    dfCoalstagnant = dfCoal.to_csv()
    dfCoal["Date"] = pd.to_datetime(dfCoal["Date"])
    dfCoal['Type'] = dfCoal['Type'].astype('string')
    dfCoal = dfCoal.set_index(['Date','Type'])
    dfCoal = dfCoal.drop(['Unnamed: 0'],axis=1)
    dfCoal = dfCoal.drop(['MC'],axis=1)



    ## Download previous master Coal file, and do the same for apples to apples 
    ## comparison. 
    blob_client = container_clientCoalMaster.get_blob_client('coalMaster.csv')
    dfCoalMaster = blob_client.download_blob()
    dfCoalMaster = pd.read_csv(dfCoalMaster)


    dfCoalMaster["Date"] = pd.to_datetime(dfCoalMaster["Date"])
    dfCoalMaster['Type'] = dfCoalMaster['Type'].astype('string')
    dfCoalMaster = dfCoalMaster.set_index(['Date','Type'])
    dfCoalMaster = dfCoalMaster.drop(['Unnamed: 0'],axis=1)
    dfCoalMaster = dfCoalMaster.drop(['MC'],axis=1)

    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    ##Events are labelled by datetime and concatted with .csv
    eventName = now + ".json"
    eventNameb = now + "b.json"
    eventNameHTML = now + ".html"
    eventNameHTMLb = now + "b.html"
    ##Below is the same thing over and over again. Take comparison between drone and 
    ##master file and subtract master from drone.
    
    ## Then look for negative case (master - drone) = negative number which indicates
    ## a unit coming back. 
    
    ## Upload the drone to the master file.
#########################################################################################
    dfCoalResult = dfCoalMaster - dfCoal
    dfCoalResult = dfCoalResult.loc[dfCoalResult[dfCoalResult >= 100].any(axis=1)]
    dfCoalResult = dfCoalResult.drop('Unnamed: 0.1', axis=1)
    
    dfCoalResult=dfCoalResult.reset_index()
    dfCoalResult = pd.melt(dfCoalResult, id_vars=["Date","Type"], value_vars=list(dfCoalResult.columns[1:]))
    dfCoalResult["Date_Time"] = dfCoalResult["Date"].astype("string") + " " + dfCoalResult["variable"]
    dfCoalResult=dfCoalResult.drop(["variable","Date"],axis=1)
    dfCoalResult=dfCoalResult.set_index(['Date_Time'])
    dfCoalResult=dfCoalResult.sort_index()
    dfCoalResult['value'] = dfCoalResult['value'].astype('int')
    
    
    
    
    
    dfCoalResultHTML = dfCoalResult.to_html()
    if dfCoalResult.empty == False:
        dfCoalResult['Type'] = dfCoalResult['Type'].str.strip()
        dfCoalResult = dfCoalResult.to_json(orient="index",indent=1)
        
        container_clientCoal = blob_service_client.get_container_client("coalevents")
        blob_client = container_clientCoal.get_blob_client(eventName)
        container_clientCoal= blob_client.upload_blob(dfCoalResult,overwrite= True)
        
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfCoalResult,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfCoalResultHTML,overwrite=True) 
        
    dfCoalResulta = dfCoalMaster - dfCoal
    dfCoalResulta = dfCoalResulta.loc[dfCoalResulta[dfCoalResulta <= -100].any(axis=1)]
    dfCoalResulta = dfCoalResulta.drop('Unnamed: 0.1', axis=1)
    
    
    dfCoalResulta=dfCoalResulta.reset_index()
    dfCoalResulta = pd.melt(dfCoalResulta, id_vars=["Date","Type"], value_vars=list(dfCoalResulta.columns[1:]))
    dfCoalResulta["Date_Time"] = dfCoalResulta["Date"].astype("string") + " " + dfCoalResulta["variable"]
    dfCoalResulta=dfCoalResulta.drop(["variable","Date"],axis=1)
    dfCoalResulta=dfCoalResulta.set_index(['Date_Time'])
    dfCoalResulta=dfCoalResulta.sort_index()
    dfCoalResulta['value'] = dfCoalResulta['value'].astype('int')
    dfCoalResultaHTML = dfCoalResulta.to_html()
    if dfCoalResulta.empty == False:
        dfCoalResulta['Type'] = dfCoalResulta['Type'].str.strip()
        dfCoalResulta = dfCoalResulta.to_json(orient="index",indent=1)
        
        container_clientCoal = blob_service_client.get_container_client("coalevents")
        blob_client = container_clientCoal.get_blob_client(eventNameb)
        container_clientCoal= blob_client.upload_blob(dfCoalResulta,overwrite= True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfCoalResulta,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfCoalResultaHTML,overwrite=True) 

    container_clientCoal = blob_service_client.get_container_client("mastercoal")
    blob_client = container_clientCoal.get_blob_client("coalMaster.csv")
    container_clientCoal= blob_client.upload_blob(dfCoalstagnant,overwrite=True)


# #############################################################################################
    blob_client = container_clientGas.get_blob_client('gas.csv')
    dfgas = blob_client.download_blob()
    dfgas = pd.read_csv(dfgas)

    dfgasstagnant = dfgas.to_csv()
    dfgas["Date"] = pd.to_datetime(dfgas["Date"])
    dfgas['Type'] = dfgas['Type'].astype('string')
    dfgas = dfgas.set_index(['Date','Type'])
    dfgas = dfgas.drop(['Unnamed: 0'],axis=1)
    dfgas = dfgas.drop(['MC'],axis=1)




    blob_client = container_clientGasMaster.get_blob_client('gasMaster.csv')
    dfgasMaster = blob_client.download_blob()
    dfgasMaster = pd.read_csv(dfgasMaster)


    dfgasMaster["Date"] = pd.to_datetime(dfgasMaster["Date"])
    dfgasMaster['Type'] = dfgasMaster['Type'].astype('string')
    dfgasMaster = dfgasMaster.set_index(['Date','Type'])
    dfgasMaster = dfgasMaster.drop(['Unnamed: 0'],axis=1)
    dfgasMaster = dfgasMaster.drop(['MC'],axis=1)



    dfgasResult = dfgasMaster - dfgas
    dfgasResult = dfgasResult.loc[dfgasResult[dfgasResult >= 100].any(axis=1)]
    dfgasResult = dfgasResult.drop('Unnamed: 0.1', axis=1)
    
    dfgasResult=dfgasResult.reset_index()
    dfgasResult = pd.melt(dfgasResult, id_vars=["Date","Type"], value_vars=list(dfgasResult.columns[1:]))
    dfgasResult["Date_Time"] = dfgasResult["Date"].astype("string") + " " + dfgasResult["variable"]
    dfgasResult=dfgasResult.drop(["variable","Date"],axis=1)
    dfgasResult=dfgasResult.set_index(['Date_Time'])
    dfgasResult=dfgasResult.sort_index()
    dfgasResult['value'] = dfgasResult['value'].astype('int')    
    
    dfgasResultHTML = dfgasResult.to_html()
    if dfgasResult.empty == False:
        dfgasResult['Type'] = dfgasResult['Type'].str.strip()
        dfgasResult = dfgasResult.to_json(orient="index",indent=1)
        container_clientgas = blob_service_client.get_container_client("gasevents")
        blob_client = container_clientgas.get_blob_client(eventName)
        container_clientgas= blob_client.upload_blob(dfgasResult,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfgasResult,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfgasResultHTML,overwrite=True) 
        
    dfgasResulta = dfgasMaster - dfgas
    dfgasResulta = dfgasResulta.loc[dfgasResulta[dfgasResulta <= -100].any(axis=1)]
    dfgasResulta = dfgasResulta.drop('Unnamed: 0.1', axis=1)
    
    dfgasResulta=dfgasResulta.reset_index()
    dfgasResulta = pd.melt(dfgasResulta, id_vars=["Date","Type"], value_vars=list(dfgasResulta.columns[1:]))
    dfgasResulta["Date_Time"] = dfgasResulta["Date"].astype("string") + " " + dfgasResulta["variable"]
    dfgasResulta=dfgasResulta.drop(["variable","Date"],axis=1)
    dfgasResulta=dfgasResulta.set_index(['Date_Time'])
    dfgasResulta=dfgasResulta.sort_index()
    dfgasResulta['value'] = dfgasResulta['value'].astype('int')   
    dfgasResultHTMLa = dfgasResulta.to_html()
    if dfgasResulta.empty == False:
        dfgasResulta['Type'] = dfgasResulta['Type'].str.strip()
        dfgasResulta = dfgasResulta.to_json(orient="index",indent=1)        
        container_clientgas = blob_service_client.get_container_client("gasevents")
        blob_client = container_clientgas.get_blob_client(eventNameb)
        container_clientgas= blob_client.upload_blob(dfgasResulta,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfgasResulta,overwrite=True)
    
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfgasResultHTMLa,overwrite=True)
         
    container_clientgas = blob_service_client.get_container_client("mastergas")
    blob_client = container_clientgas.get_blob_client("gasMaster.csv")
    container_clientgas= blob_client.upload_blob(dfgasstagnant,overwrite=True)



#####################################################################################
 
    blob_client = container_clientDual.get_blob_client('dual.csv')
    dfdual = blob_client.download_blob()
    dfdual = pd.read_csv(dfdual)

    dfdualstagnant = dfdual.to_csv()
    dfdual["Date"] = pd.to_datetime(dfdual["Date"])
    dfdual['Type'] = dfdual['Type'].astype('string')
    dfdual = dfdual.set_index(['Date','Type'])
    dfdual = dfdual.drop(['Unnamed: 0'],axis=1)
    dfdual = dfdual.drop(['MC'],axis=1)




    blob_client = container_clientDualMaster.get_blob_client('dualMaster.csv')
    dfdualMaster = blob_client.download_blob()
    dfdualMaster = pd.read_csv(dfdualMaster)


    dfdualMaster["Date"] = pd.to_datetime(dfdualMaster["Date"])
    dfdualMaster['Type'] = dfdualMaster['Type'].astype('string')
    dfdualMaster = dfdualMaster.set_index(['Date','Type'])
    dfdualMaster = dfdualMaster.drop(['Unnamed: 0'],axis=1)
    dfdualMaster = dfdualMaster.drop(['MC'],axis=1)
    dfdualResult = dfdualMaster - dfdual
    dfdualResult = dfdualResult.loc[dfdualResult[dfdualResult >= 100].any(axis=1)]
    dfdualResult = dfdualResult.drop('Unnamed: 0.1', axis=1)
    
    dfdualResult=dfdualResult.reset_index()
    dfdualResult = pd.melt(dfdualResult, id_vars=["Date","Type"], value_vars=list(dfdualResult.columns[1:]))
    dfdualResult["Date_Time"] = dfdualResult["Date"].astype("string") + " " + dfdualResult["variable"]
    dfdualResult=dfdualResult.drop(["variable","Date"],axis=1)
    dfdualResult=dfdualResult.set_index(['Date_Time'])
    dfdualResult=dfdualResult.sort_index()
    dfdualResult['value'] = dfdualResult['value'].astype('int')
    
    dfdualResultHTML = dfdualResult.to_html()
    
    if dfdualResult.empty == False:
        dfdualResult['Type'] = dfdualResult['Type'].str.strip()
        dfdualResult = dfdualResult.to_json(orient="index",indent=1) 
        container_clientdual = blob_service_client.get_container_client("dualevents")
        blob_client = container_clientdual.get_blob_client(eventName)
        container_clientdual= blob_client.upload_blob(dfdualResult,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfdualResult,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfdualResultHTML,overwrite=True) 
    
    dfdualResulta = dfdualMaster - dfdual
    dfdualResulta = dfdualResulta.loc[dfdualResulta[dfdualResulta <= -100].any(axis=1)]
    dfdualResulta = dfdualResulta.drop('Unnamed: 0.1', axis=1)
    
    dfdualResulta=dfdualResulta.reset_index()
    dfdualResulta = pd.melt(dfdualResulta, id_vars=["Date","Type"], value_vars=list(dfdualResulta.columns[1:]))
    dfdualResulta["Date_Time"] = dfdualResulta["Date"].astype("string") + " " + dfdualResulta["variable"]
    dfdualResulta=dfdualResulta.drop(["variable","Date"],axis=1)
    dfdualResulta=dfdualResulta.set_index(['Date_Time'])
    dfdualResulta=dfdualResulta.sort_index()
    dfdualResulta['value'] = dfdualResulta['value'].astype('int')    
    dfdualResultaHTML = dfdualResulta.to_html()
    if dfdualResulta.empty == False:
        dfdualResulta['Type'] = dfdualResulta['Type'].str.strip()
        dfdualResulta = dfdualResulta.to_json(orient="index",indent=1)
        container_clientdual = blob_service_client.get_container_client("dualevents")
        blob_client = container_clientdual.get_blob_client(eventNameb)
        container_clientdual= blob_client.upload_blob(dfdualResulta,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfdualResulta,overwrite=True)
    
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfdualResultaHTML,overwrite=True) 
        
    container_clientdual = blob_service_client.get_container_client("masterdual")
    blob_client = container_clientdual.get_blob_client("dualMaster.csv")
    container_clientdual= blob_client.upload_blob(dfdualstagnant,overwrite=True)

############################################################################
     


 
    blob_client = container_clientHydro.get_blob_client('hydro.csv')
    dfhydro = blob_client.download_blob()
    dfhydro = pd.read_csv(dfhydro)
    dfhydrostagnant = dfhydro.to_csv()
    dfhydro["Date"] = pd.to_datetime(dfhydro["Date"])
    dfhydro['Type'] = dfhydro['Type'].astype('string')
    dfhydro = dfhydro.set_index(['Date','Type'])
    dfhydro = dfhydro.drop(['Unnamed: 0'],axis=1)
    dfhydro = dfhydro.drop(['MC'],axis=1)



    blob_client = container_clientHydroMaster.get_blob_client('hydroMaster.csv')
    dfhydroMaster = blob_client.download_blob()
    dfhydroMaster = pd.read_csv(dfhydroMaster)
 

    dfhydroMaster["Date"] = pd.to_datetime(dfhydroMaster["Date"])
    dfhydroMaster['Type'] = dfhydroMaster['Type'].astype('string')
    dfhydroMaster = dfhydroMaster.set_index(['Date','Type'])
    dfhydroMaster = dfhydroMaster.drop(['Unnamed: 0'],axis=1)
    dfhydroMaster = dfhydroMaster.drop(['MC'],axis=1)



    dfhydroResult = dfhydroMaster - dfhydro
    dfhydroResult = dfhydroResult.loc[dfhydroResult[dfhydroResult >= 100].any(axis=1)]
    dfhydroResult = dfhydroResult.drop('Unnamed: 0.1', axis=1)
    
    dfhydroResult=dfhydroResult.reset_index()
    dfhydroResult = pd.melt(dfhydroResult, id_vars=["Date","Type"], value_vars=list(dfhydroResult.columns[1:]))
    dfhydroResult["Date_Time"] = dfhydroResult["Date"].astype("string") + " " + dfhydroResult["variable"]
    dfhydroResult=dfhydroResult.drop(["variable","Date"],axis=1)
    dfhydroResult=dfhydroResult.set_index(['Date_Time'])
    dfhydroResult=dfhydroResult.sort_index()
    dfhydroResult['value'] = dfhydroResult['value'].astype('int')
    
    
    dfhydroResultHTML = dfhydroResult.to_html()
    
    if dfhydroResult.empty == False:
        dfhydroResult['Type'] = dfhydroResult['Type'].str.strip()
        dfhydroResult = dfhydroResult.to_json(orient="index",indent=1)
        container_clienthydro = blob_service_client.get_container_client("hydroevents")
        blob_client = container_clienthydro.get_blob_client(eventName)
        container_clienthydro= blob_client.upload_blob(dfhydroResult,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfhydroResult,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfhydroResultHTML,overwrite=True) 
    
    dfhydroResulta = dfhydroMaster - dfhydro
    dfhydroResulta = dfhydroResulta.loc[dfhydroResulta[dfhydroResulta <= -100].any(axis=1)]
    dfhydroResulta = dfhydroResulta.drop('Unnamed: 0.1', axis=1)
    
    dfhydroResulta=dfhydroResulta.reset_index()
    dfhydroResulta = pd.melt(dfhydroResulta, id_vars=["Date","Type"], value_vars=list(dfhydroResulta.columns[1:]))
    dfhydroResulta["Date_Time"] = dfhydroResulta["Date"].astype("string") + " " + dfhydroResulta["variable"]
    dfhydroResulta=dfhydroResulta.drop(["variable","Date"],axis=1)
    dfhydroResulta=dfhydroResulta.set_index(['Date_Time'])
    dfhydroResulta=dfhydroResulta.sort_index()    
    dfhydroResulta['value'] = dfhydroResulta['value'].astype('int')    
    
    
    dfhydroResultaHTML = dfhydroResulta.to_html()
    if dfhydroResulta.empty == False:
        dfhydroResulta['Type'] = dfhydroResulta['Type'].str.strip()
        dfhydroResulta = dfhydroResulta.to_json(orient="index",indent=1)
        container_clienthydro = blob_service_client.get_container_client("hydroevents")
        blob_client = container_clienthydro.get_blob_client(eventNameb)
        container_clienthydro= blob_client.upload_blob(dfhydroResulta,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfhydroResulta,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfhydroResultaHTML,overwrite=True) 
    
    container_clienthydro = blob_service_client.get_container_client("masterhydro")
    blob_client = container_clienthydro.get_blob_client("hydroMaster.csv")
    container_clienthydro= blob_client.upload_blob(dfhydrostagnant,overwrite=True)

    #################################################################################
    


 
    blob_client = container_clientOther.get_blob_client('other.csv')
    dfother = blob_client.download_blob()
    dfother = pd.read_csv(dfother)

    dfotherstagnant = dfother.to_csv()
    dfother["Date"] = pd.to_datetime(dfother["Date"])
    dfother['Type'] = dfother['Type'].astype('string')
    dfother = dfother.set_index(['Date','Type'])
    dfother = dfother.drop(['Unnamed: 0'],axis=1)
    dfother = dfother.drop(['MC'],axis=1)




    blob_client = container_clientOtherMaster.get_blob_client('otherMaster.csv')
    dfotherMaster = blob_client.download_blob()
    dfotherMaster = pd.read_csv(dfotherMaster)

    dfotherMaster["Date"] = pd.to_datetime(dfotherMaster["Date"])
    dfotherMaster['Type'] = dfotherMaster['Type'].astype('string')
    dfotherMaster = dfotherMaster.set_index(['Date','Type'])
    dfotherMaster = dfotherMaster.drop(['Unnamed: 0'],axis=1)
    dfotherMaster = dfotherMaster.drop(['MC'],axis=1)



    dfotherResult = dfotherMaster - dfother
    dfotherResult = dfotherResult.loc[dfotherResult[dfotherResult >= 100].any(axis=1)]
    dfotherResult = dfotherResult.drop('Unnamed: 0.1', axis=1)
    
    dfotherResult=dfotherResult.reset_index()
    dfotherResult = pd.melt(dfotherResult, id_vars=["Date","Type"], value_vars=list(dfotherResult.columns[1:]))
    dfotherResult["Date_Time"] = dfotherResult["Date"].astype("string") + " " + dfotherResult["variable"]
    dfotherResult=dfotherResult.drop(["variable","Date"],axis=1)
    dfotherResult=dfotherResult.set_index(['Date_Time'])
    dfotherResult=dfotherResult.sort_index()    
    dfotherResult['value'] = dfotherResult['value'].astype('int')        
    
    
    
    
    dfotherResultHTML = dfotherResult.to_html()
    if dfotherResult.empty == False:
        dfotherResult['Type'] = dfotherResult['Type'].str.strip()
        dfotherResult = dfotherResult.to_json(orient="index",indent=1)
        container_clientother = blob_service_client.get_container_client("otherevents")
        blob_client = container_clientother.get_blob_client(eventName)
        container_clientother= blob_client.upload_blob(dfotherResult,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfotherResult,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfotherResultHTML,overwrite=True) 

    dfotherResulta = dfotherMaster - dfother
    dfotherResulta = dfotherResulta.loc[dfotherResulta[dfotherResulta <= -100].any(axis=1)]
    dfotherResulta = dfotherResulta.drop('Unnamed: 0.1', axis=1)
    
    dfotherResulta=dfotherResulta.reset_index()
    dfotherResulta = pd.melt(dfotherResulta, id_vars=["Date","Type"], value_vars=list(dfotherResulta.columns[1:]))
    dfotherResulta["Date_Time"] = dfotherResulta["Date"].astype("string") + " " + dfotherResulta["variable"]
    dfotherResulta=dfotherResulta.drop(["variable","Date"],axis=1)
    dfotherResulta=dfotherResulta.set_index(['Date_Time'])
    dfotherResulta=dfotherResulta.sort_index()
    dfotherResulta['value'] = dfotherResulta['value'].astype('int')     
    dfgasResultHTMLb = dfgasResulta.to_html()
    if dfotherResulta.empty == False:
        
        dfotherResulta['Type'] = dfotherResulta['Type'].str.strip()
        dfotherResulta = dfotherResulta.to_json(orient="index",indent=1)
        container_clientother = blob_service_client.get_container_client("otherevents")
        blob_client = container_clientother.get_blob_client(eventNameb)
        container_clientother= blob_client.upload_blob(dfotherResulta,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfotherResulta,overwrite=True)

        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfgasResultHTMLb,overwrite=True) 
    
    container_clientother = blob_service_client.get_container_client("masterother")
    blob_client = container_clientother.get_blob_client("otherMaster.csv")
    container_clientother= blob_client.upload_blob(dfotherstagnant,overwrite=True)


    ###############################################################################################
    


 
    blob_client = container_clientStorage.get_blob_client('storage.csv')
    dfstorage = blob_client.download_blob()
    dfstorage = pd.read_csv(dfstorage)

    dfstoragestagnant = dfstorage.to_csv()
    dfstorage["Date"] = pd.to_datetime(dfstorage["Date"])
    dfstorage['Type'] = dfstorage['Type'].astype('string')
    dfstorage = dfstorage.set_index(['Date','Type'])
    dfstorage = dfstorage.drop(['Unnamed: 0'],axis=1)
    dfstorage = dfstorage.drop(['MC'],axis=1)




    blob_client = container_clientStorageMaster.get_blob_client('storageMaster.csv')
    dfstorageMaster = blob_client.download_blob()
    dfstorageMaster = pd.read_csv(dfstorageMaster)


    dfstorageMaster["Date"] = pd.to_datetime(dfstorageMaster["Date"])
    dfstorageMaster['Type'] = dfstorageMaster['Type'].astype('string')
    dfstorageMaster = dfstorageMaster.set_index(['Date','Type'])
    dfstorageMaster = dfstorageMaster.drop(['Unnamed: 0'],axis=1)
    dfstorageMaster = dfstorageMaster.drop(['MC'],axis=1)



    dfstorageResult = dfstorageMaster - dfstorage
    dfstorageResult = dfstorageResult.loc[dfstorageResult[dfstorageResult >= 100].any(axis=1)]
    dfstorageResult = dfstorageResult.drop('Unnamed: 0.1', axis=1)
    
    dfstorageResult=dfstorageResult.reset_index()
    dfstorageResult = pd.melt(dfstorageResult, id_vars=["Date","Type"], value_vars=list(dfstorageResult.columns[1:]))
    dfstorageResult["Date_Time"] = dfstorageResult["Date"].astype("string") + " " + dfstorageResult["variable"]
    dfstorageResult=dfstorageResult.drop(["variable","Date"],axis=1)
    dfstorageResult=dfstorageResult.set_index(['Date_Time'])
    dfstorageResult=dfstorageResult.sort_index()
    dfstorageResult['value'] = dfstorageResult['value'].astype('int') 
    dfstorageResultHTML = dfstorageResult.to_html()
    if dfstorageResult.empty == False:
        
        dfstorageResult['Type'] = dfstorageResult['Type'].str.strip()
        dfstorageResult = dfstorageResult.to_json(orient="index",indent=1)
        container_clientstorage = blob_service_client.get_container_client("storageevents")
        blob_client = container_clientstorage.get_blob_client(eventName)
        container_clientstorage= blob_client.upload_blob(dfstorageResult,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfstorageResult,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfstorageResultHTML,overwrite=True) 
    
    dfstorageResulta = dfstorageMaster - dfstorage
    dfstorageResulta = dfstorageResulta.loc[dfstorageResulta[dfstorageResulta <= -100].any(axis=1)]
    dfstorageResulta = dfstorageResulta.drop('Unnamed: 0.1', axis=1)
    dfstorageResulta=dfstorageResulta.reset_index()
    dfstorageResulta = pd.melt(dfstorageResulta, id_vars=["Date","Type"], value_vars=list(dfstorageResulta.columns[1:]))
    dfstorageResulta["Date_Time"] = dfstorageResulta["Date"].astype("string") + " " + dfstorageResulta["variable"]
    dfstorageResulta=dfstorageResulta.drop(["variable","Date"],axis=1)
    dfstorageResulta=dfstorageResulta.set_index(['Date_Time'])
    dfstorageResulta=dfstorageResulta.sort_index()
    
    dfstorageResulta['value'] = dfstorageResulta['value'].astype('int')     
    dfstorageResultaHTML = dfstorageResulta.to_html()
    if dfstorageResulta.empty == False:
        dfstorageResulta['Type'] = dfstorageResulta['Type'].str.strip()
        dfstorageResulta = dfstorageResulta.to_json(orient="index",indent=1)
        container_clientstorage = blob_service_client.get_container_client("storageevents")
        blob_client = container_clientstorage.get_blob_client(eventNameb)
        container_clientstorage= blob_client.upload_blob(dfstorageResulta,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfstorageResulta,overwrite=True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfstorageResultaHTML,overwrite=True)  

    container_clientstorage = blob_service_client.get_container_client("masterstorage")
    blob_client = container_clientstorage.get_blob_client("storageMaster.csv")
    container_clientstorage= blob_client.upload_blob(dfstoragestagnant,overwrite=True)

    ############################################################################################
    


 
    blob_client = container_clientSolar.get_blob_client('solar.csv')
    dfsolar = blob_client.download_blob()
    dfsolar = pd.read_csv(dfsolar)

    dfsolarstagnant = dfsolar.to_csv()
    dfsolar["Date"] = pd.to_datetime(dfsolar["Date"])
    dfsolar['Type'] = dfsolar['Type'].astype('string')
    dfsolar = dfsolar.set_index(['Date','Type'])
    dfsolar = dfsolar.drop(['Unnamed: 0'],axis=1)
    dfsolar = dfsolar.drop(['MC'],axis=1)




    blob_client = container_clientSolarMaster.get_blob_client('solarMaster.csv')
    dfsolarMaster = blob_client.download_blob()
    dfsolarMaster = pd.read_csv(dfsolarMaster)


    dfsolarMaster["Date"] = pd.to_datetime(dfsolarMaster["Date"])
    dfsolarMaster['Type'] = dfsolarMaster['Type'].astype('string')
    dfsolarMaster = dfsolarMaster.set_index(['Date','Type'])
    dfsolarMaster = dfsolarMaster.drop(['Unnamed: 0'],axis=1)
    dfsolarMaster = dfsolarMaster.drop(['MC'],axis=1)



    dfsolarResult = dfsolarMaster - dfsolar
    dfsolarResult = dfsolarResult.loc[dfsolarResult[dfsolarResult >= 100].any(axis=1)]
    dfsolarResult = dfsolarResult.drop('Unnamed: 0.1', axis=1)
    
    dfsolarResult=dfsolarResult.reset_index()
    dfsolarResult = pd.melt(dfsolarResult, id_vars=["Date","Type"], value_vars=list(dfsolarResult.columns[1:]))
    dfsolarResult["Date_Time"] = dfsolarResult["Date"].astype("string") + " " + dfsolarResult["variable"]
    dfsolarResult=dfsolarResult.drop(["variable","Date"],axis=1)
    dfsolarResult=dfsolarResult.set_index(['Date_Time'])
    dfsolarResult=dfsolarResult.sort_index()
    
    dfsolarResult['value'] = dfsolarResult['value'].astype('int')      
    dfsolarResultHTML = dfsolarResult.to_html()
    if dfsolarResult.empty == False:
        dfsolarResult['Type'] = dfsolarResult['Type'].str.strip()
        dfsolarResult = dfsolarResult.to_json(orient="index",indent=1)
        container_clientsolar = blob_service_client.get_container_client("solarevents")
        blob_client = container_clientsolar.get_blob_client(eventName)
        container_clientsolar= blob_client.upload_blob(dfsolarResult,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfsolarResult,overwrite=True)    
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfsolarResultHTML,overwrite=True)
    
    dfsolarResulta = dfsolarMaster - dfsolar
    dfsolarResulta = dfsolarResulta.loc[dfsolarResulta[dfsolarResulta <= -100].any(axis=1)]
    dfsolarResulta = dfsolarResulta.drop('Unnamed: 0.1', axis=1)
    
    dfsolarResulta=dfsolarResulta.reset_index()
    dfsolarResulta = pd.melt(dfsolarResulta, id_vars=["Date","Type"], value_vars=list(dfsolarResulta.columns[1:]))
    dfsolarResulta["Date_Time"] = dfsolarResulta["Date"].astype("string") + " " + dfsolarResulta["variable"]
    dfsolarResulta=dfsolarResulta.drop(["variable","Date"],axis=1)
    dfsolarResulta=dfsolarResulta.set_index(['Date_Time'])
    dfsolarResulta=dfsolarResulta.sort_index()
    
    dfsolarResulta['value'] = dfsolarResulta['value'].astype('int')     
    dfsolarResultHTMLb = dfsolarResulta.to_html()
    if dfsolarResulta.empty == False:
        dfsolarResulta['Type'] = dfsolarResulta['Type'].str.strip()
        dfsolarResulta = dfsolarResulta.to_json(orient="index",indent=1)
        container_clientsolar = blob_service_client.get_container_client("solarevents")
        blob_client = container_clientsolar.get_blob_client(eventNameb)
        container_clientsolar= blob_client.upload_blob(dfsolarResulta,overwrite = True)
        
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfsolarResulta,overwrite=True)   
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTMLb)
        container_clientHTML= blob_client.upload_blob(dfsolarResultHTMLb,overwrite=True)
    
    container_clientsolar = blob_service_client.get_container_client("mastersolar")
    blob_client = container_clientsolar.get_blob_client("solarMaster.csv")
    container_clientsolar= blob_client.upload_blob(dfsolarstagnant,overwrite=True)


    ##############################################################################################
    


 
    blob_client = container_clientWind.get_blob_client('wind.csv')
    dfwind = blob_client.download_blob()
    dfwind = pd.read_csv(dfwind)

    dfwindstagnant = dfwind.to_csv()
    dfwind["Date"] = pd.to_datetime(dfwind["Date"])
    dfwind['Type'] = dfwind['Type'].astype('string')
    dfwind = dfwind.set_index(['Date','Type'])
    dfwind = dfwind.drop(['Unnamed: 0'],axis=1)
    dfwind = dfwind.drop(['MC'],axis=1)




    blob_client = container_clientWindMaster.get_blob_client('windMaster.csv')
    dfwindMaster = blob_client.download_blob()
    dfwindMaster = pd.read_csv(dfwindMaster)


    dfwindMaster["Date"] = pd.to_datetime(dfwindMaster["Date"])
    dfwindMaster['Type'] = dfwindMaster['Type'].astype('string')
    dfwindMaster = dfwindMaster.set_index(['Date','Type'])
    dfwindMaster = dfwindMaster.drop(['Unnamed: 0'],axis=1)
    dfwindMaster = dfwindMaster.drop(['MC'],axis=1)



    dfwindResult = dfwindMaster - dfwind
    dfwindResult = dfwindResult.loc[dfwindResult[dfwindResult >= 100].any(axis=1)]
    dfwindResult = dfwindResult.drop('Unnamed: 0.1', axis=1)
    
    dfwindResult=dfwindResult.reset_index()
    dfwindResult = pd.melt(dfwindResult, id_vars=["Date","Type"], value_vars=list(dfwindResult.columns[1:]))
    dfwindResult["Date_Time"] = dfwindResult["Date"].astype("string") + " " + dfwindResult["variable"]
    dfwindResult=dfwindResult.drop(["variable","Date"],axis=1)
    dfwindResult=dfwindResult.set_index(['Date_Time'])
    dfwindResult=dfwindResult.sort_index()
    
    
    
    dfwindResult['value'] = dfwindResult['value'].astype('int')      
    dfwindResultHTML = dfwindResult.to_html()
    if dfwindResult.empty == False:
        dfwindResult['Type'] = dfwindResult['Type'].str.strip()
        dfwindResult = dfwindResult.to_json(orient="index",indent=1)
        container_clientwind = blob_service_client.get_container_client("windevents")
        blob_client = container_clientwind.get_blob_client(eventName)
        container_clientwind= blob_client.upload_blob(dfwindResult,overwrite = True)
    
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfwindResult,overwrite=True)   
            
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventName)
        container_clientHTML= blob_client.upload_blob(dfwindResultHTML,overwrite=True)
        
        
    dfwindResulta = dfwindMaster - dfwind
    dfwindResulta = dfwindResulta.loc[dfwindResulta[dfwindResulta <= -100].any(axis=1)]
    dfwindResulta = dfwindResulta.drop('Unnamed: 0.1', axis=1)
    
    dfwindResulta=dfwindResulta.reset_index()
    dfwindResulta = pd.melt(dfwindResulta, id_vars=["Date","Type"], value_vars=list(dfwindResulta.columns[1:]))
    dfwindResulta["Date_Time"] = dfwindResulta["Date"].astype("string") + " " + dfwindResulta["variable"]
    dfwindResulta=dfwindResulta.drop(["variable","Date"],axis=1)
    dfwindResulta=dfwindResulta.set_index(['Date_Time'])
    dfwindResulta=dfwindResulta.sort_index()
    
    
    dfwindResulta['value'] = dfwindResulta['value'].astype('int')      
    dfwindResultHTMLa = dfwindResulta.to_html()
    if dfwindResulta.empty == False:
        dfwindResulta['Type'] = dfwindResulta['Type'].str.strip()
        dfwindResulta = dfwindResulta.to_json(orient="index",indent=1)
        container_clientwind = blob_service_client.get_container_client("windevents")
        blob_client = container_clientwind.get_blob_client(eventName)
        container_clientwind= blob_client.upload_blob(dfwindResulta,overwrite = True)
    
        container_clientCOAL1 = blob_service_client1.get_container_client("sevenday")
        blob_client1= container_clientCOAL1.get_blob_client(eventName)
        container_clientCOAL1= blob_client1.upload_blob(dfwindResulta,overwrite=True)   
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventName)
        container_clientHTML= blob_client.upload_blob(dfwindResultHTMLa,overwrite=True)
        
        
    container_clientwind = blob_service_client.get_container_client("masterwind")
    blob_client = container_clientwind.get_blob_client("windMaster.csv")
    container_clientwind= blob_client.upload_blob(dfwindstagnant,overwrite=True)