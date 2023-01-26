
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
    eventName = now + ".csv"
    eventNameb = now + "b.csv"
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
    dfCoalResultHTML = dfCoalResult.to_html()
    if dfCoalResult.empty == False:
        
        dfCoalResult = dfCoalResult.to_csv()
        container_clientCoal = blob_service_client.get_container_client("coalevents")
        blob_client = container_clientCoal.get_blob_client(eventName)
        container_clientCoal= blob_client.upload_blob(dfCoalResult,overwrite= True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfCoalResultHTML,overwrite=True) 
        
    dfCoalResulta = dfCoalMaster - dfCoal
    dfCoalResulta = dfCoalResulta.loc[dfCoalResulta[dfCoalResulta <= -100].any(axis=1)]
    dfCoalResulta = dfCoalResulta.drop('Unnamed: 0.1', axis=1)
    dfCoalResultaHTML = dfCoalResulta.to_html()
    if dfCoalResulta.empty == False:
        
        dfCoalResulta = dfCoalResulta.to_csv()
        container_clientCoal = blob_service_client.get_container_client("coalevents")
        blob_client = container_clientCoal.get_blob_client(eventNameb)
        container_clientCoal= blob_client.upload_blob(dfCoalResulta,overwrite= True)
        
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
    dfgasResultHTML = dfgasResult.to_html()
    if dfgasResult.empty == False:
        dfgasResult = dfgasResult.to_csv()
        container_clientgas = blob_service_client.get_container_client("gasevents")
        blob_client = container_clientgas.get_blob_client(eventName)
        container_clientgas= blob_client.upload_blob(dfgasResult,overwrite = True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfgasResultHTML,overwrite=True) 
        
    dfgasResulta = dfgasMaster - dfgas
    dfgasResulta = dfgasResulta.loc[dfgasResulta[dfgasResulta <= -100].any(axis=1)]
    dfgasResulta = dfgasResulta.drop('Unnamed: 0.1', axis=1)
    dfgasResultHTMLa = dfgasResulta.to_html()
    if dfgasResulta.empty == False:
        
        dfgasResulta = dfgasResulta.to_csv()
        container_clientgas = blob_service_client.get_container_client("gasevents")
        blob_client = container_clientgas.get_blob_client(eventNameb)
        container_clientgas= blob_client.upload_blob(dfgasResulta,overwrite = True)
    
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
    dfdualResultHTML = dfdualResult.to_html()
    
    if dfdualResult.empty == False:
        
        dfdualResult = dfdualResult.to_csv()
        container_clientdual = blob_service_client.get_container_client("dualevents")
        blob_client = container_clientdual.get_blob_client(eventName)
        container_clientdual= blob_client.upload_blob(dfdualResult,overwrite = True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfdualResultHTML,overwrite=True) 
    
    dfdualResulta = dfdualMaster - dfdual
    dfdualResulta = dfdualResulta.loc[dfdualResulta[dfdualResulta <= -100].any(axis=1)]
    dfdualResulta = dfdualResulta.drop('Unnamed: 0.1', axis=1)
    dfdualResultaHTML = dfdualResulta.to_html()
    if dfdualResulta.empty == False:
        
        dfdualResulta = dfdualResulta.to_csv()
        container_clientdual = blob_service_client.get_container_client("dualevents")
        blob_client = container_clientdual.get_blob_client(eventNameb)
        container_clientdual= blob_client.upload_blob(dfdualResulta,overwrite = True)
    
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
    dfhydroResultHTML = dfhydroResult.to_html()
    
    if dfhydroResult.empty == False:
        
        dfhydroResult = dfhydroResult.to_csv()
        container_clienthydro = blob_service_client.get_container_client("hydroevents")
        blob_client = container_clienthydro.get_blob_client(eventName)
        container_clienthydro= blob_client.upload_blob(dfhydroResult,overwrite = True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfhydroResultHTML,overwrite=True) 
    
    dfhydroResulta = dfhydroMaster - dfhydro
    dfhydroResulta = dfhydroResulta.loc[dfhydroResulta[dfhydroResulta <= -100].any(axis=1)]
    dfhydroResulta = dfhydroResulta.drop('Unnamed: 0.1', axis=1)
    dfhydroResultaHTML = dfhydroResulta.to_html()
    if dfhydroResulta.empty == False:
        
        dfhydroResulta = dfhydroResulta.to_csv()
        container_clienthydro = blob_service_client.get_container_client("hydroevents")
        blob_client = container_clienthydro.get_blob_client(eventNameb)
        container_clienthydro= blob_client.upload_blob(dfhydroResulta,overwrite = True)
        
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
    dfotherResultHTML = dfotherResult.to_html()
    if dfotherResult.empty == False:
        
        dfotherResult = dfotherResult.to_csv()
        container_clientother = blob_service_client.get_container_client("otherevents")
        blob_client = container_clientother.get_blob_client(eventName)
        container_clientother= blob_client.upload_blob(dfotherResult,overwrite = True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfotherResultHTML,overwrite=True) 

    dfotherResulta = dfotherMaster - dfother
    dfotherResulta = dfotherResulta.loc[dfotherResulta[dfotherResulta <= -100].any(axis=1)]
    dfotherResulta = dfotherResulta.drop('Unnamed: 0.1', axis=1)
    dfgasResultHTMLb = dfgasResulta.to_html()
    if dfotherResulta.empty == False:
        
        dfotherResulta = dfotherResulta.to_csv()
        container_clientother = blob_service_client.get_container_client("otherevents")
        blob_client = container_clientother.get_blob_client(eventNameb)
        container_clientother= blob_client.upload_blob(dfotherResulta,overwrite = True)

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
    dfstorageResultHTML = dfstorageResult.to_html()
    if dfstorageResult.empty == False:
        
        dfstorageResult = dfstorageResult.to_csv()
        container_clientstorage = blob_service_client.get_container_client("storageevents")
        blob_client = container_clientstorage.get_blob_client(eventName)
        container_clientstorage= blob_client.upload_blob(dfstorageResult,overwrite = True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfstorageResultHTML,overwrite=True) 
    
    dfstorageResulta = dfstorageMaster - dfstorage
    dfstorageResulta = dfstorageResulta.loc[dfstorageResulta[dfstorageResulta <= -100].any(axis=1)]
    dfstorageResulta = dfstorageResulta.drop('Unnamed: 0.1', axis=1)
    dfstorageResultaHTML = dfstorageResulta.to_html()
    if dfstorageResulta.empty == False:
        
        dfstorageResulta = dfstorageResulta.to_csv()
        container_clientstorage = blob_service_client.get_container_client("storageevents")
        blob_client = container_clientstorage.get_blob_client(eventNameb)
        container_clientstorage= blob_client.upload_blob(dfstorageResulta,overwrite = True)
        
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
    dfsolarResultHTML = dfsolarResult.to_html()
    if dfsolarResult.empty == False:
        
        dfsolarResult = dfsolarResult.to_csv()
        container_clientsolar = blob_service_client.get_container_client("solarevents")
        blob_client = container_clientsolar.get_blob_client(eventName)
        container_clientsolar= blob_client.upload_blob(dfsolarResult,overwrite = True)
        
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventNameHTML)
        container_clientHTML= blob_client.upload_blob(dfsolarResultHTML,overwrite=True)
    
    dfsolarResulta = dfsolarMaster - dfsolar
    dfsolarResulta = dfsolarResulta.loc[dfsolarResulta[dfsolarResulta <= -100].any(axis=1)]
    dfsolarResulta = dfsolarResulta.drop('Unnamed: 0.1', axis=1)
    dfsolarResultHTMLb = dfsolarResulta.to_html()
    if dfsolarResulta.empty == False:
        
        dfsolarResulta = dfsolarResulta.to_csv()
        container_clientsolar = blob_service_client.get_container_client("solarevents")
        blob_client = container_clientsolar.get_blob_client(eventNameb)
        container_clientsolar= blob_client.upload_blob(dfsolarResulta,overwrite = True)
        
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
    dfwindResultHTML = dfwindResult.to_html()
    if dfwindResult.empty == False:
        
        dfwindResult = dfwindResult.to_csv()
        container_clientwind = blob_service_client.get_container_client("windevents")
        blob_client = container_clientwind.get_blob_client(eventName)
        container_clientwind= blob_client.upload_blob(dfwindResult,overwrite = True)
    
    
        container_clientHTML = blob_service_client.get_container_client("sevenhtml")
        blob_client = container_clientHTML.get_blob_client(eventName)
        container_clientHTML= blob_client.upload_blob(dfwindResultHTML,overwrite=True)
        
        
    dfwindResulta = dfwindMaster - dfwind
    dfwindResulta = dfwindResulta.loc[dfwindResulta[dfwindResulta <= -100].any(axis=1)]
    dfwindResulta = dfwindResulta.drop('Unnamed: 0.1', axis=1)
    dfwindResultHTMLb = dfwindResulta.to_html()
    if dfwindResulta.empty == False:
        
        dfwindResulta = dfwindResulta.to_csv()
        container_clientwind = blob_service_client.get_container_client("windevents")
        blob_client = container_clientwind.get_blob_client(eventName)
        container_clientwind= blob_client.upload_blob(dfwindResultHTMLb,overwrite = True)
    
    
    container_clientwind = blob_service_client.get_container_client("masterwind")
    blob_client = container_clientwind.get_blob_client("windMaster.csv")
    container_clientwind= blob_client.upload_blob(dfwindstagnant,overwrite=True)