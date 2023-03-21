import logging
from datetime import datetime
import logging
import requests
import pandas as pd
import re
from pretty_html_table import build_table
from io import StringIO
from typing import BinaryIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient


async def main(mytimer: func.TimerRequest):
    
    blob_connection_Pulls = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")

    blob_connection_clientJSON = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=outageblobspremium;AccountKey=hYBKzIYX0cNoB//eU5OqR1m3AFiB3dEOqnI+BDUv94CQZ5Ep1fHEeMsczoJPQuIXRnkOIgxKSAXO+AStyiep+A==;EndpointSuffix=core.windows.net")
    
    
    
    #DRONES
    container_clientCoal = blob_connection_Pulls.get_container_client("outcoal7day")
    blob_clientCOAL = container_clientCoal.get_blob_client('coal.csv')
    dfDroneCOAL = blob_clientCOAL.download_blob()        
        
    container_clientGas = blob_connection_Pulls.get_container_client("outgas7day")
    blob_clientGAS = container_clientGas.get_blob_client('gas.csv')
    dfDroneGAS = blob_clientGAS.download_blob()  
    
    container_clientDual = blob_connection_Pulls.get_container_client("outdual7day")
    blob_clientDUAL = container_clientDual.get_blob_client('dual.csv')
    dfDroneDUAL = blob_clientDUAL.download_blob()  
    
    container_clientHydro = blob_connection_Pulls.get_container_client("outhydro7day")
    blob_clientHYDRO = container_clientHydro.get_blob_client('hydro.csv')
    dfDroneHYDRO = blob_clientHYDRO.download_blob()  
    
    container_clientSolar = blob_connection_Pulls.get_container_client("outsolar7day")
    blob_clientSOLAR = container_clientSolar.get_blob_client('solar.csv')
    dfDroneSOLAR = blob_clientSOLAR.download_blob()  
    
    container_clientWind = blob_connection_Pulls.get_container_client("outwind7day")
    blob_clientWIND = container_clientWind.get_blob_client('wind.csv')
    dfDroneWIND = blob_clientWIND.download_blob()  
    
    container_clientStorage = blob_connection_Pulls.get_container_client("outstorage7day")
    blob_clientSTORAGE = container_clientStorage.get_blob_client('storage.csv')
    dfDroneSTORAGE = blob_clientSTORAGE.download_blob()  
    
    container_clientOther = blob_connection_Pulls.get_container_client("outother7day")
    blob_clientOTHER = container_clientOther.get_blob_client('other.csv')
    dfDroneOTHER = blob_clientOTHER.download_blob()  
    
    
    
    
    
    
    #MASTER
    container_clientCoalMaster = blob_connection_Pulls.get_container_client("mastercoal")
    blob_clientCOALMASTER = container_clientCoalMaster.get_blob_client('coalMaster.csv')
    dfMasterCOAL = blob_clientCOALMASTER.download_blob()    
    
    container_clientGasMaster = blob_connection_Pulls.get_container_client("mastergas")
    blob_clientGASMASTER = container_clientGasMaster.get_blob_client('gasMaster.csv')
    dfMasterGAS = blob_clientGASMASTER.download_blob()   
    
    container_clientDualMaster = blob_connection_Pulls.get_container_client("masterdual")
    blob_clientDUALMASTER = container_clientDualMaster.get_blob_client('dualMaster.csv')
    dfMasterDUAL = blob_clientDUALMASTER.download_blob()   
    
    container_clientHydroMaster = blob_connection_Pulls.get_container_client("masterhydro")
    blob_clientHYDROMASTER = container_clientHydroMaster.get_blob_client('hydroMaster.csv')
    dfMasterHYDRO = blob_clientHYDROMASTER.download_blob()   
    
    container_clientSolarMaster = blob_connection_Pulls.get_container_client("mastersolar")
    blob_clientSOLARMASTER = container_clientSolarMaster.get_blob_client('solarMaster.csv')
    dfMasterSOLAR = blob_clientSOLARMASTER.download_blob()   
    
    container_clientWindMaster = blob_connection_Pulls.get_container_client("masterwind")
    blob_clientWINDMASTER = container_clientWindMaster.get_blob_client('windMaster.csv')
    dfMasterWIND = blob_clientWINDMASTER.download_blob()   
    
    container_clientStorageMaster = blob_connection_Pulls.get_container_client("masterstorage")
    blob_clientSTORAGEMASTER = container_clientStorageMaster.get_blob_client('storageMaster.csv')
    dfMasterSTORAGE = blob_clientSTORAGEMASTER.download_blob()   
    
    container_clientOtherMaster = blob_connection_Pulls.get_container_client("masterother") 
    blob_clientOTHERMASTER = container_clientOtherMaster.get_blob_client('otherMaster.csv')
    dfMasterOTHER = blob_clientOTHERMASTER.download_blob()   
    
    list_of_names = ["mastercoal","mastergas","masterdual","masterhydro","mastersolar","masterwind","masterstorage","masterother"]
    


    
    
    
    
    def start(blob_connection_Pulls,blob_connection_clientJSON,container_clientDrone,container_clientMaster,list_of_names,list_of_namesCSV):
        container_clientDrone = container_clientDrone
        container_clientMaster = container_clientMaster
        blob_connection_Pulls = blob_connection_Pulls
        blob_connection_clientJSON = blob_connection_clientJSON
        list_of_names = list_of_names
        list_of_namesCSV = list_of_namesCSV
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        ##Events are labelled by datetime and concatted with .csv
        eventName = now + list_of_names + ".json"
        eventNameHTML = now + list_of_names +  ".html"

        
        df_drone = pd.read_csv(container_clientDrone,index_col=[0])
        df_drone = df_drone.reset_index(drop=True)


        df_drone["Date"] = pd.to_datetime(df_drone["Date"])
        df_drone['Type'] = df_drone['Type'].astype('string')
        df_drone = df_drone.set_index(['Date','Type'])
        df_drone = df_drone.drop(['MC'],axis=1)
        df_dronestagnant = df_drone.to_csv()
        
        

        df_Master = pd.read_csv(container_clientMaster)

        df_Master["Date"] = pd.to_datetime(df_Master["Date"])
        df_Master['Type'] = df_Master['Type'].astype('string')
        df_Master = df_Master.set_index(['Date','Type'])

        df_droneResult = df_Master - df_drone
        df_droneResult = df_droneResult.loc[(df_droneResult[df_droneResult >= 100].any(axis=1)) | (df_droneResult[df_droneResult <= -100].any(axis=1))]
        df_droneResult=df_droneResult.sort_index()
        df_droneResult=df_droneResult.reset_index()
        df_droneResultJSON  = df_droneResult

        df_droneResult = pd.melt(df_droneResult, id_vars=["Date","Type"], value_vars=list(df_droneResult.columns[1:]))
        df_droneResult["Date_Time"] = df_droneResult["Date"].astype("string") + " " + df_droneResult["variable"]
        
        df_droneResult=df_droneResult.drop(["variable","Date"],axis=1)



        df_droneResult['value'] = df_droneResult['value'].astype('int')
        df_droneResult = df_droneResult.iloc[:,[2,0,1]]
        df_droneResult = df_droneResult.sort_values(by='Date_Time', ascending=True)
        df_droneResult = df_droneResult[df_droneResult['value'] != 0]
        html_table = build_table(df_droneResult
            , 'blue_light'
            , font_size='medium'
            , font_family='Open Sans sans-serif'
            , text_align='justify'
            
            , width_dict=['200px','150px', '150px']
            , index=False
            ,conditions={
                'value': {
                    'min': -1,
                    'max': 1,
                    'min_color': 'green',
                    'max_color': 'red',
                }
            }) 
        df_droneResultHTML = html_table

        if df_droneResult.empty == False:

            df_droneResultJSON['Date'] = df_droneResultJSON['Date'].astype("string")
            df_droneResultJSON  = df_droneResultJSON.loc[:,'Date':'HE24']
            result = {row['Date']: row.loc['HE01':'HE24'].to_dict() for _, row in df_droneResultJSON.iterrows()}
            data = {'type': df_droneResultJSON.loc[0,'Type'].strip(),'dates':result}
            dataJSON = pd.DataFrame(data=data)
            dataJSON = dataJSON.to_json()
            # container_clientgas = blob_connection_Pulls.get_container_client("gasevents")
            # blob_client = container_clientgas.get_blob_client(eventName)
            # container_clientgas= blob_client.upload_blob(df_droneResult,overwrite = True)
            
            container_clientCOAL1 = blob_connection_clientJSON.get_container_client("sevenday")
            blob_client1= container_clientCOAL1.get_blob_client(eventName)
            container_clientCOAL1= blob_client1.upload_blob(dataJSON,overwrite=True)
            
            container_clientHTML = blob_connection_Pulls.get_container_client("sevenhtml")
            blob_client = container_clientHTML.get_blob_client(eventNameHTML)
            container_clientHTML= blob_client.upload_blob(df_droneResultHTML,overwrite=True) 

        container_clientgas = blob_connection_Pulls.get_container_client(list_of_names)
        blob_client = container_clientgas.get_blob_client(list_of_namesCSV)
        container_clientgas= blob_client.upload_blob(df_dronestagnant,overwrite=True)

    list_of_names = ["mastercoal","mastergas","masterdual","masterhydro","mastersolar","masterwind","masterstorage","masterother"]
    list_of_namesCSV = ["coalMaster.csv","gasMaster.csv","dualMaster.csv","hydroMaster.csv","solarMaster.csv","windMaster.csv","storageMaster.csv","otherMaster.csv"]
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneCOAL,dfMasterCOAL,list_of_names[0],list_of_namesCSV[0])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneGAS,dfMasterGAS,list_of_names[1],list_of_namesCSV[1])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneHYDRO,dfMasterHYDRO,list_of_names[3],list_of_namesCSV[3])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneDUAL,dfMasterDUAL,list_of_names[2],list_of_namesCSV[2])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneSOLAR,dfMasterSOLAR,list_of_names[4],list_of_namesCSV[4])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneWIND,dfMasterWIND,list_of_names[5],list_of_namesCSV[5])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneOTHER,dfMasterOTHER,list_of_names[7],list_of_namesCSV[7])
    start(blob_connection_Pulls,blob_connection_clientJSON,dfDroneSTORAGE,dfMasterSTORAGE,list_of_names[6],list_of_namesCSV[6])

