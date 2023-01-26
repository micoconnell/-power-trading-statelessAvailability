import datetime
import logging
import requests
import pandas as pd
import re

import azure.functions as func
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient

async def main(mytimer: func.TimerRequest):
        utc_timestamp = datetime.datetime.utcnow().replace(
                tzinfo=datetime.timezone.utc).isoformat()

        if mytimer.past_due:
                logging.info('The timer is past due!')

        logging.info('Python timer trigger function ran at %s', utc_timestamp)
        ## This website is beastly to scrape. Dataframe transformations below are due to poor 
        ## transferability to the dataframe format.
        url = "http://ets.aeso.ca/ets_web/ip/Market/Reports/SevenDaysHourlyAvailableCapabilityReportServlet?contentType=html"

        df = pd.read_html(url)[2]
        df = pd.DataFrame(df)
        print(df)
        ## Pick correct table, and drop blank web-ding looking rows.

        df = df.drop(df.index [ [ 8,16,24,32,40,48,56 ] ])
        df = df.drop(df.iloc[:,26:100],axis=1)

        ##Search function to get both MC and keywords (coal,gas,wind etc)
        def find_number(text):
                num = re.findall(r'[0-9]+',text)
                return " ".join(num)
        df['MC']=df[0].apply(lambda x: find_number(x))

        def find_word_Coal(text):
                num = re.findall(r'COAL',text)
                return " ".join(num)
        def find_word_Dual(text):
                num = re.findall(r'DUAL',text)
                return " ".join(num)
        def find_word_Gas(text):
                num = re.findall(r'GAS',text)
                return " ".join(num)
        def find_word_Hydro(text):
                num = re.findall(r'HYDRO',text)
                return " ".join(num)
        def find_word_Solar(text):
                num = re.findall(r'SOLAR',text)
                return " ".join(num)
        def find_word_Wind(text):
                num = re.findall(r'WIND',text)
                return " ".join(num)
        def find_word_Energy(text):
                num = re.findall(r'ENERGY STORAGE',text)
                return " ".join(num)
        def find_word_Bio(text):
                num = re.findall(r'BIOMASS and OTHER',text)
                return " ".join(num)
        ##Putting it all together      
        df['Coal']=df[0].apply(lambda x: find_word_Coal(x))
        df['Gas']=df[0].apply(lambda x: find_word_Gas(x))
        df['Hydro']=df[0].apply(lambda x: find_word_Hydro(x))
        df['Solar']=df[0].apply(lambda x: find_word_Solar(x))
        df['Wind']=df[0].apply(lambda x: find_word_Wind(x))
        df['Energy']=df[0].apply(lambda x: find_word_Energy(x))
        df['Bio']=df[0].apply(lambda x: find_word_Bio(x))
        df['Dualfuel']=df[0].apply(lambda x: find_word_Dual(x))
        df['MC']=df[0].apply(lambda x: find_number(x))
        
        #Final Renaming of columns. Rejoin / get rid of extra columns created from conversion
        df[0] = df[['Coal','Dualfuel','Gas','Hydro','Solar','Wind','Energy','Bio']].agg(' '.join, axis=1)
        df = df.drop(df.iloc[:,28:100],axis=1)
        df = df.drop(0,axis=0)

        df = df.rename(columns={0:"Type", 1: "Date",2:"HE01",3:"HE02",4:"HE03",5:"HE04",6:"HE05",7:"HE06",8:"HE07",9:"HE08",10:"HE09",11:"HE10",12:"HE11",
                                13:"HE12",
                                14:"HE13",15:"HE14",16:"HE15",17:"HE16",18:"HE17",19:"HE18",20:"HE19",21:"HE20",22:"HE21",23:"HE22",24:"HE23",25:"HE24"})
        df = df.drop(['Coal'],axis=1)
        #df=df.convert_dtypes()
        listOFBULLSHIT = ["HE01","HE02","HE03","HE04","HE05","HE06","HE07","HE08","HE09","HE10","HE11","HE12","HE13","HE14","HE15","HE16","HE17","HE18","HE19","HE20","HE21","HE22","HE23","HE24"]
        for x in listOFBULLSHIT:
                df[x] = df[x].str.rstrip('%').astype('float') / 100.0
        for x in listOFBULLSHIT:
                df[x] = df[x].astype(float)
                df[x] = df[x] * df['MC'].astype(float)
        

        
        
        #
        dfCOAL = df[0:7]
        dfCOAL = dfCOAL.round(0) 
        dfGAS = df[7:14]
        dfGAS = dfGAS.round(0) 
        dfDUAL = df[14:21]
        dfDUAL = dfDUAL.round(0) 
        dfHYDRO = df[21:28]
        dfHYDRO = dfHYDRO.round(0) 
        dfWIND = df[28:35]
        dfWIND = dfWIND.round(0) 
        dfSOLAR = df[35:42]
        dfSOLAR = dfSOLAR.round(0) 
        dfSTORAGE = df[42:49]
        dfSTORAGE = dfSTORAGE.round(0) 
        dfOTHER = df[49:56]
        dfOTHER = dfOTHER.round(0) 
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
        container_clientCoal = blob_service_client.get_container_client("outcoal7day")
        container_clientGas = blob_service_client.get_container_client("outgas7day")
        container_clientDual = blob_service_client.get_container_client("outdual7day")
        container_clientHydro = blob_service_client.get_container_client("outhydro7day")
        container_clientSolar = blob_service_client.get_container_client("outsolar7day")
        container_clientWind = blob_service_client.get_container_client("outwind7day")
        container_clientStorage = blob_service_client.get_container_client("outstorage7day")
        container_clientOther = blob_service_client.get_container_client("outother7day")
        dfCOAL = dfCOAL.to_csv()
        dfGAS = dfGAS.to_csv()
        dfHYDRO = dfHYDRO.to_csv()
        dfDUAL = dfDUAL.to_csv()
        dfSOLAR = dfSOLAR.to_csv()
        dfWIND = dfWIND.to_csv()
        dfSTORAGE = dfSTORAGE.to_csv()
        dfOTHER = dfOTHER.to_csv()
        
        #Final uploads for drones by asset type
        blob_client = container_clientCoal.get_blob_client("coal.csv")
        container_clientCoal= blob_client.upload_blob(dfCOAL,overwrite=True)
        
        blob_client = container_clientGas.get_blob_client("gas.csv")
        container_clientGas= blob_client.upload_blob(dfGAS,overwrite=True)

        blob_client = container_clientHydro.get_blob_client("hydro.csv")
        container_clientHydro= blob_client.upload_blob(dfHYDRO,overwrite=True)

        blob_client = container_clientDual.get_blob_client("dual.csv")
        container_clientDual = blob_client.upload_blob(dfDUAL,overwrite=True)

        blob_client = container_clientSolar.get_blob_client("solar.csv")
        container_clientSolar = blob_client.upload_blob(dfSOLAR,overwrite=True)

        blob_client = container_clientWind.get_blob_client("wind.csv")
        container_clientWind= blob_client.upload_blob(dfWIND,overwrite=True)

        blob_client = container_clientStorage.get_blob_client("storage.csv")
        container_clientStorage= blob_client.upload_blob(dfSTORAGE,overwrite=True)
        
        blob_client = container_clientOther.get_blob_client("other.csv")
        container_clientOther= blob_client.upload_blob(dfOTHER,overwrite=True)
        


