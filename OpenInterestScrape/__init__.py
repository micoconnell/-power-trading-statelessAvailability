import datetime
import logging
import pandas as pd

import http.client
import json
import time
import csv
from datetime import datetime
from datetime import timedelta
import re
from tracemalloc import start
import pandas as pd
import os
import zipfile
import certifi
import ssl
import sys
from azure.storage.blob import BlobServiceClient


import azure.functions as func


def main(mytimer: func.TimerRequest):

    


    yesterday = datetime.today() + timedelta(days=-1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    print(yesterday)
    today = datetime.today() + timedelta(days=0)
    today = today.strftime('%Y-%m-%d')
    print(today)
    filename = datetime.today().strftime('%d%m%Y')
    class NRGStreamApiGasGen:

        def __init__(self,username=None,password=None):
                self.username = 'suncor2'
                self.password = 'anisoTropical308'                
                self.server = 'api.nrgstream.com'        
                self.tokenPath = '/api/security/token'
                self.releasePath = '/api/ReleaseToken'
                self.tokenPayload = f'grant_type=password&username={self.username}&password={self.password}'
                self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                self.accessToken = ""
                    

        def getToken(self):
                try:
                    if self.isTokenValid() == False:                             
                        headers = {"Content-type": "application/x-www-form-urlencoded"}      
                        # Connect to API server to get a token
                        context = ssl.create_default_context(cafile=certifi.where())
                        conn = http.client.HTTPSConnection(self.server,context=context)
                        conn.request('POST', self.tokenPath, self.tokenPayload, headers)
                        res = conn.getresponse()                
                        res_code = res.code
                        # Check if the response is good
                        
                        if res_code == 200:
                            res_data = res.read()
                            # Decode the token into an object
                            jsonData = json.loads(res_data.decode('utf-8'))
                            self.accessToken = jsonData['access_token']                         
                            # Calculate new expiry date
                            self.tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])                        
                            #print('token obtained')
                            #print(self.accessToken)
                        else:
                            res_data = res.read()
                            print(res_data.decode('utf-8'))
                        conn.close()                          
                except Exception as e:
                    print("getToken: " + str(e))
                    # Release token if an error occured
                    self.releaseToken()      
        def releaseToken(self):
                try:            
                    headers = {}
                    headers['Authorization'] = f'Bearer {self.accessToken}'            
                    context = ssl.create_default_context(cafile=certifi.where())
                    conn = http.client.HTTPSConnection(self.server,context=context)
                    conn.request('DELETE', self.releasePath, None, headers)  
                    res = conn.getresponse()
                    res_code = res.code
                    if res_code == 200:   
                        # Set expiration date back to guarantee isTokenValid() returns false                
                        self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                        #print('token released')            
                except Exception as e:
                    print("releaseToken: " + str(e))
                            
        def isTokenValid(self):
                if self.tokenExpiry==None:
                    return False
                elif datetime.now() >= self.tokenExpiry:            
                    return False
                else:
                    return True            

        def GetStreamDataByStreamId(self,streamIds, fromDate, toDate, dataFormat='csv', dataOption=''):
                stream_data = "" 
                # Set file format to csv or json            
                DataFormats = {}
                DataFormats['csv'] = 'text/csv'
                DataFormats['json'] = 'Application/json'
                
                try:                            
                    for streamId in streamIds:            
                        # Get an access token            
                        self.getToken()    
                        if self.isTokenValid():
                            # Setup the path for data request. Pass dates in via function call
                            path = f'/api/StreamData/{streamId}'
                            if fromDate != '' and toDate != '':
                                path += f'?fromDate={fromDate.replace(" ", "%20")}&toDate={toDate.replace(" ", "%20")}'
                            if dataOption != '':
                                if fromDate != '' and toDate != '':
                                    path += f'&dataOption={dataOption}'        
                                else:
                                    path += f'?dataOption={dataOption}'        
                            
                            # Create request header
                            headers = {}            
                            headers['Accept'] = DataFormats[dataFormat]
                            headers['Authorization']= f'Bearer {self.accessToken}'
                            
                            # Connect to API server
                            context = ssl.create_default_context(cafile=certifi.where())
                            conn = http.client.HTTPSConnection(self.server,context=context)
                            conn.request('GET', path, None, headers)
                            res = conn.getresponse()        
                            res_code = res.code                    
                            if res_code == 200:   
                                try:
                                    print(f'{datetime.now()} Outputing stream {path} res code {res_code}')
                                    # output return data to a text file            
                                    if dataFormat == 'csv':
                                        stream_data += res.read().decode('utf-8').replace('\r\n','\n') 
                                    elif dataFormat == 'json':
                                        stream_data += json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                                    conn.close()

                                except Exception as e:
                                    print(str(e))            
                                    self.releaseToken()
                                    return None  
                            else:
                                print(str(res_code) + " - " + str(res.reason) + " - " + str(res.read().decode('utf-8')))
                            
                        self.releaseToken()   
                        # Wait 1 second before next request
                        time.sleep(1)
                    return stream_data        
                except Exception as e:
                    print(str(e))    
                    self.releaseToken()
                    return None
        def csvStreamToPandas(self, streamData):
            # split lines of return string from api
            streamData = streamData.split("\n")
            
            # remove empty elements from list
            streamData = [x for x in streamData if len(x) > 0] 
            
            # remove header data
            streamData = [x for x in streamData if x[0] != '#'] 
                        
            # split elements into lists of lists                     
            streamData = [x.split(",") for x in streamData] 
            
            # create dataframe
            df = pd.DataFrame(streamData[1:], columns=streamData[0]) 
            
            return df

    try:    
        # Authenticate with your NRGSTREAM username and password contained in credentials.txt, file format = username,password 
        #f = open(r"C:\Users\micoconnell\OneDrive - Suncor Energy Inc\Desktop\PythonRepo\AnnualDetectionChange\credentials.txt", "r")
        
        
        #credentials = f.readline().split(',')
        reedus = 2594
        #reedus = int(reedus)
        #f.close()
        nrgStreamApi = NRGStreamApiGasGen("micoconnell@suncor.com","anisoTropical308")         
        # Date range for your data request
        # Date format must be 'mm/dd/yyyy hh:ss'
        fromDateStr = yesterday
        toDateStr = today

        # Specify output format - 'csv' or 'json'
        dataFormat = 'csv'
        
        # Convert streams to Pandas dataframes
        # Only compatible with getByStream and getByFolder
        dataFrameConvert = True
        
        # Data Option
        dataOption = 'ALL'
        
        # Output from the API request is written to the stream_data variable
        stream_data = ""

        # Output from the API request is written to the streamList variable
        streamList = ""
        
        # Output from the API request is written to the folderList variable
        folderList = ""
        
        # Output from the API request is written to the groupExtractsList variable
        groupExtractsList = ""
        

        # Change to True to get streams by Stream Id
        getByStream = True    
        if getByStream:
            # Pass in individual stream id
            streamIds = [reedus]       
            # Or pass in list of stream ids
            #streamIds = [139308, 3, 225, 4117, 17, 545, 40034]         
            stream_data = nrgStreamApi.GetStreamDataByStreamId(streamIds, fromDateStr, toDateStr, dataFormat, dataOption) 
            
            if(dataFrameConvert and dataFormat == 'csv'):
                if(len(streamIds) > 1):
                    print('Please only convert 1 stream to a Pandas dataframe at a time')
                else:
                    stream_data = nrgStreamApi.csvStreamToPandas(stream_data)
            print (stream_data)
            
        # Change to True to get streams by Folder Id
        getByFolder = False
        if getByFolder:
            # Pass in individual folder id
            folderId = 2594
            nrgStreamApi.GetStreamDataByFolderId(folderId, fromDateStr, toDateStr, dataFormat)

        # Change to True to retrieve a list of data options available for a given stream
        getStreamDataOptions = False
        if getStreamDataOptions:          
            # Pass in a list of streamIds to get the shapes available for each
            # These shapes can be passed to StreamData endpoint as 'displayOption' to retrieve only that shape
            streamIds = 2594   
            streamDataOptions = nrgStreamApi.StreamDataOptions(streamIds, dataFormat)
            print(streamDataOptions)
    
        
    except Exception as e:
        print(str(e))
        

    
    stream_data = stream_data.rename(columns={'Trade Date': 'DateModified', 'Instrument Date': 'BeginDate','Low':'Low','Volume':'TotalVolume','Open Interest':'netOI','WAvg':'Open'})
    stream_data['BeginDate'] = pd.to_datetime(stream_data['BeginDate'])
    stream_data['EndDate'] = stream_data['BeginDate']
    stream_data['DateModified'] = pd.to_datetime(stream_data['DateModified'])
    DateMod = datetime.today()
    DateMod = DateMod.strftime('%Y-%m-%d')
    stream_data['DateModified'] = DateMod
    #print(stream_data)


    collist=stream_data['BeginDate']
    #print(collist)
    d = []
    i=0
    while i < len(collist):
        de = pd.Period(collist[i],freq='M').end_time.date()
        d.append(de)
        i= i+1
    stream_data['EndDate'] = d
    
    stream_data['Trades'] = 0
 
    stream_data['DateModified'] = pd.to_datetime(stream_data['DateModified'])
    stream_data['EndDate'] = pd.to_datetime(stream_data['EndDate'])
    stream_data['Trades']= pd.to_numeric(stream_data["Trades"],errors="ignore")
    stream_data['TotalVolume']=pd.to_numeric(stream_data["TotalVolume"],errors="ignore")
    stream_data['Open']=pd.to_numeric(stream_data["Open"],errors="ignore")
    stream_data['High']= pd.to_numeric(stream_data["High"],errors="ignore")
    stream_data['Low']=pd.to_numeric(stream_data["Low"],errors="ignore")
    stream_data['Settle']= pd.to_numeric(stream_data["Settle"],errors="ignore")
    stream_data['netOI']= pd.to_numeric(stream_data["netOI"],errors="ignore")
    stream_data.set_index('DateModified',inplace=True)
    finalDF = stream_data.to_csv()
    date_today = datetime.today().strftime('%d%m%Y')
    date_today = date_today + '.csv'
    # stream_data= stream_data.drop(stream_data.columns['DateModified'], axis=1)
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientCOAL1 = blob_service_client.get_container_client("openinterestcsv")
    blob_client1= container_clientCOAL1.get_blob_client(date_today)
    container_clientCOAL1= blob_client1.upload_blob(finalDF,overwrite=True)

