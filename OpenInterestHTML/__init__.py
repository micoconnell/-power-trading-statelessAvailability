import datetime
import logging
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import azure.functions as func
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import psutil
import kaleido
from azure.storage.blob import BlobServiceClient
from io import BytesIO

from plotly.subplots import make_subplots
import plotly.io as pio






def main(mytimer: func.TimerRequest):

    def init_connection():
        
        server =r'tcp:supowerdatabase.database.windows.net' 
        database =r'SUpowerFinancials' 
        username =r'micoconnell1' 
        password =r'anisoTropical+308'
        driver= r'{ODBC Driver 17 for SQL Server}'
        
        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' +
        server + ';PORT=1433;DATABASE=' + database +
        ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        return cursor

    cursor = init_connection()


    result = cursor.execute("SELECT * FROM dbo.OpenInterestProd WHERE DateModified > '2022-12-01' ORDER BY DateModified DESC ")
    rows = result.fetchall()
    cols = []
    #result = cursor.execute('SELECT * FROM dbo.AILForecasts ORDER BY dateModified DESC')

    for i,_ in enumerate(result.description):
        cols.append(result.description[i][0])
        

    df = pd.DataFrame(np.array(rows), columns=cols)

    def yesterday(frmt='%Y-%m-%d', string=True):
        
        yesterday = datetime.now() - timedelta(0)
        if string:
            return yesterday.strftime(frmt)
        return yesterday

    frmt='%Y-%m-%d'        
    currentMonth = datetime.today().replace(day=1)
    currentMonth=currentMonth.strftime(frmt)

    def daybefore(frmt='%Y-%m-%d', string=True):
        
        yesterday = datetime.now() - timedelta(1)
        if string:
            return yesterday.strftime(frmt)
        return yesterday

    def dateFilter(frmt='%Y-%m-%d', string=True):
        
        yesterday = datetime.now() + timedelta(1100)
        if string:
            return yesterday.strftime(frmt)
        return yesterday

    yesterday=yesterday()
    daybefore=daybefore()
    dateFilter=dateFilter()
    df['BeginDate'] =  pd.to_datetime(df['BeginDate'])
    df['EndDate'] =  pd.to_datetime(df['EndDate'])
    df['DateModified'] =  pd.to_datetime(df['DateModified'])
    df['timeElasped'] = df['EndDate'] - df['BeginDate']
    df['timeElasped'] = df['timeElasped'] + np.timedelta64(1, 'D')
    df['timeElasped'] = (df['timeElasped'].values.astype(float)) / 86400000000000
    df['netOI'] = df['netOI'].replace(',','', regex=True)
    df['netOIMW'] = (df['netOI'].values.astype(float))
    df = df[df['timeElasped'] > 27 ]
    df['netOIMW'] = df['netOIMW'] / (df['timeElasped'] * 24)
    #print(df)
    df_yesterday = df[df['DateModified'] == yesterday]
    df_daybefore = df[df['DateModified'] == daybefore]
    # print(df_yesterday)
    # print(df_daybefore)
    #df_daybefore = df_daybefore[df_daybefore['DateModified'] == yesterday]


    df_yesterday = df_yesterday.drop_duplicates()
    df_daybefore = df_daybefore.drop_duplicates()
    df_daybefore['netOIMW']= pd.to_numeric(df_daybefore['netOIMW'])
    df_daybefore= df_daybefore.set_index('BeginDate')

    df_yesterday['netOIMW']= pd.to_numeric(df_yesterday['netOIMW'])
    df_yesterday= df_yesterday.set_index('BeginDate')

    df_yesterdayseries = df_yesterday['netOIMW']
    df_daybeforeseries = df_daybefore['netOIMW']
    differences = df_yesterdayseries - df_daybeforeseries
    differences = differences.reset_index()
    #df_yesterday= df_yesterday.set_index('BeginDate')



    differences['BeginDate'] = pd.to_datetime(differences['BeginDate'])
    differences= differences.set_index('BeginDate')
    df_yesterday = df_yesterday[df_yesterday.index < dateFilter]
    differences = differences[differences.index < dateFilter]


    
    df_yesterday = df_yesterday[df_yesterday.index >= currentMonth]
    differences = differences[differences.index >= currentMonth]
    #df_yesterday.drop(df_yesterday.tail(1).index,inplace=True)
    df_yesterday = df_yesterday.sort_index()
    print(df_yesterday)
    #differences = differences.drop_duplicates()
    differences= differences.dropna()
    #print(differences)
    differences["Color"] = np.where(differences["netOIMW"]<0, 'red', 'blue')
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Bar(x=differences.index, y=differences.netOIMW,marker_color=differences.Color, name=" New Open Interest (MW)", text=differences.netOIMW),
        secondary_y=True,
    )
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig.add_trace(
        go.Scatter(x=df_yesterday.index, y=df_yesterday.netOIMW, name="Total Open Interest (MW)"),
        secondary_y=False,
    )

    fig.update_layout(yaxis_range=[0,2000])
    fig.update_layout(yaxis2_range=[-150,200])
    fig.update_layout(
            title={
                'text': "Open Interest (MW)" +"  " + yesterday,
                'y':.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
                )
    fig.update_yaxes(title_text="<b>Net Open Interest</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Net Change (MW)</b>", secondary_y=True)
    fig.update_xaxes(title_text="Date (Month)")
    fig.update_layout(
    autosize=True,
    width=1500,
    height=600,
    
    )
    #fig.update_traces(text=df_yesterday.netOIMW)
    #fig.update_traces(textposition='to center')

    
    output = fig.to_html()
    eventName = "master.html"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("openinteresthtml")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(output,overwrite=True)