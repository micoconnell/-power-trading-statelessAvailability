import logging
from datetime import timedelta
import pyodbc
import pandas as pd
import numpy as np
from datetime import date 
import azure.functions as func
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient


def main(mytimer: func.TimerRequest) -> None:

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

    today = date.today().replace(day=1)
    next_month = today + relativedelta(months=+1, days=0)
    next_month = str(next_month)
    next_month= next_month.replace("-","")
    
    
    
    twentyfour_months = today + relativedelta(months=+25, days=-1)
    twentyfour_months = str(twentyfour_months)
    twentyfour_months= twentyfour_months.replace("-","")

    def query_collection(database):
        database = str(database)
        result = cursor.execute("SELECT * FROM {0} WHERE DateNum >= {1} AND DateNum <= {2} ORDER BY DateNum".format(database,next_month,twentyfour_months))

        rows = result.fetchall()
        cols = []

        for i,_ in enumerate(result.description):
            cols.append(result.description[i][0])

        df = pd.DataFrame(np.array(rows), columns=cols)
        return df
    coal = query_collection("coalAB")
    
    
    gas = query_collection("gasAB")
    hydro = query_collection("hydroAB")
    interties = query_collection("intertieAB")
    dual = query_collection("dualfuelAB")
    coalFinal = coal[['CalendarDate','StackModelOutages']]
    coalFinal['CalendarDate'] = coalFinal['CalendarDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    coalFinal = coalFinal.set_index('CalendarDate',drop=True)
    
    
    gasFinal = gas[['CalendarDate','StackModelOutages']]
    gasFinal['CalendarDate'] = gasFinal['CalendarDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    gasFinal = gasFinal.set_index('CalendarDate',drop=True)
    
    
    
    
    
    hydroFinal = hydro[['CalendarDate','StackModelOutages']]
    hydroFinal['CalendarDate'] = hydroFinal['CalendarDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    hydroFinal = hydroFinal.set_index('CalendarDate',drop=True)
    
    
    
    intertiesFinal = interties[['CalendarDate','BCMATLIM']]
    intertiesFinal['CalendarDate'] = intertiesFinal['CalendarDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    intertiesFinal = intertiesFinal[['CalendarDate','BCMATLIM']]
    
    
    
    dualFinal = dual[['CalendarDate','StackModelOutages']]
    dualFinal['CalendarDate'] = dualFinal['CalendarDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    dualFinal = dualFinal[['CalendarDate','StackModelOutages']]
    
    merged_df = pd.merge(coalFinal,gasFinal,how='outer' ,on='CalendarDate')
    merged_df = merged_df.rename(columns={"StackModelOutages_x": "Coal_Outages", "StackModelOutages_y": "Gas_Outages"}, errors="raise")
    merged_df = pd.merge(merged_df,hydroFinal,how='outer' ,on='CalendarDate')
    merged_df = merged_df.rename(columns={"StackModelOutages": "Hydro_Outages"}, errors="raise")

    merged_df = pd.merge(merged_df,intertiesFinal,how='outer' ,on='CalendarDate')

    merged_df = pd.merge(merged_df,dualFinal,how='outer' ,on='CalendarDate')
    merged_df = merged_df.rename(columns={"StackModelOutages": "DualFuel_Outages"}, errors="raise")
    merged_df['BCMATLIM'] = merged_df['BCMATLIM'].apply(lambda x: 850 - x if x != 0 else x)
    print(merged_df)
    #merged_df = pd.merge(merged_df, interties, on='CalendarDate')
    #merged_df = pd.merge(merged_df, hydro, on='CalendarDate')
    #merged_df = pd.merge(merged_df, dual, on='CalendarDate')
    merged_df = merged_df.to_csv()

    
    
    
    
    
    
    #print(coal,gas,hydro,interties,dual)
    coal = coal.to_csv()
    gas = gas.to_csv()
    hydro = hydro.to_csv()
    interties = interties.to_csv()
    dual = dual.to_csv()
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")

    container_clientGAS = blob_service_client.get_container_client("outageupdate")
    blob_client = container_clientGAS.get_blob_client("gas.csv")
    container_clientGAS= blob_client.upload_blob(gas,overwrite=True)
    
    container_clientCoal = blob_service_client.get_container_client("outageupdate")
    blob_client = container_clientCoal.get_blob_client("coal.csv")
    container_clientCoal= blob_client.upload_blob(coal,overwrite=True)
    
    container_clientintertie = blob_service_client.get_container_client("outageupdate")
    blob_client = container_clientintertie.get_blob_client("intertie.csv")
    container_clientintertie= blob_client.upload_blob(interties,overwrite=True)
    
    container_clientDual = blob_service_client.get_container_client("outageupdate")
    blob_client = container_clientDual.get_blob_client("dual.csv")
    container_clientDual= blob_client.upload_blob(dual,overwrite=True)
    
    container_clientHydro = blob_service_client.get_container_client("outageupdate")
    blob_client = container_clientHydro.get_blob_client("hydro.csv")
    container_clientHydro= blob_client.upload_blob(hydro,overwrite=True)
    # blob_client = container_clientGAS.get_blob_client("gas.csv")
    # container_clientGAS= blob_client.upload_blob(gas)
    
    
    container_clientCollated = blob_service_client.get_container_client("outageupdate")
    blob_client = container_clientCollated.get_blob_client("collatedLowRes.csv")
    container_clientCollated= blob_client.upload_blob(merged_df,overwrite=True)