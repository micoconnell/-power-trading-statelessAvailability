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
    cursor1 = init_connection()
    today = date.today().replace(day=1)
    next_month = today + relativedelta(months=+1, days=0)
    next_month = str(next_month)
    next_month= next_month.replace("-","")
    todayOI = date.today()
    todayOI = str(todayOI)
    
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
    
    def query_collectionOI():
        
        result = cursor1.execute("SELECT * FROM dbo.OpenInterestProd WHERE DateModified > '2023-03-29' ")

        rows = result.fetchall()
        cols = []

        for i,_ in enumerate(result.description):
            cols.append(result.description[i][0])

        df = pd.DataFrame(np.array(rows), columns=cols)
        return df
    
    
    OI = query_collectionOI()
    coal = query_collection("coalAB")
    gas = query_collection("gasAB")
    hydro = query_collection("hydroAB")
    interties = query_collection("intertieAB")
    dual = query_collection("dualfuelAB")
    
    OpenInterest = OI
    OpenInterest['DateModified'] = OpenInterest['DateModified'].apply(lambda x: x.strftime('%m%d%Y'))
    OpenInterest['BeginDate'] = OpenInterest['BeginDate'].apply(lambda x: x.strftime('%m%d%Y'))
    OpenInterest['EndDate'] = OpenInterest['EndDate'].apply(lambda x: x.strftime('%m%d%Y'))
    OpenInterest['CalendarDate'] = OpenInterest['DateModified']
    OpenInterest = OpenInterest[['CalendarDate','BeginDate','EndDate','netOI']]
    OpenInterest = OpenInterest[OpenInterest['BeginDate'] <= twentyfour_months]
    print(OpenInterest)
    
    
    
    
    coalFinal = coal[['CalendarDate','AESO']]
    coalFinal['CalendarDate'] = coalFinal['CalendarDate'].apply(lambda x: x.strftime('%m%d%Y'))
    coalFinal = coalFinal.set_index('CalendarDate',drop=True)
    
    
    gasFinal = gas[['CalendarDate','AESO']]
    gasFinal['CalendarDate'] = gasFinal['CalendarDate'].apply(lambda x: x.strftime('%m%d%Y'))
    gasFinal = gasFinal.set_index('CalendarDate',drop=True)
    
    
    
    
    
    hydroFinal = hydro[['CalendarDate','AESO']]
    hydroFinal['CalendarDate'] = hydroFinal['CalendarDate'].apply(lambda x: x.strftime('%m%d%Y'))
    hydroFinal = hydroFinal.set_index('CalendarDate',drop=True)
    
    
    
    intertiesFinal = interties[['CalendarDate','BCMATLIM']]
    intertiesFinal['CalendarDate'] = intertiesFinal['CalendarDate'].apply(lambda x: x.strftime('%m%d%Y'))
    intertiesFinal = intertiesFinal[['CalendarDate','BCMATLIM']]
    
    
    
    dualFinal = dual[['CalendarDate','AESO']]
    dualFinal['CalendarDate'] = dualFinal['CalendarDate'].apply(lambda x: x.strftime('%m%d%Y'))
    dualFinal = dualFinal[['CalendarDate','AESO']]
    
    merged_df = pd.merge(coalFinal,gasFinal,how='outer' ,on='CalendarDate')
    merged_df = merged_df.rename(columns={"AESO_x": "Coal_Outages", "AESO_y": "Gas_Outages"}, errors="raise")
    merged_df = pd.merge(merged_df,hydroFinal,how='outer' ,on='CalendarDate')
    merged_df = merged_df.rename(columns={"AESO": "Hydro_Outages"}, errors="raise")

    merged_df = pd.merge(merged_df,intertiesFinal,how='outer' ,on='CalendarDate')

    merged_df = pd.merge(merged_df,dualFinal,how='outer' ,on='CalendarDate')
    merged_df = merged_df.rename(columns={"AESO": "DualFuel_Outages"}, errors="raise")
    merged_df['BCMATLIM'] = merged_df['BCMATLIM'].apply(lambda x: 850 - x if x != 0 else x)
    
    
    
    
    
    
    csv = merged_df
    csv = csv.to_csv()
    df_json = (merged_df.set_index('CalendarDate')
             .apply(lambda x: x.astype(int).to_dict(), axis=1)
             .reset_index(name='outages')
             .to_json(orient='records'))

    merged_df = df_json
    #merged_df = pd.merge(merged_df, interties, on='CalendarDate')
    #merged_df = pd.merge(merged_df, hydro, on='CalendarDate')
    #merged_df = pd.merge(merged_df, dual, on='CalendarDate')
    #merged_df = merged_df.to_json()

    
    
    
    
    
    
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
    blob_client = container_clientCollated.get_blob_client("collatedLowRes.json")
    container_clientCollated= blob_client.upload_blob(merged_df,overwrite=True)
    
    container_clientCollatedCSV = blob_service_client.get_container_client("outageupdate")
    blob_clientCSV = container_clientCollatedCSV.get_blob_client("collatedLowRes.csv")
    container_clientCollatedCSV= blob_clientCSV.upload_blob(csv,overwrite=True)
    
    # # Assuming your dataframe is named `df`
    # json_list = []

    # # Loop through each row in the dataframe and create a JSON object for each date
    # for index, row in merged_df.iterrows():
    #     json_object = {
    #         "date": str(row["CalendarDate"]),
    #         "outages": {
    #             "Coal_Outages": int(row["Coal_Outages"]),
    #             "Gas_Outages": int(row["Gas_Outages"]),
    #             "Interties": int(row["BCMATLIM"]),
    #             "DualFuel_Outages": int(row["DualFuel_Outages"])
    #         }
    #     }
    #     json_list.append(json_object)
    

    
    
    #     type = DFSTORAGEJSON.columns[1]
    # DFSTORAGEJSON = {
    #     'type' : type,
    #     'dates' : {row['Month']: row[type] for _, row in DFSTORAGEJSON.iterrows()}
    # }
    # DFSTORAGEJSON = pd.DataFrame(data=DFSTORAGEJSON)
    # DFSTORAGEJSON = DFSTORAGEJSON.to_json()