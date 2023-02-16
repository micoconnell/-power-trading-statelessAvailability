from datetime import datetime
from datetime import timedelta
import logging
import pyodbc
import azure.functions as func
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import date
import requests


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
    twentyfour_months = today + relativedelta(months=+25, days=-1)
    twentyfour_months = str(twentyfour_months)
    twentyfour_months= twentyfour_months.replace("-","")

    four_months = today + relativedelta(months=+4, days=-1)
    four_months = str(four_months)
    four_months= four_months.replace("-","")

    result = cursor.execute("SELECT * FROM dbo.gasAB WHERE DateNum <= {0}  ORDER BY DateNum ".format(twentyfour_months))
    rows = result.fetchall()
    cols = []

    for i,_ in enumerate(result.description):
        cols.append(result.description[i][0])

    df = pd.DataFrame(np.array(rows), columns=cols)
    df = df[df['DateNum'] > four_months]
    df = df.set_index('CalendarDate')
    s_sub = df.loc[pd.date_range(df.index.min(), df.index.max(), freq='M')]
    s_sub = s_sub['AESO']
    #s_sub = str(s_sub)
    #s_sub = pd.to_datetime(s_sub,errors='coerce')
    print(s_sub)
    # DBA = df.iloc[:,13:95]
    # DBA['DateNum'] = df['DateNum']
    # DBA['Sum']=DBA.iloc[:,1:80].sum(axis=1)
    # DBA = DBA.apply(pd.to_numeric, errors='coerce')
    # DBA['Sum']= DBA['Sum'] - DBA['BR4'] - DBA['BR5'] - DBA['KH2'] - DBA['KH3'] - DBA['SH1'] - DBA['SH2'] - DBA['SD6']
    # DBA = DBA.set_index('DateNum')
    # DBAB = DBA['Sum']
    # print(DBAB)
    # # FinalDbUpdate = df['DateNum']
    # # FinalDbUpdate['Sum'] = DBA['Sum']  
    # # #FinalDbUpdate = FinalDbUpdate.set_index('DateNum')
    # # print(FinalDbUpdate)
    # # FinalDbUpdate.to_csv("s.csv")
    # def outageAESO(DF,DataBaseGrouping):
    #     DF=DF
    #     DataBaseGrouping = DataBaseGrouping
    #     i=0
    #     for ind in DF.index:
            
    #         first_day_of_month = ind
    #         print(ind)
    #         volume = DF.values[i]
    #         #tempVal = DF[i]
    #         #print(tempVal)
    #         i=i+1
    #         print(DataBaseGrouping)
    #         response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype={0}&asset=KnownUnknown&datestart={1}&dateend={2}&volume={3}".format(DataBaseGrouping,ind,ind,volume))
    #         print(response)
              
    # outageAESO(DBAB,"gasAB")
