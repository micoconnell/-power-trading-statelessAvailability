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


    today = date.today().replace(day=1)
    twentyfour_months = today + relativedelta(months=+25, days=-1)
    twentyfour_months = str(twentyfour_months)
    twentyfour_months= twentyfour_months.replace("-","")
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


    
    result = cursor.execute("SELECT * FROM dbo.coalAB WHERE DateNum <= {0}  ORDER BY DateNum ".format(twentyfour_months))
    rows = result.fetchall()
    cols = []

    for i,_ in enumerate(result.description):
        cols.append(result.description[i][0])

    df = pd.DataFrame(np.array(rows), columns=cols)

    today = date.today().replace(day=1)



    def correct_Date(df):
        df=df
        
        i = 4
        j = 5
        while j <= 24:
            today = date.today().replace(day=1)


            four_months = today + relativedelta(months=i, days=1)
            four_months = str(four_months)
            four_months= four_months.replace("-","")
            
            
            four_monthsExact = today + relativedelta(months=i, days=0)
            four_monthsExact = str(four_monthsExact)
            four_monthsExact= four_monthsExact.replace("-","")
            
            four_monthsend = today + relativedelta(months=i, days=-1)
            four_monthsend = str(four_monthsend)
            four_monthsend= four_monthsend.replace("-","")
            
            five_months = today + relativedelta(months=j, days=0)
            five_months = str(five_months)
            five_months= five_months.replace("-","")
                        
            five_monthsend = today + relativedelta(months=j, days=-1)
            five_monthsend = str(five_months)
            five_monthsend= five_monthsend.replace("-","")
            
            i = i+1
            j = j+1
            dfa = df[df['DateNum'] > four_monthsend]
            dfa = dfa[dfa['DateNum'] < five_months]
            #dfa = dfa[dfa['DateNum'] <= four_monthsend]
            #dfa = dfa[dfa['DateNum'] < five_months]

            residual_calc = dfa['AESO'] -dfa['KnownUnknown']

            residual_calc = residual_calc.mean()
            residual_calc_AESO = dfa['AESO'].mean()
            #print(residual_calc.round())
            residual_calc = residual_calc.astype('float')
            dfa['AESO'] = dfa['AESO'].astype('float')
            final_residual = residual_calc_AESO - residual_calc
            dfa['StackModelOutages'] = dfa['AESO'] - final_residual

            #residual_calc =  dfa['AESO']
            # print(dfa['AESO'] - residual_calc)

            #print(four_monthsend)

            for x in range(0,len(dfa)):
                dateResponse = dfa.iloc[x]
                valueResponse = dateResponse['StackModelOutages']
                dateResponse = dateResponse['DateNum']
                print(valueResponse,dateResponse)
                response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype=coalAB&asset=StackModelOutages&datestart={0}&dateend={1}&volume={2}".format(dateResponse,dateResponse,valueResponse))
            
            
            
            
            
    correct_Date(df)



















    # df = df[df['DateNum'] > five_months]
    # print(df)
    #df['calendarDate'] = pd.to_datetime(df['calendarDate'])
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # df = df.set_index('CalendarDate')
    # target_month = four_months
    # df_filtered = df[df['CalendarMonth'] == five_months]
    # print(df_filtered)    
    
    
    # s_subDay = df.loc[pd.date_range(df.index.min(), df.index.max(), freq='D')]
    # s_sub = df.loc[pd.date_range(df.index.min(), df.index.max(), freq='M')]
    # print(s_subDay)
    # s_subAESO = s_sub[['AESO','CalendarDay','DateNum']]
    # s_subKnownUnknown = s_sub[['KnownUnknown','CalendarDay','DateNum']]
    # print(s_subKnownUnknown)
    #s_sub = str(s_sub)
    #s_sub = pd.to_datetime(s_sub,errors='coerce')

    #
    # for x in range(0,len(s_subAESO)):
    #     print(s_subKnownUnknown[x])
    #     df_filtered = s_subKnownUnknown[s_subKnownUnknown['CalendarDay'].dt.month == target_month]
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
