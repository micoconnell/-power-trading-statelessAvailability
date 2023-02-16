from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import requests
import azure.functions as func
import pandas as pd
import lxml
from dateutil.relativedelta import relativedelta
import time
import re
def main(mytimer: func.TimerRequest) -> None:
    #Function below gets the dates needed. Will update the AESO column in all Databases.
    
    
    today = date.today().replace(day=1)
    four_months = today + relativedelta(months=+3)
    four_months = pd.to_datetime(four_months).date()
    first_day_of_month = str(today)
    first_day_of_month=first_day_of_month.replace("-","")
    
    last_date_of_month = date(four_months.year, four_months.month, 1) + relativedelta(months=1, days=-1)
    last_date_of_month = str(last_date_of_month)
    last_date_of_month=last_date_of_month.replace("-","")
    print(first_day_of_month,last_date_of_month)
    # twentyfour_months = today + relativedelta(months=+24)
    # #print(four_months,twentyfour_months)
    # CurrentEndAESOUPDATE = None
    URLMONTH = 'http://ets.aeso.ca/ets_web/ip/Market/Reports/DailyOutageReportServlet?contentType=html'
    DFMONTHLY = pd.read_html(URLMONTH)
    DFMONTHLY = DFMONTHLY[1][3:]
    DFMONTHLY.columns = DFMONTHLY.iloc[0]
    DFMONTHLY = DFMONTHLY[1:]
    
    DFMONTHLY['Date'] = pd.to_datetime(DFMONTHLY['Date'])
    DFMONTHLY['Date'] = DFMONTHLY['Date'].dt.date
    DFMONTHLY = DFMONTHLY.set_index('Date')
    print(DFMONTHLY)
    # # Lame Timestamp [NS] Conversion. 


    dfCoal = DFMONTHLY['Coal']
    dfGas = DFMONTHLY['Gas']
    dfDual = DFMONTHLY['Dual Fuel']
    dfHydro = DFMONTHLY['Hydro']
    dfSolar = DFMONTHLY['Solar']
    dfWind = DFMONTHLY['Wind']
    dfEnergy = DFMONTHLY['Energy Storage']
    dfOther = DFMONTHLY['Biomass and Other']
    print(dfCoal)
    def outageAESO(DF,DataBaseGrouping):
        DF=DF
        DataBaseGrouping = DataBaseGrouping
        i=0
        for ind in DF.index:
            
            first_day_of_month = ind
            first_day_of_month = str(first_day_of_month)
            first_day_of_month=first_day_of_month.replace("-","")
            
            last_date_of_month = first_day_of_month
            tempVal = DF[i]
            print(tempVal)
            i=i+1
            print(DataBaseGrouping)
            response = requests.get("https://azureeventtest.azurewebsites.net/api/httptrigger1?gentype={0}&asset=AESO&datestart={1}&dateend={2}&volume={3}".format(DataBaseGrouping,first_day_of_month,last_date_of_month,tempVal))
            print(response)
            time.sleep(2)   
    outageAESO(dfCoal,"coalAB")
    outageAESO(dfGas,"gasAB")
    outageAESO(dfDual,"dualAB")
    outageAESO(dfHydro,"hydroAB")
    outageAESO(dfSolar,"solarAB")
    outageAESO(dfWind,"windAB")
    outageAESO(dfEnergy,"energyAB")
    outageAESO(dfOther,"otherAB")