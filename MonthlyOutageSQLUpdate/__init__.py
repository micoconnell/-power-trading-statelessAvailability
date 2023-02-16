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
    four_months = today + relativedelta(months=+4)
    four_months = pd.to_datetime(four_months).date()
    print(type(four_months))
    twentyfour_months = today + relativedelta(months=+24)
    #print(four_months,twentyfour_months)
    CurrentEndAESOUPDATE = None
    URLMONTH = 'http://ets.aeso.ca/ets_web/ip/Market/Reports/MonthlyOutageForecastReportServlet?contentType=html'
    DFMONTHLY = pd.read_html(URLMONTH)
    DFMONTHLY = DFMONTHLY[2]
    # Lame Timestamp [NS] Conversion. 
    DFMONTHLY['Month'] = pd.to_datetime(DFMONTHLY['Month'])
    DFMONTHLY['Month'] = DFMONTHLY['Month'].dt.date

    DFMONTHLY = DFMONTHLY.round(0)
    DFMONTHLY = DFMONTHLY[DFMONTHLY['Month'] >= four_months]
    DFMONTHLY = DFMONTHLY.set_index('Month')
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
            
            first_day_of_month = ind.replace(day=1)
            first_day_of_month = str(first_day_of_month)
            first_day_of_month=first_day_of_month.replace("-","")
            
            last_date_of_month = date(ind.year, ind.month, 1) + relativedelta(months=1, days=-1)
            last_date_of_month = str(last_date_of_month)
            last_date_of_month=last_date_of_month.replace("-","")
            print(first_day_of_month,last_date_of_month)
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