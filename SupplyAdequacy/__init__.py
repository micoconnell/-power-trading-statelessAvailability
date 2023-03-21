from datetime import datetime
import logging
import requests
import pandas as pd
import re
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:


    ## Load Daily outage table and save results. Timer is set for every two minutes, starting at the top of the 
    ## hour. 
    from datetime import datetime
    import logging
    import requests
    import pandas as pd
    import re


 
    url2 = "http://ets.aeso.ca/ets_web/ip/Market/Reports/SupplyAdequacyReportServlet"
    df = pd.read_html(url2)
    df = df[1]
    new_columns = {}
    for i in range(1, 25):
        if i < 10:
            column_name = "Hour Ending 0" + str(i)
        else:
            column_name = "Hour Ending " + str(i)
        new_columns[str(i)] = column_name

    #df = df.rename(columns=new_columns)


    df.columns=df.iloc[0] 
    df=df.tail(-1)
    #df = df.set_index('HE')


    #df = df.fillna(method='ffill')
    
    print(df)
    #df2 = df2.apply(pd.to_numeric)`