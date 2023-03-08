import datetime
import logging
import requests
import pandas as pd
import re
import numpy as np
import azure.functions as func
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import xlrd
import dataframe_image as dfi


def main(mytimer: func.TimerRequest) :

    ## Read Drone

    def make_equal_size(df1, df2):
        """
        Add rows with empty values to the smaller dataframe until both dataframes are of equal size.
        
        Parameters:
            df1 (pandas.DataFrame): First dataframe.
            df2 (pandas.DataFrame): Second dataframe.
        
        Returns:
            Tuple of two pandas.DataFrame objects: Dataframes of equal size.
        """
        if len(df1) > len(df2):
            diff = len(df1) - len(df2)
            new_rows = pd.DataFrame({col: ['']*diff for col in df2.columns})
            df2 = pd.concat([df2, new_rows], ignore_index=True)
        elif len(df2) > len(df1):
            diff = len(df2) - len(df1)
            new_rows = pd.DataFrame({col: ['']*diff for col in df1.columns})
            df1 = pd.concat([df1, new_rows], ignore_index=True)
        return df1, df2
    
    
    
    # ##MAKE them the same shape. Because they are empty, it can be evaluated as zero!!


    ## This now returns all rows that 5
    # df_filtered = df_zeros[(df_zeros == 5).any(axis=1)]
    # print(df_filtered)
    

    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_clientDrone = blob_service_client.get_container_client("transmissiondrone")    
    blob_client = container_clientDrone.get_blob_client("transmissiondrone.csv")
    dfDrone = blob_client.download_blob()
    dfDrone = pd.read_csv(dfDrone,index_col=0)

 
    ## Read Master file
    container_clientDrone = blob_service_client.get_container_client("transmissionmaster")    
    blob_client = container_clientDrone.get_blob_client("transmissionmaster.csv")
    dfMaster = blob_client.download_blob()
    dfMaster = pd.read_csv(dfMaster,index_col=0)



    dfDrone['Complete Date']= pd.to_datetime(dfDrone['Complete Date'])
    dfDrone['Start Date']= pd.to_datetime(dfDrone['Start Date'])

    dfDrone['Outage Type']= dfDrone['Outage Type'].astype('string')
    dfDrone['Outage Status']= dfDrone['Outage Status'].astype('string')
    dfDrone['Outage #']= dfDrone['Outage #'].astype('string') 

    dfDrone.reset_index(drop=True,inplace=True)
    dfDrone = dfDrone.set_index(['Outage #'])
    dfDrone.sort_index(ascending=True,inplace=True)



    dfMaster['Complete Date']= pd.to_datetime(dfMaster['Complete Date'])
    dfMaster['Start Date']= pd.to_datetime(dfMaster['Start Date'])

    dfMaster['Outage Type']= dfMaster['Outage Type'].astype('string')
    dfMaster['Outage Status']= dfMaster['Outage Status'].astype('string')
    # dfMaster['Outage #']= dfMaster['Outage #'].astype('string')
    dfMaster.reset_index(drop=True,inplace=True)
    #dfMaster = dfMaster.set_index(['Outage #'])   
    dfMaster.sort_index(ascending=True,inplace=True)
    dfMaster.replace('', np.nan, inplace=True)   
    dfDrone.replace('', np.nan, inplace=True)
    dfDrone = dfDrone.dropna()
    dfMaster = dfMaster.dropna() 
    # print(dfMaster['Complete Date'])
    # print(dfDrone['Complete Date'])
    # print(dfMaster['Complete Date'].equals(dfDrone['Complete Date']))
    dfDronetoMasterStagnant = dfMaster
    droneSizeCol = dfDrone.shape[1]
    droneSize = dfDrone.shape[0]
    masterSizeCol = dfMaster.shape[1]
    masterSize = dfMaster.shape[0]
    df_zerosSameSize = dfDrone.applymap(lambda x: 0)
    df_zerosDroneBigger = dfDrone.applymap(lambda x: 0)
    df_zerosDroneBigger = pd.DataFrame(dfDrone).reindex(columns=dfDrone.columns)
    df_zerosMasterBigger = dfMaster.applymap(lambda x: 0)    

    num_rows_Drone = dfDrone.shape[0]
    num_rows_Master = dfMaster.shape[0]
    
    
        
    ##Make them the same size
    if droneSize != masterSize:
        if droneSize > masterSize:
            df_zerosSameSize = dfDrone.applymap(lambda x: 0)
            dfDrone,dfMaster = make_equal_size(dfDrone,dfMaster)
            print(dfDrone)
            for x in range(0,droneSize):
                for y in range(0,droneSizeCol):
                    row = dfDrone.iloc[x][y]
                    rowMaster = dfMaster.iloc[x][y]
                    if row == rowMaster:
                        df_zerosSameSize.iloc[x][y] = 1
                    else:
                        df_zerosSameSize.iloc[x][y] = 5
            print("Drone is bigger")
            df_filtered = df_zerosSameSize[(df_zerosSameSize == 5).any(axis=1)] 
            subset_df1 = dfDrone.loc[df_filtered.index]
            subset_df1.replace('', np.nan, inplace=True)
            dfMaster.replace('', np.nan, inplace=True)   
            dfDrone.replace('', np.nan, inplace=True)          
            subset_df1 = subset_df1.dropna()
            dfDrone = dfDrone.dropna()
            dfMaster = dfMaster.dropna()                
            ##Update Master file
            dfDroneCSV = dfDrone.to_csv()
            container_clientMaster = blob_service_client.get_container_client("transmissionmaster")
            blob_client = container_clientMaster.get_blob_client("transmissionmaster.csv")
            container_clientMaster= blob_client.upload_blob(dfDroneCSV,overwrite=True)
            ## New HTML file for new changes
            dfFinalHTML = subset_df1.to_html().replace('<th>','<th style="background-color: royalblue; color: white">').replace('<td>','<td style="background-color: lightcoral;">')
            container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdate")
            blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdate.html")
            container_clientHTMLupdate= blob_client.upload_blob(dfFinalHTML,overwrite=True)
            
            ##Old Schedule (Full)                    
            dfMasterHTML = dfMaster.to_html().replace('<th>','<th style = "background-color: royalblue; color: white">')
            container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdateold")
            blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdateold.html")
            container_clientHTMLupdate= blob_client.upload_blob(dfMasterHTML,overwrite=True)

            ## New scheudle (HTML)
            dfDroneHTML = dfDrone.to_html().replace('<th>','<th style = "background-color: royalblue; color: white">')
            container_clientHTML = blob_service_client.get_container_client("transmissionhtml")
            blob_client = container_clientHTML.get_blob_client("transmission.html")
            container_clientHTML= blob_client.upload_blob(dfDroneHTML,overwrite=True)
                     
        if droneSize < masterSize:
            df_zerosSameSize = dfMaster.applymap(lambda x: 0)
            dfDrone,dfMaster = make_equal_size(dfDrone,dfMaster)
            for x in range(0,masterSize):
                for y in range(0,droneSizeCol):
                    row = dfMaster.iloc[x][y]
                    rowMaster = dfDrone.iloc[x][y]
                    if row == rowMaster:
                        df_zerosSameSize.iloc[x][y] = 1
                    else:
                        df_zerosSameSize.iloc[x][y] = 5
            print("Drone is Smaller")
            df_filtered = df_zerosSameSize[(df_zerosSameSize == 5).any(axis=1)]
            subset_df1 = dfMaster.loc[df_filtered.index]
            subset_df1.replace('', np.nan, inplace=True)
            dfMaster.replace('', np.nan, inplace=True)   
            dfDrone.replace('', np.nan, inplace=True)   
            subset_df1 = subset_df1.dropna()
            dfDrone = dfDrone.dropna()
            dfMaster = dfMaster.dropna()
            ##Update Master file
            dfDroneCSV = dfDrone.to_csv()
            container_clientMaster = blob_service_client.get_container_client("transmissionmaster")
            blob_client = container_clientMaster.get_blob_client("transmissionmaster.csv")
            container_clientMaster= blob_client.upload_blob(dfDroneCSV,overwrite=True)
            ## New HTML file for new changes
            dfFinalHTML = subset_df1.to_html().replace('<th>','<th style="background-color: royalblue; color: white">').replace('<td>','<td style="background-color: lightcoral;">')
            container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdate")
            blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdate.html")
            container_clientHTMLupdate= blob_client.upload_blob(dfFinalHTML,overwrite=True)
            
            ##Old Schedule (Full)                    
            dfMasterHTML = dfMaster.to_html().replace('<th>','<th style = "background-color: royalblue; color: white">')
            container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdateold")
            blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdateold.html")
            container_clientHTMLupdate= blob_client.upload_blob(dfMasterHTML,overwrite=True)

            ## New scheudle (HTML)
            dfDroneHTML = dfDrone.to_html().replace('<th>','<th style = "background-color: royalblue; color: white">')
            container_clientHTML = blob_service_client.get_container_client("transmissionhtml")
            blob_client = container_clientHTML.get_blob_client("transmission.html")
            container_clientHTML= blob_client.upload_blob(dfDroneHTML,overwrite=True)
    elif droneSize == masterSize:
        df_zerosSameSize = dfDrone.applymap(lambda x: 0)
        for x in range(0,droneSize):
            for y in range(0,droneSizeCol):
                row = dfDrone.iloc[x][y]
                rowMaster = dfMaster.iloc[x][y]
                if row == rowMaster:
                    df_zerosSameSize.iloc[x][y] = 1
                else:
                    df_zerosSameSize.iloc[x][y] = 5


        ##The problem is here. Because we are looking by index. 
        df_filtered = df_zerosSameSize[(df_zerosSameSize == 5).any(axis=1)]
        print(df_zerosSameSize)
        
        subset_df1 = dfDrone.loc[df_filtered.index]
        subset_df1.replace('', np.nan, inplace=True)
        dfMaster.replace('', np.nan, inplace=True)   
        dfDrone.replace('', np.nan, inplace=True)       
        
        subset_df1 = subset_df1.dropna()
        dfDrone = dfDrone.dropna()
        dfMaster = dfMaster.dropna()
        print(subset_df1)
        if subset_df1.empty == True:
            print("Pass")
        else:
            
            ##Update Master file
            dfDroneCSV = dfDrone.to_csv()
            container_clientMaster = blob_service_client.get_container_client("transmissionmaster")
            blob_client = container_clientMaster.get_blob_client("transmissionmaster.csv")
            container_clientMaster= blob_client.upload_blob(dfDroneCSV,overwrite=True)
            ## New HTML file for new changes
            dfFinalHTML = subset_df1.to_html().replace('<th>','<th style="background-color: royalblue; color: white">').replace('<td>','<td style="background-color: lightcoral;">')
            container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdate")
            blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdate.html")
            container_clientHTMLupdate= blob_client.upload_blob(dfFinalHTML,overwrite=True)
            
            ##Old Schedule (Full)                    
            dfMasterHTML = dfMaster.to_html().replace('<th>','<th style = "background-color: royalblue; color: white">')
            container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdateold")
            blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdateold.html")
            container_clientHTMLupdate= blob_client.upload_blob(dfMasterHTML,overwrite=True)

            ## New scheudle (HTML)
            dfDroneHTML = dfDrone.to_html().replace('<th>','<th style = "background-color: royalblue; color: white">')
            container_clientHTML = blob_service_client.get_container_client("transmissionhtml")
            blob_client = container_clientHTML.get_blob_client("transmission.html")
            container_clientHTML= blob_client.upload_blob(dfDroneHTML,overwrite=True)
            print("There was an equal dataframe but changes detected")











    # if num_rows_Drone > num_rows_Master:
    #         for x in range(0,droneSize):
    #             for y in range(0,masterSizeCol):
    #                 row = dfDrone.iloc[x][y]
    #                 rowMaster = dfMaster.iloc[x][y]
    #                 if row == rowMaster:
    #                     df_zeros.iloc[x][y] = 1
    #                 else:
    #                     df_zeros.iloc[x][y] = 5

    # # ## This now returns all rows that 5
    # # df_filtered = df_zeros[(df_zeros == 5).any(axis=1)]
    # # print(df_filtered) 
    
    
    
    
        
    # # Row = dfDrone["Outage #"][0]
    # # if dfDrone['Outage #'][0].isin(dfMaster['Outage #'][i]).any()

        
    # # # ## Take all data, and piecemeal it out to different functions. 
    # # def Transmission_Data(dfMaster,dfDrone):
    # #     dfMaster = dfMaster
    # #     dfDrone = dfDrone
        
    # #     num_rows_Drone = dfDrone.shape[0]
    # #     num_rows_Master = dfMaster.shape[0]
        
    # #     if num_rows_Drone != num_rows_Master:
    # #         df_all = dfDrone.merge(dfMaster.drop_duplicates(), on=['Outage #'], 
    #                how='left', indicator=True)
    #         print(df_all)
    #     # def compare_dataframes(df1, df2):
    #     #     """
    #     #     Compares two dataframes with the exact same structure and puts any changes into a new dataframe called df_changes
            
    #     #     Parameters:
    #     #     df1 (pandas.DataFrame): The first dataframe to compare
    #     #     df2 (pandas.DataFrame): The second dataframe to compare
            
    #     #     Returns:
    #     #     df_changes (pandas.DataFrame): A dataframe containing the rows that are different between the two dataframes
    #     #     """
    #     #     df1 = df1
    #     #     df2 = df2
    #     #     # Check that the two dataframes have the same structure
    #     #     if df1.shape != df2.shape:
    #     #         raise ValueError("The two dataframes have different shapes")
    #     #     if not df1.columns.equals(df2.columns):
    #     #         raise ValueError("The two dataframes have different column names or order")
            
    #     #     # Initialize an empty dataframe to hold the rows that are different
    #     #     df_changes = pd.DataFrame(columns=df1.columns)
            
    #     #     # Iterate through each row in df1 and compare it to the corresponding row in df2
    #     #     for i in range(df1.shape[0]):
    #     #         row1 = df1.iloc[i]
    #     #         row2 = df2.iloc[i]
                
    #     #         # Check if the two rows are different
    #     #         if not row1.equals(row2):
    #     #             # If they are different, append the row to the df_changes dataframe
    #     #             df_changes = df_changes.append(row1, ignore_index=True)
                    
    #     #     return df_changes
        
    #     # def remove_row_from_df(row, dfDrone):
    #     #     dfDrone = dfDrone
    #     #     row = row
    #     #     index = row.name
    #     #     dfDrone = dfDrone.drop(index)
    #     #     return dfDrone
            
            
            
            
            
    #     # def update_Master():
    #     #     pass
            
        
        
    #     # if num_rows_Drone != num_rows_Master:
    #     #     dfListedChanges = get_different_rows()
    #     #     row_to_remove = dfListedChanges
    #     #     dfDroneSansAdditons = remove_row_from_df(row_to_remove, dfDrone)
    #     #     df_changes = compare_dataframes(dfDroneSansAdditons,dfMaster)
    #     #     ## This returns a change log of updated entries
    #     #     #df_changeLogFinal = df_changes.append(dfListedChanges, ignore_index=True)
    #     #     return df_changes

    #     # else:
    #     #     df_changes = compare_dataframes(dfDrone,dfMaster)
    #     #     ## This returns a change log of updated entries
    #     #     return df_changes


    # dfFinal = Transmission_Data(dfDrone,dfMaster)
    # print(dfFinal)
    # # def updates_Azure(dfDrone,dfMaster,dfFinal):
    # #     dfDrone = dfDrone
    # #     dfMaster = dfMaster
    # #     dfFinal = dfFinal
        
    # # # UPDATE MASTER FILE
        # dfDroneCSV = dfDrone.to_csv()
        # container_clientMaster = blob_service_client.get_container_client("transmissionmaster")
        # blob_client = container_clientMaster.get_blob_client("transmissionmaster.csv")
        # container_clientMaster= blob_client.upload_blob(dfDroneCSV,overwrite=True)

    # # # UPLOAD JUST THE UPDATED TRANSMISSION OUTAGES
    # #     dfFinal = dfFinal.to_html()
    # #     container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdate")
    # #     blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdate.html")
    # #     container_clientHTMLupdate= blob_client.upload_blob(dfFinal,overwrite=True)

    # # # HTML FILE FOR NOW DEFUNCT OLDER SCHEDULE
    # #     dfMaster = dfMaster.to_html()
    # #     container_clientHTMLupdate = blob_service_client.get_container_client("transmissionupdateold")
    # #     blob_client = container_clientHTMLupdate.get_blob_client("transmissionupdateold.html")
    # #     container_clientHTMLupdate= blob_client.upload_blob(dfMaster,overwrite=True)

    # # # NEW HTML FILE FOR CURRENT SCHEDULE

    # #     ## Upload an html of the new *complete* schedule
    # #     dfDrone = dfDrone.to_html()
    # #     container_clientHTML = blob_service_client.get_container_client("transmissionhtml")
    # #     blob_client = container_clientHTML.get_blob_client("transmission.html")
    # #     container_clientHTML= blob_client.upload_blob(dfDrone,overwrite=True)


    # # if dfFinal.empty == False :
    # #     updates_Azure(dfDrone,dfMaster,dfFinal)
    # # print(dfFinal)




