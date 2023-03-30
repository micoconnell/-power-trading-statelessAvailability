import datetime
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:
    url = ' '
    df = pd.read_html(url)
    df = df[0]
    df.columns = [df.iloc[0][i] for i in range(len(df.columns))]
    df = df.drop(df.index[0])
    # split Posting Date by semicolon
    df[['Posting Date', 'Effective Date']] = df['Posting Date'].str.split('; ', expand=True)

    # convert Posting Date to datetime
    df['Posting Date'] = pd.to_datetime(df['Posting Date'])

    # move Effective Date to a new row if it exists
    if 'Effective Date' in df.columns:
        df = df.rename(columns={'Effective Date': df.columns[-1] + ' (Effective)'})
        df = df.reindex(columns=[*df.columns[:-1], df.columns[-1]])
    print(df)
    df = df.set_index("ID Number")
    df = df.to_csv(index=True)
    eventName = "monthlyinfo.csv"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("aesoinfo")
    blob_client = container_client.get_blob_client(eventName)
    container_client = blob_client.upload_blob(df,overwrite=True)