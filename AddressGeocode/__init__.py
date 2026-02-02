from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage import blob
from azure.storage.blob import BlobServiceClient
import pyodbc
import requests
import json
import sqlalchemy as sa
from sqlalchemy import create_engine, Integer, NVARCHAR, JSON
from urllib.parse import quote_plus #(Python 3)
import pandas as pd
import time
import io
import azure.functions as func
import json
import logging
import time


def main(body: str) -> str:

    """
    ################################################################################

    Author: Benjamin Smith
    Description: This function extracts address data from View Migration staging 
    databases, posts to Azure Maps (batch) Geocode API, gets the results from Azure 
    Maps and drops the resulting batch to a CSV in blob storage

    This function is not intended to be invoked directly; it will be triggered by
    an orchestrator function instead.

    Created for View Data Migration Program (DevOps Task 1832).

    Change History

    Date        Author                Change Reason
    ----------  --------------------  ----------------------------------------------
    2021-07-28  Benjamin Smith        Original version.
    2021-08-10  Benjamin Smith        Debugging

    ################################################################################
    """
    credential = DefaultAzureCredential()
    data_factory_name = body['dataFactoryName']

    key_vaults = {
        "<DatafactoryName>-dev": "https://<KVDev>.vault.azure.net/",
        "<DatafactoryName>-prd": "https://<kvPrd>.vault.azure.net/",
        "<DatafactoryName>-tst": "https://<kvTst.vault.azure.net/",
        "<DatafactoryName>-uat": "https:/<kvUAT>.vault.azure.net/"
    }

    logging.info("Retrieving secrets...")

    key_vault_url = key_vaults.get(data_factory_name)
    secret_client = SecretClient(vault_url = key_vault_url, credential = credential)
 

    subscription_key=secret_client.get_secret('<AzureMapsKeySecretName>').value
    server = secret_client.get_secret('<SQLServerNameSecret>').value
    database = secret_client.get_secret('<SQLDBSameSecret>').value
    username = secret_client.get_secret('<SQLUsernameSecret').value
    password = secret_client.get_secret('<SQLPasswordSecret>').value
    port = '1433'
    storage_name = secret_client.get_secret('<StorageURL>').value
    storage_secret = secret_client.get_secret('<StorageSecret>').value
    driver = '{ODBC Driver 17 for SQL Server}'#'{ODBC Driver 17 for SQL Server}'
    subscription_key = secret_client.get_secret('<AZMapsSubscriptionKey>')
    storage_container = secret_client.get_secret('<StorageContainerName>'
    
    logging.info("Secrets retrieved.")

    logging.info("Establishing blob account connection...")

    blob_account = BlobServiceClient(account_url=storage_name, credential=storage_secret)

    logging.info("Blob account connection established.")
    #parsed URL
    odbc_str = 'DRIVER='+ driver +';SERVER='+ server +';PORT='+ str(port) +';DATABASE='+ database +';UID='+ username +';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + quote_plus(odbc_str)

    #connect with sa url format
    sa_url = f"mssql+pyodbc:///{username}:{password}@{server}:{port}/{database}?driver={driver}"
    engine = create_engine(connect_str) 
     

    def getMaxGroup(max_group_query:str):
        """
        retrieve max group ID 
        
        """
        maxdf = pd.read_sql(max_group_query, connect_str)
        return int(maxdf['maxGroupId'])

    def address_processing(query:str):

        """ 
        Process Addresses : Extract , Cleanse and Load

        """

        dfp = pd.read_sql(query, connect_str)
        req = dfp['dataset'][0]
        url = f"https://atlas.microsoft.com/search/address/batch/json?subscription-key={subscription_key}&api-version=1.0"
        payload = json.loads(req)
        payload = json.dumps(payload)
        headers = {
        'Content-Type': 'application/json',
        'APIKey': subscription_key
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        
        #confirm API processing completed and get URL to result set
        newUrl = response.headers['Location']
        time.sleep(5)
        logging.info(newUrl)
        r = requests.get(newUrl)

        #get results
        i = 0
        logging.info(r.status_code)
        while r.status_code > 201 and i < 5:
            logging.warning('Not Ready, waiting for 30 seconds')
            time.sleep(30)
            r = requests.get(newUrl)
            i = i + 1
        j = r.json()
        try:
            j = j['batchItems']
        except:
            logging.critical(j)
        df = pd.DataFrame.from_dict(j) # pd.json_normalize(j) 
        batchItems = json.loads(req)

        #Compose dataframe
        sourcedf = pd.DataFrame.from_dict(batchItems['batchItems'])
        result = pd.merge(sourcedf, df, left_index=True, right_index=True)
        from datetime import date, datetime
        now = datetime.now()
        
        #output to blob
        filename = 'address_geo_response_'+now.strftime("%Y%m%d%H%M%S")+'.csv'
        logging.info('writing to file:  ' + filename)
        
        datafile=result.to_csv(sep="|", index = False, encoding="utf-8")
        logging.info('writing to blob:  ' + storage_name+'/NewFiles/Geocode/'+ filename)
        blob_client = blob_account.get_blob_client(container = {storage_container}, blob = 'NewFiles/Geocode/'+filename)
        blob_client.upload_blob(datafile,blob_type="BlockBlob")
        logging.info('writing to blob COMPLETE')
        
    query = "SELECT dataset from stg.AddressGeoLookupBatch where GroupId = 1"
    max_group_query = "SELECT max(GroupId) as maxGroupId from stg.AddressGeoLookupBatch"

    max_group = getMaxGroup(max_group_query)

###for debug
    #max_group = 9

#    t0 = time.time()

    for i in range(0,max_group+1):
        query = "SELECT dataset from stg.AddressGeoLookupBatch where GroupId = {i}".format(i=i)
        logging.info('processing group Id {i}'.format(i=i))
        address_processing(query)
        time.sleep(5)
 #       t1 = time.time() - t0    

 #       logging.info("Time elapsed: ", t1)
