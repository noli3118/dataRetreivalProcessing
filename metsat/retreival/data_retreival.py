from IPython.display import *
import datetime
import shutil
import eumdac
import requests
import time
from dask.distributed import LocalCluster

if __name__ == '__main__':
    cluster = LocalCluster()
    client = cluster.get_client()
    print(client.dashboard_link)

    api_key = '8fE_zRdHknUuhjW6MTUB1mWB1XMa'
    api_secret = 'ulAzJ2ny5u1pvWKf4fnpX2ICoiQa'

    cred = (api_key, api_secret)

    token = eumdac.AccessToken(cred)

    try:
        print(f'This token: {token} expires on {token.expiration}')
    except requests.exceptions.HTTPError as error:
        print(f'Unexpected error: {error}')

    dstore = eumdac.DataStore(token)

    area = '36.21, 11.86, 55.45, 39.09'
    start = datetime.datetime(2024, 1, 31, 19, 50)
    end = datetime.datetime(2024, 1, 31, 22, 0)

    try:
        # for col in dstore.collections:
            # products = col.search(bbox=area, 
            #     dtstart=start, 
            #     dtend=end,
            #     sort="start,time,1")
            # if (products.total_results): print(f'Found collection: {col} with {products.total_results} datasets.')
        col = dstore.get_collection("EO:EUM:DAT:MSG:HRSEVIRI-IODC")
        latest_products = col.search(bbox=area, dtstart=start, dtend=end, sort="start,time,1")
    #Exception Handling
    except eumdac.datastore.DataStoreError as err:
        print(f'Error related to the EUDataStore: {err.msg}')
    except eumdac.collection.CollectionError as err:
        print(f"Error related to a collection: '{err.msg}'")
    except requests.exceptions.RequestException as err:
        print(f"Unexpected error: {err}")
    # print(list(latest))
    for entry in latest_products:
        try:
            print(entry)
        except eumdac.product.ProductError as error:
            print(f"Error related to the product: '{error.msg}'")
        except requests.exceptions.ConnectionError as error:
            print(f"Error related to the connection: '{error.msg}'")
        except requests.exceptions.RequestException as error:
            print(f"Unexpected error: {error}")
    latest_winter_product = latest_products.first()
    try:
        print(f"Selected product {latest_winter_product}")
    except eumdac.datastore.DataStoreError as error:
        print(f"Error related to the data store: '{error.msg}'")
    except eumdac.collection.CollectionError as error:
        print(f"Error related to the collection: '{error.msg}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")

    datatailor = eumdac.DataTailor(token)

    # To check if Data Tailor works as expected, we are requesting our quota information
    try:
        display(datatailor.quota)
    except eumdac.datatailor.DataTailorError as error:
        print(f"Error related to the Data Tailor: '{error.msg}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")

    # try:
    #     with latest_winter_product.open(entry='manifest.xml') as fsrc, \
    #             open(fsrc.name, mode='wb') as fdst:
    #         shutil.copyfileobj(fsrc, fdst)
    #         print(f'Download of file {fsrc.name} finished.')
    # except eumdac.product.ProductError as error:
    #     print(f"Error related to the product '{latest_winter_product}' while trying to download it: '{error.msg}'")
    # except requests.exceptions.ConnectionError as error:
    #     print(f"Error related to the connection: '{error.msg}'")
    # except requests.exceptions.RequestException as error:
    #     print(f"Unexpected error: {error}")

    # with open('manifest.xml') as f:
    #     print(f.read())

