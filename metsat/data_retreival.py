import eumdac
from IPython.core.display import HTML
import datetime
import shutil
import requests
import time
from dask.distributed import LocalCluster

if __name__ == '__main__':
    cluster = LocalCluster()
    client = cluster.get_client()
    print(client.dashboard_link)

    api_key = ''
    api_secret = ''

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
        latest = col.search(bbox=area).first()
        print(f'Latest product: {latest}')
    #Exception Handling
    except eumdac.datastore.DataStoreError as err:
        print(f'Error related to the EUDataStore: {err.msg}')
    except eumdac.collection.CollectionError as err:
        print(f"Error related to a collection: '{err.msg}'")
    except requests.exceptions.RequestException as err:
        print(f"Unexpected error: {err}")

    for entry in latest.entries:
        try:
            print(entry)
        except eumdac.product.ProductError as error:
            print(f"Error related to the product: '{error.msg}'")
        except requests.exceptions.ConnectionError as error:
            print(f"Error related to the connection: '{error.msg}'")
        except requests.exceptions.RequestException as error:
            print(f"Unexpected error: {error}")

    try: 
        with latest.open() as fsrc, \
                    open(fsrc.name, mode='wb') as fdst:
                shutil.copyfileobj(fsrc, fdst)
                print(f'Download of product {latest} finished.')
    except eumdac.product.ProductError as err:
            print(f"Error related to the product '{latest}' while trying to download it: '{err.msg}'")
    except requests.exceptions.ConnectionError as err:
        print(f"Error related to the connection: '{err.msg}'")
    except requests.exceptions.RequestException as err:
        print(f"Unexpected error: {err}")

    # try:
    #     with latest.open(entry='manifest.xml') as fsrc, \
    #             open(fsrc.name, mode='wb') as fdst:
    #         shutil.copyfileobj(fsrc, fdst)
    #         print(f'Download of file {fsrc.name} finished.')
    # except eumdac.product.ProductError as error:
    #     print(f"Error related to the product '{latest}' while trying to download it: '{error.msg}'")
    # except requests.exceptions.ConnectionError as error:
    #     print(f"Error related to the connection: '{error.msg}'")
    # except requests.exceptions.RequestException as error:
    #     print(f"Unexpected error: {error}")

    # with open('manifest.xml') as f:
    #     print(f.read())

