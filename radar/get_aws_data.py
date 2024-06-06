import boto
from   boto.s3.connection   import S3Connection
import pyart
from   pyart.config         import get_metadata
from   pyart.graph          import radarmapdisplay
from   datetime             import datetime, timedelta, timezone
import pytz
import tempfile
from dask.distributed import LocalCluster
import matplotlib as plt


#Helper function to get nearest date
def _nearestDate(dates, pivot):
    return min(dates, key=lambda x: abs(x - pivot))


def get_radarobj_from_aws(site, datetime_t):
    """
    Get the closest volume of NEXRAD data to a user-defined datetime
    Params
    ----------
    site       : string
                 - four letter radar designation, ex: KHNX
    datetime_t : datetime
                 - desired date time
    Returns
    -------
    radar      : Py-ART Radar Object
                 - Radar closest to the queried datetime    
    """
    # Create the query string for the bucket knowing how NOAA and AWS store the data
    my_pref     = datetime_t.strftime('%Y/%m/%d/') + site
    # set up connection
    conn        = S3Connection(anon = True)
    # connect to bucket
    bucket      = conn.get_bucket('noaa-nexrad-level2')
    dir(bucket)      
    # Get a list of files
    # print('............ Retrieving all available volume keys from '+my_pref+'...')
    bucket_list = list(bucket.list(prefix = my_pref))   
    # create empty lists of keys and datetimes to allow easy searching
    keys        = []
    datetimes   = []
    # populate the lists
    for i in range(len(bucket_list)):
        this_str = str(bucket_list[i].key)
        # handle L2 binary gzipped volume format
        if 'gz' in this_str:
            endme = this_str[-22: -4]
            fmt   = '%Y%m%d_%H%M%S_V0'
            dt    = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])
        # handle L2 binary V06 volume format
        if this_str[-3::] == 'V06':
            endme = this_str[-19: : ]
            fmt   = '%Y%m%d_%H%M%S_V06'
            dt    = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])   
    # find the closest available radar volume to user datetime
    closest_datetime = _nearestDate(datetimes, datetime_t)
    closest_index    = datetimes.index(closest_datetime)
    # define localfile from closest_index
    localfile        = tempfile.NamedTemporaryFile()
    keys[closest_index].get_contents_to_filename(localfile.name)
    print('............ Closest volume to user-defined = '+str(keys[closest_index]))
    # read L2 localfile into radar volume object
    radar            = pyart.io.read(localfile.name)
    return radar

# print('... LOAD INPUT DATASETS...')
# print('..... Radar L2 NEXRAD volume data (in native polar coords) ...')
# if config['user']['NEXRAD_file_src']  == 'AWS':
#     #print('........ Get current local and UTC datetimes...')
#     now_local_dt    = datetime.now()
#     now_local_dt    = pytz.timezone(config['user']['tz']).localize(now_local_dt)
#     now_utc_dt      = datetime.now(timezone.utc)      
#     if config['user']['NEXRAD_retrieve_mode'] == 'RADAR-REALTIME':
#         #print('....... User requesting REALTIME volume from AWS for rrrr = '+case['case_rrrr']+'...')
#         #print('....... NOTE: AWS NEXRAD volume latency from realtime is 15 to 17m')
#         #print('.............. Current YYYY/MM/DD date = '+now_utc_dt.strftime("%Y/%m/%d"))  
#         #print('.............. Current HH:MM:SS time   = '+now_utc_dt.strftime("%H:%M:%S")) 
#         #print('.......... Define base_date from now_utc_dt ...')
#         base_date_UTC   = now_utc_dt.strftime('%Y')+now_utc_dt.strftime('%m')+now_utc_dt.strftime('%d')+'_'+now_utc_dt.strftime("%H")+now_utc_dt.strftime("%M")+now_utc_dt.strftime("%S")
#         my_pref         = now_utc_dt.strftime("%Y/%m/%d")+'/'+case['case_rrrr']+'/'
#     elif config['user']['NEXRAD_retrieve_mode'] == 'RADAR-ARCHIVE':
#         #print('........ User requesting ARCHIVE mode volume from AWS from '+str(config['case']['case_yyyymmddhhmmss'])+' for rrrr = '+config['case']['case_rrrr']+'...')     
#         #print('......... Get user-defined YYYY/MM/DD/<radar>...')
#         base_date_UTC   = config['case']['case_yyyymmddhhmmss']
#         my_pref         = base_date_UTC[0:4]+'/'+base_date_UTC[5:6]+'/'+base_date_UTC[7:8]+'/'+config['case']['case_rrrr']+'/' # pull these constants out to user config file
#     #print('.......... base_date = '+base_date_UTC)
#     #print('.......... Define custom formatted b_d for load function (from base_date)...')
#     fmt         = '%Y%m%d_%H%M%S'
#     b_d         = datetime.strptime(base_date_UTC, fmt)
#     #print('.......... Attempt NEXRAD L2 volume load to object=radar, from AWS source in raw binary format...')
#     try:
#         radar = get_radarobj_from_aws(config['case']['case_rrrr'], b_d )
#     except:
#         print('............ ERROR: an exception occurred')

if __name__ == '__main__':
    #Enable parallel processing using dask
    client = LocalCluster().get_client()

    station = 'KATX'
    my_datetime = datetime.now()
    radar = get_radarobj_from_aws(station, my_datetime)
    print(radar.fields['reflectivity'])

    
    
