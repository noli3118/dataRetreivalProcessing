from   satpy.scene           import Scene
from   satpy                 import find_files_and_readers
from   glob                  import glob
from   datetime              import datetime # Basic dates and time types
import numpy                 as     np
import matplotlib.pyplot     as     plt      # Plotting library
import cartopy, cartopy.crs  as     ccrs     # Plot maps
import warnings

# import metsat.retreival.data_retreival as data_retreival

warnings.filterwarnings('ignore')
#-------------------------------------------
print('DEFINE CONSTANTS ...')
#-------------------------------------------

#-----------------------------------------
print('CREATE/OUTPUT LOG FILE ...')
#-----------------------------------------

#-----------------------------------------
print('FUNCTION DEFINITIONS ...')
#-----------------------------------------

#-------------------------------------------
print('DEFINE PATHS ...')
#-------------------------------------------
# define case
case_yyyymmdd = '20240609'

# define path to MSG-SEVIRI test data folder on local machine
base_path     = '/home/noli3118/git_repos/weatherML/'
data_path     = 'data/'
sat_path      = 'MSG/'
sat_file_path = 'MSG2-SEVI-MSG15-0100-NA-20240609025740.532000000Z-NA/'
filenames     = glob(base_path+data_path+sat_path+sat_file_path+'*'+case_yyyymmdd+'*')

print('-----------------------------------------------------------------------------------------------')
print('LOAD DATA ...')
print('----------------------------------------------------------------------------------------------')
global_scene = Scene(filenames=filenames, reader='seviri_l1b_native')
global_scene.load(global_scene.available_dataset_names(), upper_right_corner='NE')

print('-----------------------------------------------------------------------------------------------')
print('DISPLAY DATA ...')
print('----------------------------------------------------------------------------------------------')
print('... Channels in file = '+str(len(global_scene.available_dataset_names())))
global_scene.available_dataset_names()
print(global_scene)
print(global_scene['IR_108'])

print('------------------------------------------------------------------------------------------------')
print('MANIPULATE INPUT DATASETS...')
print('... Prep:MSG SEVIRI module ...')
vis006                 = global_scene['VIS006']
vis006_meas            = vis006.values
vis006_lon, vis006_lat = vis006.attrs['area'].get_lonlats()

print('------------------------------------------------------------------------------------------------')
print('VISUALIZATION OF DATA...')
print('... MSG-SEVIRI channels, mapview plot...')
for i, val in enumerate(global_scene.available_dataset_names()[1:-1]):
    crs = global_scene[val].attrs['area'].to_cartopy_crs()
    ax  = plt.axes(projection=crs)
    ax.coastlines()
    ax.gridlines()
    ax.set_global()
    plt.imshow(global_scene[val], transform=crs, extent=crs.bounds, origin='upper')
    plt.title('MSG-SEVIRI channel '+val+'; '+case_yyyymmdd+' @ XX:XX UTC')
    cbar = plt.colorbar()
    cbar.set_label('Temp [deg K]')
    plt.show()

print('------------------------------------------------------------------------------------------------')
print('SAVE DATA...')