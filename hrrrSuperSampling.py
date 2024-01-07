from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import time
from pprint import pprint
from herbie.archive import Herbie
import time
from pprint import pprint
import os
import numcodecs as ncd
from numcodecs import Blosc
import sys
import logging
import shutil
import setproctitle
import xarray as xr
# from skimage.transform import rescale
import numpy as np


sys.stdout.reconfigure(encoding='utf-8')
# setproctitle.setproctitle("HRRRCronJob")

model = 'hrrr'  
product = 'sfc'

logging.basicConfig(level=logging.INFO)  # Configure logging

scheduler = BackgroundScheduler()
# temp = 21
last_execution_time = None
data_folder = os.path.join(os.path.dirname(os.getcwd()), 'dataStore/now/')

def get_latest_hrrr_data():
   global temp
   global last_execution_time

   now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
   date = now.strftime('%Y%m%d')
   print('-----')
   print(now)
   print('-----')
   print(last_execution_time)
   print('-----')

   if last_execution_time is not None and now.strftime('%Y-%m-%d %H') == last_execution_time.strftime('%Y-%m-%d %H'):
        logging.info(f'Skipping execution for {now.strftime("%Y-%m-%d %H")} UTC')
        return

   max_subhourly_time = 49 if now.hour in [0, 6, 12, 18] else 19
  #  use max_subhourly_time for all the forecast hours
   for fhour in range(2):
    ds = None
    retrieved_successfully = False
    retries = 0
    while not retrieved_successfully and ds is None and retries < 10:
            try:
                hrrr = Herbie(now, model=model, product=product, fxx=fhour)
                ds_vis = hrrr.xarray('VIS')  # Retrieve surface visibility data
                ds_t2m = hrrr.xarray('TMP:2 m')  # Retrieve temperature data
                ds = xr.merge([ds_vis, ds_t2m])
                # ds_vis = ds['vis']   
                # ds_t2m = ds['t2m']
                #Version 1
                # # upscale_factor = 3
                # row_scale = 2  
                # col_scale = 3
                # # Rescale to numpy arrays
                # t2m_1km = rescale(ds_t2m.values, (row_scale, col_scale), order=1)  
                # vis_1km = rescale(ds_vis.values, (row_scale, col_scale), order=1)
                # # Convert back to DataArrays
                # t2m_1km = xr.DataArray(t2m_1km, dims=ds_t2m.dims, name='t2m')
                # vis_1km = xr.DataArray(vis_1km, dims=ds_vis.dims, name='vis')
                # new_x = np.linspace(ds_t2m.x[0], ds_t2m.x[-1], t2m_1km.shape[-1])
                # new_y = np.linspace(ds_t2m.y[0], ds_t2m.y[-1], t2m_1km.shape[-2])
                # new_coords = {'x': new_x, 'y': new_y}
                # t2m_1km.coords['latitude'] = ds_t2m['latitude'].interp(new_coords)
                # t2m_1km.coords['longitude'] = ds_t2m['longitude'].interp(new_coords)
                # vis_1km.coords['latitude'] = ds_vis['latitude'].interp(new_coords)
                # vis_1km.coords['longitude'] = ds_vis['longitude'].interp(new_coords)
                # ds_1km = xr.merge([t2m_1km, vis_1km])
                # ds = ds_1km

                # Version 2
                ds_alpha = ds
                # Create new 'y' and 'x' coordinates
                y_new = np.linspace(ds_alpha.y.min(), ds_alpha.y.max(), ds_alpha.dims['y'] * 3)  # 3 times the original resolution
                x_new = np.linspace(ds_alpha.x.min(), ds_alpha.x.max(), ds_alpha.dims['x'] * 3)  # 3 times the original resolution

                # Create a new grid
                y_grid, x_grid = np.meshgrid(y_new, x_new, indexing='ij')

                # Create new 'latitude' and 'longitude' coordinates
                lat_new = xr.DataArray(ds_alpha.latitude.interp(y=y_new, x=x_new), dims=['y', 'x'], coords={'y': y_grid[:, 0], 'x': x_grid[0, :]})
                lon_new = xr.DataArray(ds_alpha.longitude.interp(y=y_new, x=x_new), dims=['y', 'x'], coords={'y': y_grid[:, 0], 'x': x_grid[0, :]})

                # Interpolate the data variables to the new grid
                ds_new = ds_alpha.interp(y=y_new, x=x_new)

                # Assign the new 'latitude' and 'longitude' coordinates
                ds_new = ds_new.assign_coords(latitude=lat_new, longitude=lon_new)
                ds = ds_new
                retrieved_successfully = True
            except Exception as e:
                logging.error(f'Error retrieving latest subhourly data: {e}')
                logging.info('Retrying in 10 seconds...')
                retries += 1
                time.sleep(10)
    if retrieved_successfully:
        logging.info(f'Latest subhourly data for {now.strftime("%Y-%m-%d %H")} UTC')
        subhourly_dir = os.path.join(data_folder, now.strftime('%Y-%m-%d_%H'))
        os.makedirs(subhourly_dir, exist_ok=True)
        hour_dir = os.path.join(subhourly_dir, str(fhour))
        os.makedirs(hour_dir, exist_ok=True)
        ds_chunked = ds.chunk({"y": 150, "x": 150})
        ds_chunked.attrs = {}
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.SHUFFLE)
        encoding = {vname: {'compressor': compressor} for vname in ds_chunked.data_vars}
        ds_chunked.to_zarr(hour_dir, encoding=encoding, consolidated=True)
        logging.info(ds_chunked)
    
   last_execution_time = now
   delete_old_folders(data_folder, keep_hours=4)

def delete_old_folders(relative_path, keep_hours=4):
    logging.info(f"Entering deletion block")
    current_time = datetime.datetime.now()
    folders = [f for f in os.listdir(relative_path) if os.path.isdir(os.path.join(relative_path, f))]
    
    for folder in folders:
        folder_path = os.path.join(relative_path, folder)
        folder_time = datetime.datetime.strptime(folder, '%Y-%m-%d_%H')
        age = current_time - folder_time
        logging.info(f"Age of the deleting folder {age}")
        if age.total_seconds() >= keep_hours * 3600:
            shutil.rmtree(folder_path)
            logging.info(f"Deleted old folder: {folder_path}")


get_latest_hrrr_data()

scheduler.add_job(get_latest_hrrr_data, 'interval', minutes=1, id='vis', name='HRRR Visibility task')
scheduler.start()

# Keep the scheduler running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    scheduler.shutdown()
