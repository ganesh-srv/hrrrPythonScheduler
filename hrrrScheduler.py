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



sys.stdout.reconfigure(encoding='utf-8')
setproctitle.setproctitle("HRRRCronJob")

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
