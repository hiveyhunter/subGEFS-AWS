import os
import datetime
import pandas as pd
import re
import gc
import aiohttp
import asyncio
import nest_asyncio
from concurrent.futures import ProcessPoolExecutor
nest_asyncio.apply()

# these functions may be needed for jupyter
async def first_request():
    await asyncio.sleep(2) 
    return "First request response"


async def second_request():
    await asyncio.sleep(2)
    return "Second request response"


async def make_requests_in_parallel():
    """Make requests in parallel and return the responses."""
    return await asyncio.gather(first_request(), second_request())

def main():
    """Make results available to async-naive users""" 
    return asyncio.run(make_requests_in_parallel())

results = main()

dir_GEFS = os.getcwd()

# define a start and end time for forecast file download
download_stime = datetime.datetime(2019, 8, 1, 0, 0)
download_etime = datetime.datetime(2019, 8, 1, 18, 0)

# specify S3 bucket, GEFS intervals, and number of ensemble members
aws_s3_bucket = 'https://noaa-gefs-pds.s3.amazonaws.com/'
download_interval = 6
forecast_interval = 6
forecast_period = 240
n_ensemble = 21

# download idx file asynchronously
async def download_idx_file(session, url, filename):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                with open(filename, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
             #   print(f"Downloaded: {filename}")
            else:
                print(f"Failed to download: {filename}, Status code: {response.status}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")

# Function to process idx file to find byte ranges
def process_idx_file(file_path):
    grib_commands = []
    if os.path.exists(file_path):
        try:
            f = pd.read_fwf(file_path, header=None)
            sst_row = f[f.apply(lambda row: row.astype(str).str.contains('ICEC:surface').any(), axis=1)]
            if not sst_row.empty:
                sst_str = sst_row.to_string(index=False, header=False)
                sst_byte = re.search(r':(\d+):', sst_str).group(1)
                sst_index = sst_row.index[0]
                end_index = sst_index + 1
                end_row = f.iloc[end_index]
                end_str = end_row.to_string(index=False, header=False)
                end_byte = int(re.search(r':(\d+):', end_str).group(1)) - 1
                end_byte = str(end_byte)

                # Get file names from idx file path
                parts = file_path.split('/')
                YYYYMMDD = parts[-3]
                HH = parts[-2]
                file_name = parts[-1]
                file_id = file_name.split('.')
                file_type = file_id[0][:3]
                idn = file_id[0][3:5]
                forecast_ntime_str = file_id[2].split('bf')[1]

                # Define the command to download grib file
                aws_directory = f'gefs.{YYYYMMDD}/{HH}/pgrb2b/'
                download_grib_name = os.path.join(dir_GEFS, YYYYMMDD, HH, f'{file_type}{idn}.t{HH}z.pgrb2bf{forecast_ntime_str}')
                grib_command = f'aws s3api get-object --no-sign-request --bucket noaa-gefs-pds --key {aws_directory}{file_type}{idn}.t{HH}z.pgrb2bf{forecast_ntime_str} --range bytes={sst_byte}-{end_byte} {download_grib_name}'
                grib_commands.append((grib_command, file_path))
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    return grib_commands

# store AWS subset command list
aws_commands_list = []

# the script loops through specified forecast file times
download_ntime = download_stime
while download_ntime <= download_etime:
    YYYYMMDD = download_ntime.strftime('%Y%m%d')
    HH = download_ntime.strftime('%H')
    
    # create folders for each date and subdirectories for each initialization hour
    dir_download = os.path.join(dir_GEFS, YYYYMMDD)
    os.makedirs(dir_download, exist_ok=True)
    dir_download = os.path.join(dir_download, HH)
    os.makedirs(dir_download, exist_ok=True)

    # loop through each ensemble member and forecast time for each initialization datetime
    for idn in range(0, n_ensemble):
        forecast_ntime = 0
        while forecast_ntime <= forecast_period:
            file_type = 'gep'
            if idn == 0:
                file_type = 'gec'

            # format forecast time string
            forecast_ntime_str = str(forecast_ntime).zfill(2) if forecast_ntime < 10 else str(forecast_ntime)

            # file names from idx file path
            file_name = f'{file_type}{str(idn).zfill(2)}.t{HH}z.pgrb2bf{forecast_ntime_str}'
            file_idx_name = f'{file_name}.idx'

            # Define local download path for index file
            download_idx_name = os.path.join(dir_download, file_idx_name)
            # S3 directory path
            aws_directory = f'gefs.{YYYYMMDD}/{HH}/pgrb2b/'
            # Create full command for downloading index file
            idx_url = f'{aws_s3_bucket}{aws_directory}{file_idx_name}'
            aws_commands_list.append((idx_url, download_idx_name))

            # Identify next forecast interval
            forecast_ntime += forecast_interval
            
        gc.collect()

    # Identify next initialization datetime
    download_ntime += datetime.timedelta(hours=download_interval)

# Asynchronous downloading of idx files
async def download_idx_files(aws_commands):
    async with aiohttp.ClientSession() as session:
        tasks = [download_idx_file(session, url, filename) for url, filename in aws_commands]
        await asyncio.gather(*tasks)

# Number of concurrent downloads
concurrent_downloads = 8

# Split commands list into chunks for parallel processing
aws_commands_chunks = [aws_commands_list[i:i+concurrent_downloads] for i in range(0, len(aws_commands_list), concurrent_downloads)]

# Run the asyncio event loop manually
async def run_main():
    for chunk in aws_commands_chunks:
        await download_idx_files(chunk)

# Run the main coroutine using asyncio.run()
asyncio.run(run_main())

# Asynchronous processing of idx files
async def process_idx_files(idx_files):
    loop = asyncio.get_event_loop()
    with ProcessPoolExecutor() as executor:
        tasks = [loop.run_in_executor(executor, process_idx_file, file[1]) for file in idx_files]
        results = await asyncio.gather(*tasks)
    return results

# Run the asynchronous idx file processing
grib_commands = asyncio.run(process_idx_files(aws_commands_list))

# Flatten the list of lists
grib_commands_flat = [item for sublist in grib_commands for item in sublist]

# Save grib commands list to a file for the second script to use
pd.DataFrame(grib_commands_flat, columns=["command", "idx_file"]).to_csv('grib_commands_list.csv', index=False)