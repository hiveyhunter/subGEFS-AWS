import os
import datetime
import numpy as np
import pandas as pd
import re
import gc
from joblib import Parallel, delayed

# directory where files should be saved
os.chdir('/Volumes/Archive/GEFSb')
dir_GEFS = '/Volumes/Archive/GEFSb'

# set start and end datetime for downloads
download_stime = datetime.datetime(2019, 4,  28, 18, 0, 0)
download_etime = datetime.datetime(2019, 4,  28, 18, 0, 0)

# specify command
aws_s3_cp = 'aws s3 cp --no-sign-request '

# specify bucket
aws_s3_bucket = 's3://noaa-gefs-pds/'

# identifies time step (hours) for downloads and in forecast, final forecast time (hours), number of ensemble members to download
download_interval = 6
forecast_interval = 6
forecast_period = 384
n_ensemble = 21


def download_forecast(grib_command):
    os.system(grib_command)

# change depending on cores available
num_jobs = 3

# the script creates a list of commands which will download all forecasts specified
aws_commands_list = []

download_ntime = download_stime
while download_ntime <= download_etime:
    YYYYMMDD = download_ntime.strftime('%Y%m%d')
    HH = download_ntime.strftime('%H')

    dir_download = '/'.join([dir_GEFS, YYYYMMDD])
    os.system('mkdir -p ' + dir_download)

    dir_download = '/'.join([dir_download, HH])
    os.system('mkdir -p ' + dir_download)

    for idn in range(0, n_ensemble):
        forecast_ntime = 0
        while forecast_ntime <= forecast_period:
            file_type = 'gep'
            if idn == 0:
                file_type = 'gec'
                forecast_ntime_str = str(forecast_ntime).zfill(2) if forecast_ntime < 10 else str(forecast_ntime)
                file_idx_name = file_type + str(idn).zfill(2) + '.t' + HH + 'z.pgrb2bf' + forecast_ntime_str + '.idx'
                file_grib_name = file_type + str(idn).zfill(2) + '.t' + HH + 'z.pgrb2bf' + forecast_ntime_str
                download_idx_name = '/'.join([dir_download, file_idx_name])
                download_grib_name = '/'.join([dir_download, file_grib_name])

                aws_directory = '/'.join(['gefs.' + YYYYMMDD, HH, 'pgrb2b/'])

                idx_command = aws_s3_cp + aws_s3_bucket + aws_directory + file_idx_name + ' ' + download_idx_name
                
                os.system(idx_command)
                f = pd.read_fwf(download_idx_name, header=None)
                
# this code constrains the byte range to download
# identify the a paramer to download by name if only interested in specific variable
# for full forecast, code can be commented out and grib command adjusted to remove byte range
# note this needs to be edited for else statement as well

                sst_row = f[f.apply(lambda row: row.astype(str).str.contains('TMP:surface').any(), axis=1)]
                sst_str = sst_row.to_string(index=False, header=False)
                sst_byte = re.search(r':(\d+):', sst_str)
                sst_byte = sst_byte.group(1)
                sst_index = f[f.apply(lambda row: row.astype(str).str.contains('TMP:surface').any(), axis=1)].index[0]
                end_index = sst_index + 1
                end_row = f.iloc[end_index]
                end_str = end_row.to_string(index=False, header=False)
                end_byte = re.search(r':(\d+):', end_str)
                end_byte = end_byte.group(1)
                end_byte = int(end_byte) - 1
                end_byte = str(end_byte)

                grib_command = 'aws s3api get-object --no-sign-request --bucket noaa-gefs-pds --key '+ aws_directory + file_grib_name +' --range bytes=' +sst_byte+'-'+end_byte+ ' '+download_grib_name
                
            else:
                forecast_ntime_str = str(forecast_ntime).zfill(2) if forecast_ntime < 10 else str(forecast_ntime)
                file_idx_name = file_type + str(idn).zfill(2) + '.t' + HH + 'z.pgrb2bf' + forecast_ntime_str + '.idx'
                file_grib_name = file_type + str(idn).zfill(2) + '.t' + HH + 'z.pgrb2bf' + forecast_ntime_str
                download_idx_name = '/'.join([dir_download, file_idx_name])
                download_grib_name = '/'.join([dir_download, file_grib_name])

                aws_directory = '/'.join(['gefs.' + YYYYMMDD, HH, 'pgrb2b/'])

                idx_command = aws_s3_cp + aws_s3_bucket + aws_directory + file_idx_name + ' ' + download_idx_name
                
                os.system(idx_command)

                f = pd.read_fwf(download_idx_name, header=None)
                sst_row = f[f.apply(lambda row: row.astype(str).str.contains('TMP:surface').any(), axis=1)]
                sst_str = sst_row.to_string(index=False, header=False)
                sst_byte = re.search(r':(\d+):', sst_str)
                sst_byte = sst_byte.group(1)
                sst_index = f[f.apply(lambda row: row.astype(str).str.contains('TMP:surface').any(), axis=1)].index[0]
                end_index = sst_index + 1
                end_row = f.iloc[end_index]
                end_str = end_row.to_string(index=False, header=False)
                end_byte = re.search(r':(\d+):', end_str)
                end_byte = end_byte.group(1)
                end_byte = int(end_byte) - 1
                end_byte = str(end_byte)

                grib_command = 'aws s3api get-object --no-sign-request --bucket noaa-gefs-pds --key '+ aws_directory + file_grib_name +' --range bytes=' +sst_byte+'-'+end_byte+ ' '+download_grib_name

            aws_commands_list.append(grib_command)
            gc.collect()

            forecast_ntime += forecast_interval

    download_ntime += datetime.timedelta(hours=download_interval)

# execute all commands to download files in parallel
Parallel(n_jobs=num_jobs)(delayed(download_forecast)(grib_command) for grib_command in aws_commands_list)