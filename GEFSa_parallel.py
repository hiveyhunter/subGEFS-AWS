import os
import datetime
import numpy as np
from joblib import Parallel, delayed

# THIS SHOULD BE ADJUSTED TO THE DIRECTORY WHERE THE FILE SHOULD TO BE SAVED
os.chdir('/Volumes/Archive/GEFSa')
dir_GEFS = '/Volumes/Archive/GEFSa'

# SET START AND END DATETIME FOR FILE DOWNLOADS
download_stime = datetime.datetime(2019, 4,  28, 12, 0, 0)
download_etime = datetime.datetime(2019, 4,  28, 18, 0, 0)

# IDENTIFIES AWS BUCKET FOR NOAA GEFS
aws_s3_cp = 'aws s3 cp --no-sign-request '
aws_s3_bucket = 's3://noaa-gefs-pds/'

# IDENTIFIES TIME STEP (HOURS) DESIRED TO DOWNLOAD AND IN FORECAST MODEL, FINAL FORECAST TIME (HOURS), NUMBER OF ENSEMBLE MEMBERS TO DOWNLOAD
download_interval = 6
forecast_interval = 6
forecast_period = 384
n_ensemble = 21

def download_forecast(aws_command):
    os.system(aws_command)

# CHANGE ACCORDING TO CORES AVAILABLE
num_jobs = 3

# THIS SCRIPT CREATES A LIST OF COMMANDS WHICH WILL DOWNLOAD ALL FORECASTS SPECIFIED
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
                file_name = file_type + str(idn).zfill(2) + '.t' + HH + 'z.pgrb2af' + forecast_ntime_str
                download_file_name = '/'.join([dir_download, file_name])

                aws_directory = '/'.join(['gefs.' + YYYYMMDD, HH, 'pgrb2a/'])

                aws_command = aws_s3_cp + aws_s3_bucket + aws_directory + file_name + ' ' + download_file_name
            else:
                forecast_ntime_str = str(forecast_ntime).zfill(2) if forecast_ntime < 10 else str(forecast_ntime)
                file_name = file_type + str(idn).zfill(2) + '.t' + HH + 'z.pgrb2af' + forecast_ntime_str
                download_file_name = '/'.join([dir_download, file_name])

                aws_directory = '/'.join(['gefs.' + YYYYMMDD, HH, 'pgrb2a/'])

                aws_command = aws_s3_cp + aws_s3_bucket + aws_directory + file_name + ' ' + download_file_name

            aws_commands_list.append(aws_command)

            forecast_ntime += forecast_interval

    download_ntime += datetime.timedelta(hours=download_interval)

# EXECUTE ALL COMMANDS TO DOWNLOAD FILES IN PARALLEL
Parallel(n_jobs=num_jobs)(delayed(download_forecast)(aws_command) for aws_command in aws_commands_list)