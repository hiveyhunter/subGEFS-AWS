# subGEFS-AWS
Scripts to download specified variables from archived Global Ensemble Forecast System (GEFS) forecasts stored in the AWS S3 bucket [noaa-gefs-pds](https://registry.opendata.aws/noaa-gefs/)

--------------------------------------------------------
## Note
##### This is a personal and independent project. Scripts were written in JupyterLab version 4.0.11 to meet the needs of my own work and are not fully generalizable in their current states.
##### Future updates may include cleanup of .idx files in parallel GEFSb. Additionally, I may adapt scripts to more easily alternate between a and b parameters as well as switch between downloads of full forecasts and single parameters via function input.
--------------------------------------------------------

## Parallel

##### Python script `GEFSa_parallel.py` creates and executes lists of commands to download specified ranges of full forecasts of GEFSa parameters.
```
GEFSa_parallel.py
```

- create directories of each date of initialized forecast and subdirectories within each directory with the initialization time
- create and store commands to download grib files from aws for all specified ens. members and forecast times within initialization time subdirectory
- execute grib downloads


##### Python script `GEFSb_parallel.py` creates and executes lists of commands to download specified ranges of a single vertical level of a single GEFSb parameter.
```
GEFSb_parallel.py
```

- create directories of each date of initialized forecast and subdirectories within each directory with the initialization time
- download each idx file for all specified ens. members and forecast times within initialization time subdirectory
- read idx file and locate the first and last byte of the target variable specified by string
- create and store commands to download the subset grib files from aws
- execute grib downloads

##### These scripts were written specifically for parallel execution in JupyterLab. This may not be the most efficient way to download these files in parallel outside of Jupyter.

##### Before using, edit dates, initialization times, variables to be downloaded. Adjust the string used in byte subsetting to match the variable name to download.


#### All libraries and modules used:
```
os
gc
datetime
numpy ==1.24.4
pandas == 2.2.2
joblib == 1.3.2
re ==  2.2.1
```

## Asynchronous
##### Python scripts `GEFS_idx_commands.py` and `GEFS_grib_async.py` should be used together

#### To run, first execute 
`GEFS_idx_commands.py`

#### followed by
`GEFS_grib_async.py`

#### The two .py files executed in order will do the following:

```
GEFS_idx_commands.py
```

- create directories of each date of initialized forecast and subdirectories within each directory with the initialization time
- download each idx file for all specified ens. members and forecast times within initialization time subdirectory
- read idx file and locate the first and last byte of the target variable specified by string
- create a command to download the subset grib file from aws using awscli
- create and download a csv which contains one column with each aws command and a paired column with the associated idx file name
  
```
GEFS_grib_async.py
```

- read commands from csv
- execute grib downloads
- delete associated idx files

##### These scripts were written specifically for asynchronous execution in jupyter lab. The nest_asyncio module and some functions are not necessary outside of jupyter or on other versions of jupyter. There are also other ways to handle the issue of jupyter's event loop without the use of these functions.

##### Before using, edit parameters in idx_commands to determine dates, initialization times, variables to be downloaded. Adjust the string used in byte subsetting to match the variable name to download. The current uploaded version will download ice fraction from GEFSb parameters.

#### All libraries and modules used:
```
os
gc
datetime
nest_asyncio
concurrent.futures
subprocess
re == 2.2.1
pandas == 2.2.2
aiohttp == 3.9.5
asyncio == 3.4.3
awscli == 1.33.22
```
--------------------------------------------------------
## Output:
- ```YYYYMMDD/HH/{filetype}{ens_member}.t{HH}z.pgrb2{a|b}f{fc_hour}``` subset grib files within subdirectories of forecast initialization times
- (For asynchronous only) ```grib_commands_list.csv``` csv file with the aws grib download commands and associated idx file names


