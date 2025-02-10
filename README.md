# subGEFS-AWS
Scripts to download specified variables from archived Global Ensemble Forecast System (GEFS) forecasts stored in the AWS S3 bucket noaa-gefs-pds

## Contents
Options for parallel downloads are provided
(Asynchronous uploads to come)

Python script GEFSa_parallel.py creates and executes lists of commands to download specified ranges of full forecasts of GEFSa parameters

Python script GEFSb_parallel.py creates and executes lists of commands to download specified ranges of a single vertical level of a single GEFSb parameter

## Note
This is a personal and independent project. Scripts were written to meet the needs of my own work and thus are not fully generalizable in their current states.
