import os
import pandas as pd
import asyncio
import subprocess
import nest_asyncio

nest_asyncio.apply()

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

# read the CSV file into df
grib_commands_df = pd.read_csv('grib_commands_list.csv')

# define asynch function to run commands which download grib and delete idx
async def download_forecast_async(grib_command, idx_file_path):
    try:
        # Execute the GRIB command using subprocess
        process = await asyncio.create_subprocess_shell(grib_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
        #    print(f"Downloaded: {grib_command}")
            # Remove the idx file
            os.remove(idx_file_path)
         #   print(f"Removed: {idx_file_path}")
            
        else:
            print(f"Failed to download: {grib_command}\n{stderr.decode()}")

    except Exception as e:
        print(f"Error processing {idx_file_path}: {e}")

async def main():
    # Number of concurrent downloads
    concurrent_downloads = 8  
    
    # Split the commands into chunks for parallel processing
    aws_commands_chunks = [grib_commands_df[i:i + concurrent_downloads] for i in range(0, len(grib_commands_df), concurrent_downloads)]
    
    for chunk in aws_commands_chunks:
        tasks = [download_forecast_async(row['command'], row['idx_file']) for index, row in chunk.iterrows()]
        await asyncio.gather(*tasks)

# Run the main coroutine
if __name__ == "__main__":
    asyncio.run(main())