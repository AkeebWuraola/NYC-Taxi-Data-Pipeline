
"""
@author: Akeeb Wuraola
"""

import time 
import os
import requests
import pyarrow.parquet as pq
import pyarrow.csv as pc
from datetime import datetime

# load_start_time = time.time()      # start timing
# print(load_start_time)
# print(f"Data download process starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# Create destination folder
def create_directory(*folders): # * is to unpack a list
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Created '{folder}' folder")
        else:
            print(f"Folder '{folder}' already exists")
# create_directory('newyork','cali')
# create_directory(*trip_type)


def generate_url(base_url,trip_type,year,months):
    urls = []
    for trip in trip_type:
        for month in months:
            filename = f'{trip}_{year}-{month}.parquet'
            # print(filename)
            urls.append((f'{base_url}/{filename}',filename,trip))
            # print(urls)
    return urls
# print(generate_url(base_url,trip_type,year,months)) #Test the output of the function
# To cleanly print each url and filename for readability
# urls = generate_url(base_url,trip_type,year,months)
# for url, filename,trip in urls:
#     print(url, filename,trip)

base_url ='https://d37ci6vzurychx.cloudfront.net/trip-data'
year = 2024 
trip_type = ['yellow_tripdata','green_tripdata','fhv_tripdata','fhvhv_tripdata']
months = [f'{m:02d}' for m in range(1,3)]

# create folder per trip
create_directory(*trip_type)
urls =  generate_url(base_url,trip_type,year,months)


load_start_time = time.time()      # start timing
print(f"Data download process starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def download_parquet():
    for url,filename,trip in urls:
        folder = trip
        # Skip if file already exists
        if os.path.exists(os.path.join(folder,filename)):
            print(f"⏩ {filename} already downloaded.")
            continue
        
        print(f"Downloading {filename}...")
        start_time = time.time()     
        
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(os.path.join(folder,filename), 'wb') as file:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                    
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"✅ Saved {filename}  in {elapsed_time} seconds")
        
load_end_time = time.time()  # start timing
elapsed_time = (load_end_time - load_start_time)

print(f"All Data download process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total data download took {elapsed_time}seconds")

   



# #create a single folder
# # def create_folder(folder_name):
# #     if not os.path.exists(folder_name):
# #         os.makedirs(folder_name)
# create_folder('csv_folder')

# print(trip_type)
# print(months)

# urls = ['https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet',
#        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-02.parquet'
#       ]

# #filename = 'C:\Users\User\Downloads\TaxiTripData\yellow_tripdata_2024-01'
# folder_name = 'NEW_YORK_DATA'
# if not os.path.exists(folder_name):
#     os.makedirs(folder_name)
    
# for url in urls:
#     response = requests.get(url)
#     with open(os.path.join(folder_name,url.split('/')[-1]),mode = 'wb') as file:
#         file.write(response.content)
        
# print('File(s) downloaded successfully')

# This can download the files however not very optimal if dealing with large datasets

# for url,filename,trip in urls:
#     folder = trip
#     # Skip if file already exists
#     if os.path.exists(os.path.join(folder,filename)):
#         print(f"⏩ {filename} already downloaded.")
#         continue
    
#     print(f"Downloading {filename}...")
#     start_time = time.time() 
#     response = requests.get(url)
#     end_time = time.time()
#     elapsed_time = end_time - start_time
    
    # if response.status_code == 200:
    #     with open(os.path.join(folder,filename),mode = 'wb') as file:
    #         file.write(response.content)
    #         print(f"✅ Saved {filename}  in {elapsed_time} seconds")
    # else:
    #     print(f"❌ Failed to download {filename} (status: {response.status_code})")
    
