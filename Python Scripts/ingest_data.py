"""
@author: User
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

#create time formatter
def time_format(seconds):
    """Convert elapsed seconds to a human-readable string (seconds, minutes, hours)."""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"

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

def download_parquet(urls):
    for url,filename,trip in urls:
        folder = trip
        file_path = os.path.join(folder, filename)
        # Skip if file already exists
        if os.path.exists(os.path.join(folder,filename)):
            print(f"⏩ {filename} already downloaded.")
            continue
        
        print(f"Downloading {filename}...")
        start_time = time.time()     
        
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(os.path.join(folder,filename), 'wb') as file:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                        
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"✅ Saved {filename}  in {time_format(elapsed_time)}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to download {filename}: {e}\n")

def convert_all_parquet_to_csv(*trip_type):
    for trip in trip_type:
        folder = trip
        csv_folder = os.path.join(folder, "csv_trip_data")
        create_directory(csv_folder)
        
        # Loop through all parquet files in the folder
        for file in os.listdir(folder):
            if file.endswith(".parquet"):
                parquet_file = os.path.join(folder, file)
                csv_file = os.path.join(csv_folder, file.replace(".parquet", ".csv"))
                
                if os.path.exists(csv_file):
                    print(f"⏩ {file} already converted to CSV.")
                    continue
                
                print(f"Converting {file} to csv...")
                start_time = time.time()
                
                # Read Parquet and write CSV
                table = pq.read_table(parquet_file)
                pc.write_csv(table, csv_file)
                
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"✅ Converted {parquet_file} to {csv_file} in {time_format(elapsed_time)} ")

# def parquet_to_csv(parquet_file, trip_folder):

    
# Define necessary variables for ingestion
base_url ='https://d37ci6vzurychx.cloudfront.net/trip-data'
year = 2024 
trip_type = ['yellow_tripdata','green_tripdata','fhv_tripdata','fhvhv_tripdata']
months = [f'{m:02d}' for m in range(1,2)]

# create folder per trip
create_directory(*trip_type)
urls =  generate_url(base_url,trip_type,year,months)


load_start_time = time.time()      # start timing
print(f"Data download process starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

download_parquet(urls)  

load_end_time = time.time()  # start timing
elapsed_time = (load_end_time - load_start_time)
print(f"All data download process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total data download took {time_format(elapsed_time)}")

    
load_start_time = time.time()      # start timing
print(f"Conversion to CSV starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

convert_all_parquet_to_csv(*trip_type)

load_end_time = time.time()  # start timing
elapsed_time = (load_end_time - load_start_time)
print(f"All Conversion to CSV completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total CSV conversion took {time_format(elapsed_time)}")

# parquet_to_csv(file_path, folder)
# #Convert parquet files to csv for easy loading to sql server
# csv_folder = os.path.join(folder, "csv_data")
# create_directory(csv_folder)

# filename = os.path.basename(parquet_file).replace(".parquet", ".csv")

# print(filename)



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
    
