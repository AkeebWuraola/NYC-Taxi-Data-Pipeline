

from time import time 
import os
import requests
import pyarrow.parquet as pq
import pyarrow.csv as pc

base_url ='https://d37ci6vzurychx.cloudfront.net/trip-data'
year = 2024 
trip_type = ['yellow_tripdata','green_tripdata','fhv_tripdata','fhvhv_tripdata']
months = [f'{m:02d}' for m in range(1,2)]


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

# create folder per trip
create_directory(*trip_type)

urls =  generate_url(base_url,trip_type,year,months)
for url,filename,trip in urls:
    folder = trip
    print(f"Downloading {filename}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(os.path.join(folder,filename),mode = 'wb') as file:
            file.write(response.content)
            print(f"✅ Saved {filename}")
    else:
        print(f"❌ Failed to download {filename} (status: {response.status_code})")
        
