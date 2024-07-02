import requests
from azure.storage.filedatalake import DataLakeServiceClient
import os

storage_account_name = 'name'
storage_account_key = 'key'

service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=storage_account_key)

def upload_to_data_lake(file_name, data):
    file_extension = os.path.splitext(file_name)[1][1:]  

    if file_extension in ['png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff']:
        file_system_name = 'images'
    elif file_extension in ['doc', 'docx']:
        file_system_name = 'document'
    elif file_extension in ['xls', 'xlsx']:
        file_system_name = 'excel'       
    elif file_extension in ['mp3', 'wav', 'aac']:
        file_system_name = 'audio'
    elif file_extension in ['mp4', 'avi', 'mkv', 'mov']:
        file_system_name = 'video'
    else:
        file_system_name = file_extension

    file_system_client = service_client.get_file_system_client(file_system_name)
    
    if not file_system_client.exists():
        file_system_client.create_file_system()

    file_client = file_system_client.get_file_client(file_name)

    counter = 1
    if file_client.exists():
        base_name, extension = os.path.splitext(file_name)
        while file_client.exists():
            new_file_name = f"{base_name}_{counter}{extension}"
            file_client = file_system_client.get_file_client(new_file_name)
            counter += 1

    file_client.create_file()

    chunk_size = 4 * 1024 * 1024  # Kích thước mỗi phần: 4MB
    data_size = len(data)
    num_chunks = data_size // chunk_size + (1 if data_size % chunk_size != 0 else 0)

    # chunk_size = file_size // 10
    # num_chunks = 10

    for i in range(num_chunks):
        chunk_data = data[i * chunk_size:(i + 1) * chunk_size]
        file_client.append_data(data=chunk_data, offset=i * chunk_size, length=len(chunk_data))

    file_client.flush_data(data_size)
   
    if counter > 1 :
        print(f"Uploaded {file_name} to data lake storage in file system '{file_system_name}' as file name '{new_file_name}'.")

    print(f"Uploaded {file_name} to data lake storage in file system '{file_system_name}'.")

def crawl_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to retrieve data from {url}")
        return None

url = "https://example.com/data"
data = crawl_data(url)

if data:
    file_name = 'crawled_data.txt'

    upload_to_data_lake(file_name, data.encode('utf-8'))
