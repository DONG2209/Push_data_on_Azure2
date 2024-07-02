import requests
from azure.storage.blob import BlobServiceClient
import os

storage_account_name = 'name'
storage_account_key = 'key'

blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key,connection_timeout=40)

def upload_to_blob(file_name):
    file_extension = os.path.splitext(file_name)[1][1:]  

    if file_extension in ['png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff']:
        container_name = 'images'
    elif file_extension in ['doc', 'docx']:
        container_name = 'document'
    elif file_extension in ['xls', 'xlsx']:
        container_name = 'excel'
    elif file_extension in ['mp3', 'wav', 'aac']:
        container_name = 'audio'
    elif file_extension in ['mp4', 'avi', 'mkv', 'mov']:
        container_name = 'video'
    else:
        container_name = file_extension

    container_client = blob_service_client.get_container_client(container_name)

    if not container_client.exists():
        container_client.create_container()

    blob_name = os.path.basename(file_name)

    blob_client = container_client.get_blob_client(blob_name)

    counter = 1
    if blob_client.exists():
        base_name, extension = os.path.splitext(blob_name)
        while blob_client.exists():
            new_blob_name = f"{base_name}_{counter}{extension}"
            blob_client = container_client.get_blob_client(new_blob_name)
            counter += 1

    # chunk_size = 4 * 1024 * 1024  
    # offset = 0
    # index = 1

    # # Upload chunks of data
    # while offset < len(data):
    #     chunk_data = data[offset:offset + chunk_size]
    #     blob_client.upload_blob(chunk_data, length=len(chunk_data), overwrite=True if index == 1 else False, blob_type="BlockBlob")
    #     offset += chunk_size
    #     index += 1

    blob_client.upload_blob(data, overwrite=True)

    if counter > 1:
        print(f"Uploaded {file_name} to blob storage in container '{container_name}' as blob '{new_blob_name}'.")
        
    print(f"Uploaded {file_name} to blob storage in container '{container_name}'.")

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

    upload_to_blob(file_name)
