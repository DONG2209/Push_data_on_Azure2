from azure.storage.blob import BlobServiceClient
import os

storage_account_name = 'name'
storage_account_key = 'key'

blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key,connection_timeout=40)

def upload_to_blob(local_file_path):
    file_extension = os.path.splitext(local_file_path)[1][1:]  

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

    blob_name = os.path.basename(local_file_path)
    
    blob_client = container_client.get_blob_client(blob_name)

    counter = 1
    if blob_client.exists():
        base_name, extension = os.path.splitext(blob_name)
        while blob_client.exists():
            new_blob_name = f"{base_name}_{counter}{extension}"
            blob_client = container_client.get_blob_client(new_blob_name)
            counter += 1

    # chunk_size = 4 * 1024 * 1024  
    # index = 1

    # with open(local_file_path, "rb") as data:
    #     offset = 0
    #     while offset < os.path.getsize(local_file_path):
    #         chunk_data = data.read(chunk_size)
    #         blob_client.upload_blob(chunk_data, length=len(chunk_data), overwrite=True if index == 1 else False, blob_type="BlockBlob")
    #         offset += chunk_size
    #         index += 1

    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    if counter > 1:
        print(f"Uploaded {local_file_path} to blob storage in container '{container_name}' as blob '{new_blob_name}'.")

    print(f"Uploaded {local_file_path} to blob storage in container '{container_name}'.")

local_file_path = r"D:\DA\data_sample_test\user_info.txt"

upload_to_blob(local_file_path)
