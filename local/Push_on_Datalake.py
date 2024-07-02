from azure.storage.filedatalake import DataLakeServiceClient
import os

storage_account_name = 'name'
storage_account_key = 'key'

service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=storage_account_key)

def upload_to_data_lake(local_file_path):
    file_extension = os.path.splitext(local_file_path)[1][1:]  

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

    remote_file_path = os.path.basename(local_file_path)
    
    file_client = file_system_client.get_file_client(remote_file_path)

    counter = 1
    if file_client.exists():
        base_name, extension = os.path.splitext(remote_file_path)
        while file_client.exists():
            new_file_name = f"{base_name}_{counter}{extension}"
            file_client = file_system_client.get_file_client(new_file_name)
            counter += 1

    file_client.create_file()

    file_size = os.path.getsize(local_file_path)
    chunk_size = 4 * 1024 * 1024     # Kích thước mỗi phần: 4MB
    num_chunks = file_size // chunk_size + (1 if file_size % chunk_size != 0 else 0)

    # chunk_size = file_size // 10
    # num_chunks = 10

    with open(local_file_path, "rb") as data:
        for i in range(num_chunks):
            chunk_data = data.read(chunk_size)
            file_client.append_data(data=chunk_data, offset=i * chunk_size, length=len(chunk_data))

    file_client.flush_data(file_size)
   
    if counter > 1 :
        print(f"Uploaded {local_file_path} to data lake storage in file system '{file_system_name}' as file name '{new_file_name}'.")

    print(f"Uploaded {local_file_path} to data lake storage in file system '{file_system_name}'.")


local_file_path = r"D:\DA\data_sample_test\user_info.txt"

upload_to_data_lake(local_file_path)
