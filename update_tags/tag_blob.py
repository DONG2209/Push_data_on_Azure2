from azure.storage.blob import BlobServiceClient
import requests
import os
import json

# URL để tải lên file
upload_url_template = "http://42.112.213.93:8000/api/v1/{type}/applications"

# Mẫu URL để lấy dữ liệu phân tích
analysis_url_template = "http://42.112.213.93:8000/api/v1/{type}/applications/{analysis_id}"

# Đường dẫn đến file cần tải lên
# file_path = r"C:\Users\nguye\Downloads\matmahoc.apk"
file_path=r"C:\Users\nguye\Downloads\truykich\truykich\TruyKich.exe"

def get_file_type(file_path):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.exe':
        return 'windows'
    elif file_extension == '.apk':
        return 'android'
    else:
        raise ValueError("Chỉ hỗ trợ file '.exe' và '.apk'")

def upload_file_and_get_analysis(file_path):
    # Xác định loại file
    file_type = get_file_type(file_path)
    
    # Tạo URL để tải lên file
    upload_url = upload_url_template.format(type=file_type)
    
    # Tải lên file
    with open(file_path, 'rb') as file:
        files = {'file': (os.path.basename(file_path), file, 'application/octet-stream')}
        response = requests.post(upload_url, files=files)

    if response.status_code == 201:
        data = response.json()
        analysis_id = data['data']['analysis_id']
        print(f"Tải lên thành công. Analysis ID: {analysis_id}")
        
        # Tạo URL để lấy dữ liệu phân tích
        analysis_url = analysis_url_template.format(type=file_type, analysis_id=analysis_id)
        analysis_response = requests.get(analysis_url)
        
        if analysis_response.status_code == 200:
            analysis_data = analysis_response.json()
            
            analysis_file_name = f"description_{os.path.splitext(os.path.basename(file_path))[0]}.json"
            with open(analysis_file_name, 'w') as f:
                json.dump(analysis_data, f, indent=4)
            
            print(f"Dữ liệu phân tích đã được lưu vào '{analysis_file_name}'")
            
            return analysis_file_name
            
        else:
            print(f"Không thể lấy dữ liệu phân tích. Mã lỗi: {analysis_response.status_code}")
            return None
    else:
        print(f"Tải lên không thành công. Mã lỗi: {response.status_code}")
        return None

def get_malware_type(analysis_result):
    if 'data' in analysis_result and 'malware_type' in analysis_result['data']:
        return analysis_result['data']['malware_type']
    else:
        return "Unknown"

def update_blob_tags(blob_client, malware_type):
    tags = {"malware_type": malware_type}
    blob_client.set_blob_tags(tags)

def upload_json_to_blob_storage(json_file_path, connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(json_file_path))
    with open(json_file_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"Uploaded '{os.path.basename(json_file_path)}' to Azure Blob Storage in container '{container_name}'")

def main():
    connection_string = "connection string"
    container_name = os.path.splitext(file_path)[1].lstrip('.')
    blob_name = os.path.basename(file_path)

    analysis_json_path = upload_file_and_get_analysis(file_path)
    if not analysis_json_path:
        return
    
    upload_json_to_blob_storage(analysis_json_path, connection_string, container_name)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(analysis_json_path,'r') as file:
        analysis_result = json.load(file)
   
    malware_type = get_malware_type(analysis_result)

    update_blob_tags(blob_client, malware_type)
    print(f"Updated blob tags for {blob_client.blob_name} with malware_type: {malware_type}")

if __name__ == "__main__":
    main()
