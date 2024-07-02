import requests
import json
import os

upload_url_template = "http://42.112.213.93:8000/api/v1/{type}/applications"

analysis_url_template = "http://42.112.213.93:8000/api/v1/{type}/applications/{analysis_id}"

file_path = r"C:\Users\nguye\Downloads\matmahoc.apk"

def get_file_type(file_path):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.exe':
        return 'windows'
    elif file_extension == '.apk':
        return 'android'
    else:
        raise ValueError("Chỉ hỗ trợ file '.exe' và '.apk'")

def upload_file_and_get_analysis(file_path):
    file_type = get_file_type(file_path)
    
    upload_url = upload_url_template.format(type=file_type)
    
    with open(file_path, 'rb') as file:
        files = {'file': (os.path.basename(file_path), file, 'application/octet-stream')}
        response = requests.post(upload_url, files=files)
    
    if response.status_code == 201:
        data = response.json()
        analysis_id = data['data']['analysis_id']
        print(f"Tải lên thành công. Analysis ID: {analysis_id}")
        
        analysis_url = analysis_url_template.format(type=file_type, analysis_id=analysis_id)
        analysis_response = requests.get(analysis_url)
        
        if analysis_response.status_code == 200:
            analysis_data = analysis_response.json()
            
            with open('analysis_result.json', 'w') as f:
                json.dump(analysis_data, f, indent=4)
            
            print("Dữ liệu phân tích đã được lưu vào 'analysis_result.json'")
        else:
            print(f"Không thể lấy dữ liệu phân tích. Mã lỗi: {analysis_response.status_code}")
    else:
        print(f"Tải lên không thành công. Mã lỗi: {response.status_code}")

upload_file_and_get_analysis(file_path)
