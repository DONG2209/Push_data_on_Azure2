import requests
import json

# Mẫu URL để lấy dữ liệu phân tích
analysis_url_template = "http://42.112.213.93:8000/api/v1/windows/applications/6682aa96f3cb9785840ad70f"


def upload_file_and_get_analysis():
    analysis_response = requests.get(analysis_url_template)
        
    if analysis_response.status_code == 200:
        analysis_data = analysis_response.json()
            
        with open('analysis_result.json', 'w') as f:
            json.dump(analysis_data, f, indent=4)
            
        print("Dữ liệu phân tích đã được lưu vào 'analysis_result.json'")
    else:
        print(f"Không thể lấy dữ liệu phân tích. Mã lỗi: {analysis_response.status_code}")


upload_file_and_get_analysis()
