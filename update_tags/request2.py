import requests
import json

# URL để tải lên file
upload_url = "http://42.112.213.93:8000/api/v1/windows/applications"


file_path = r"C:\Users\nguye\Downloads\truykich\truykich\TruyKich.exe"

def upload_file_and_get_analysis(file_path):
    files = {'file': open(file_path, 'rb')}
    response = requests.get(upload_url, files=files)
    
    if response.status_code == 200:
        data = response.json()
        print (data)
    else:
        print(f"Tải lên không thành công. Mã lỗi: {response.status_code}")


upload_file_and_get_analysis(file_path)
