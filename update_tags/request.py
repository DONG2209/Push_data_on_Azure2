import requests
import os

# URL của API để tải lên file
upload_url = "http://42.112.213.93:8080/api/v1/windows/applications"

file_path = r"C:\Users\nguye\Downloads\truykich\truykich\TruyKich.exe"

def upload_file_chunked(upload_url, file_path):
    chunk_size = 1024  # Kích thước của mỗi chunk là 1 MB (có thể điều chỉnh)
    with open(file_path, 'rb') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            file_name = os.path.basename(file_path)
            files = {'file': (file_name, chunk, 'application/octet-stream')}
            response = requests.post(upload_url, files=files)
            if response.status_code != 200:
                print(f"Tải lên không thành công. Mã lỗi: {response.status_code}")
                return None
            data = response.json()
    return data

upload_file_chunked(upload_url,file_path)
