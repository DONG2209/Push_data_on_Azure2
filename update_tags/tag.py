from azure.storage.blob import BlobServiceClient
import json

def get_malware_type(analysis_result):
    if 'data' in analysis_result and 'malware_type' in analysis_result['data']:
        return analysis_result['data']['malware_type']
    else:
        return "Unknown"

def update_blob_tags(blob_client, malware_type):
    tags = {"malware_type": malware_type}
    blob_client.set_blob_tags(tags)

def main():
    connection_string = "connection string"
    container_name = "exe"
    blob_name = "Ninite iTunes Installer.exe" 
    analysis_json_path = r"C:\Users\nguye\Downloads\analysis.json"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(analysis_json_path, 'r') as file:
        analysis_result = json.load(file)

    malware_type = get_malware_type(analysis_result)

    update_blob_tags(blob_client, malware_type)
    print(f"Updated blob tags for {blob_client.blob_name} with malware_type: {malware_type}")

if __name__ == "__main__":
    main()
