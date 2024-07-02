import os
file_path=r"C:\Users\nguye\Downloads\truykich\truykich\TruyKich.exe"

b="description_"+f"{os.path.splitext(os.path.basename(file_path))[0]}"+".json"
print(b)