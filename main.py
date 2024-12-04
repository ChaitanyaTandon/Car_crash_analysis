from utils.utils import FileLoader
import json




data = FileLoader("configs/config.json").load_json_data()

print(data)