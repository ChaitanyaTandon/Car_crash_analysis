import json

class FileLoader:
    
    def __init__(self):
        pass
    
    def load_json_data(self,file_path):
        self.file_path = file_path
        with open(self.file_path, 'r') as file:
               data = json.load(file)
        return data
    
    def load_questions_data_mapping(self,file_path):
        self.file_path = file_path
        with open(self.file_path, 'r') as file:
               data = json.load(file)
        return data