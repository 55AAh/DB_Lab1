import os

from datafiles import load_schema, get_datafiles_list
from user import get_env, panic, PANIC_DATA_FOLDER_DOESNT_EXIST


class Fs:
    def __init__(self):
        self.data_folder = get_env("DATA_FOLDER")
        self.schema = None
        self.data_files = None

    def connect(self):
        if not os.path.exists(self.data_folder):
            panic(f"Data folder '{self.data_folder}' doesn't exist!", PANIC_DATA_FOLDER_DOESNT_EXIST)
        self.schema = load_schema(self.data_folder)
        self.data_files = get_datafiles_list(self.data_folder)

    def disconnect(self):
        pass
