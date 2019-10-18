import json
import os


class CloudInterfaceConfig:
    def __init__(self):
        pass

    @classmethod
    def set_configs(cls, config_json_path: str):
        """
        :param config_json_path:
        :return:
        """
        with open(config_json_path, 'rb') as f:
            config_json = json.load(f)

        for key, value in config_json.items():
            os.environ[key] = value
