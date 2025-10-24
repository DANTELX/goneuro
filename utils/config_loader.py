import json
from utils.path_translate import pathtr


def _load_config(path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"Configuration file ({path}) is missing or malformed: {e}")


class Model:
    def __init__(self):
        self._config_path = pathtr("config/model.json")
        self.json = _load_config(self._config_path)

    def save(self):
        with open(self._config_path, "w") as f:
            json.dump(self.json, f, indent=4, sort_keys=True)

        self.json = _load_config(self._config_path)
