import requests
import gzip
import shutil
from pathlib import Path
from urllib.parse import urlparse
import hashlib
import secrets


# TODO: Method to validate data lists and datasets
class DataCollector:
    def __init__(self, output_data_path):
        self._output_data_path = output_data_path
        self._dataset_data_path = output_data_path / "datasets"
        self._data_lists_path = output_data_path / "lists"
        self._data_lists_downloaded = []
        self._datasets_downloaded = {}

        self._check_dir_struct()
        self.update_db()

    def _check_dir_struct(self):
        if not self._output_data_path.exists():
            raise Exception(f"Output path ({self._output_data_path}) does not exists")

        # Create dir structure if not present
        self._dataset_data_path.mkdir(exist_ok=True)
        self._data_lists_path.mkdir(exist_ok=True)

    def _download_file(self, url, output_path):
        url_parsed = urlparse(url)
        filename = Path(url_parsed.path).name
        output_path = self._output_data_path / output_path / filename

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return output_path

    def _uncompress_gz(self, input_path):
        input_path = self._output_data_path / input_path

        # Remove only the last .gz extension
        if input_path.suffix == ".gz":
            output_path = input_path.with_suffix("")
        else:
            # remove .gz from the end of the name if present
            output_path = Path(str(input_path).replace(".gz", "", 1))

        with gzip.open(input_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        input_path.unlink()

        return output_path

    def update_db(self):
        # Read downloaded lists
        self._data_lists_downloaded = [
            f for f in self._data_lists_path.iterdir() if f.is_file()
        ]

        # Read downloaded datasets as a dict
        self._datasets_downloaded = {}
        datasets_dirs = [f for f in self._dataset_data_path.iterdir() if f.is_dir()]
        for dataset_dir in datasets_dirs:
            files = [f for f in dataset_dir.iterdir() if f.is_file()]
            self._datasets_downloaded[dataset_dir.name] = files

    def download_data_list(self, list_url):
        url_hash = hashlib.md5(list_url.encode()).hexdigest()

        # Check if the list already exists
        for file_path in self._data_lists_downloaded:
            filename = file_path.stem
            if url_hash == filename:
                return url_hash

        # Download list
        saved_file_path = self._download_file(list_url, "./")

        # Uncompress list
        saved_file_path = self._uncompress_gz(saved_file_path)

        # Rename file to the hash of the url and move to lists/
        new_path = self._data_lists_path / (url_hash + str(saved_file_path.suffix))
        saved_file_path.rename(new_path)

        self.update_db()

        return url_hash

    def remove_data_list(self, data_list_stem):
        if data_list_stem not in self.get_lists():
            return False

        for path in self._data_lists_downloaded:
            if path.stem == data_list_stem:
                data_list_path = path
                break

        data_list_path.unlink()

        self.update_db()
        return True

    def get_data_list(self, list_file_stem):
        for list_path in self._data_lists_downloaded:
            if list_file_stem == list_path.stem:
                with open(list_path, "r") as f:
                    return [
                        line.replace("\n", "") for line in f if "subset=warc" in line
                    ]
        return False

    def create_dataset(self):
        random_bytes = secrets.token_bytes(16)
        dir_name = hashlib.md5(random_bytes).hexdigest()
        dataset_path = self._dataset_data_path / dir_name
        dataset_path.mkdir()

        self.update_db()
        return dataset_path.name

    def remove_dataset(self, dataset_dir_name):
        del_path = self._dataset_data_path / dataset_dir_name
        shutil.rmtree(del_path)

        self.update_db()
        return True

    def download_dataset(self, base_url, path, dataset_dir_name):
        path = path.strip()
        base_url = base_url.strip()
        url = base_url + path
        dataset_output_path = self._dataset_data_path / dataset_dir_name

        if not dataset_output_path.exists():
            raise Exception(f"Dataset does not exist ({dataset_dir_name})")

        dataset_file_id = self.check_url(url, base_url)["list_file_stem"]
        print(f"id: {dataset_file_id}")
        if not dataset_file_id:
            raise Exception(f"Url is not valid ({url})")

        # Check if file already exists
        file_exist = self.get_dataset_file(dataset_file_id)

        if file_exist != False:
            # Copy file instead of download it
            shutil.copy(file_exist, dataset_output_path / file_exist.name)

            self.update_db()
            return

        # Download dataset
        download_path = self._download_file(url, dataset_output_path)

        # Rename file with id
        new_path = dataset_output_path / (dataset_file_id + download_path.suffix)
        download_path.rename(new_path)

        self.update_db()

    def get_dataset_file(self, dataset_file_id):
        for dataset in self._dataset_data_path.iterdir():
            for dataset_files in dataset.iterdir():
                if dataset_files.stem == dataset_file_id:
                    return dataset_files

        return False

    def downloaded_dataset_files(self, dataset_id):
        dataset_id_path = self._dataset_data_path / dataset_id

        if not dataset_id_path.exists():
            raise Exception(f"Dataset does not exist ({dataset_id})")

        return [file_id.stem for file_id in dataset_id_path.iterdir()]

    def list_file_to_id(self, base_url, data_list_paths):
        data_list_ids = []
        for list in data_list_paths:
            data_list_ids.append(hashlib.md5((base_url + list).encode()).hexdigest())
        return data_list_ids

    def get_lists(self):
        return [list.stem for list in self._data_lists_downloaded]

    def get_datasets(self):
        return self._datasets_downloaded

    def check_url(self, url, base_url):
        url = url.rstrip("/")

        if base_url not in url:
            return False

        try:
            response = requests.head(url, allow_redirects=True, timeout=10)

            if response.status_code == 405:  # Method Not Allowed
                response = requests.get(url, stream=True, timeout=10)

            if response.status_code == 200:
                # size = None if Content-Lenght not present
                size = response.headers.get("Content-Length")

                response_details = {
                    "list_file_stem": hashlib.md5(url.encode()).hexdigest(),
                    "size": size,
                }

                return response_details

            return False
        except requests.RequestException:
            return False
        finally:
            try:
                response.close()
            except Exception:
                pass
