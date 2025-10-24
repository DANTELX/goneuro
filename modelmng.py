from model.training.collection import DataCollector
from model.training.preprocess import PreprocessData
from model.training.setup import Setup


from utils.config_loader import Model
from utils.path_translate import pathtr

from rich.console import Console
from rich.theme import Theme
from rich.markdown import Markdown
from rich.table import Table
from rich.rule import Rule
from rich.pretty import Pretty
from rich.progress import Progress

from InquirerPy import inquirer
from utils.validators import NumberValidator

import os
import sys
from datetime import datetime

model_config = Model()
custom_theme = Theme({"info": "dim cyan", "warning": "magenta", "danger": "bold red"})
console = Console(theme=custom_theme)


class Modelmgr:
    def __init__(self):
        self._is_env_setup = False
        self.script_name = os.path.splitext(os.path.basename(__file__))[0]
        self.inquirer_keybindings = {
            "answer": [{"key": "enter"}, {"key": "right"}],
            "skip": [{"key": "left"}, {"key": "escape"}],
        }
        self.prompt = "> "
        self._check_env_setup()
        self._menu()

    def _check_env_setup(self):
        # Translate paths from conf file to absolute
        data_struct_paths = list(model_config.json["data_struct_paths"].values())
        data_struct_abs_paths = [pathtr(path) for path in data_struct_paths]

        setup = Setup(data_struct_abs_paths)

        # Check data dir structure
        missing_dir = setup.check_dir_struct()
        if setup.check_dir_struct() != False:
            console.print(
                "[X] File structure missing or incomplete", style="danger", end=""
            )
            missing_dir_str = ""
            for dir in missing_dir:
                missing_dir_str += f"- {dir}\n"
            missing_dir_str += "\n"
            missing_dir_md = Markdown(missing_dir_str)

            console.print(missing_dir_md)

            input_choice = False
            input_choice = inquirer.confirm(
                message="Do you want to create them?", default=True
            ).execute()

            if not input_choice:
                console.print("Missing required directories", style="danger")
                sys.exit(1)

            setup.create_dir_struct()
            console.print("[*] Created missing directories", style="info")

    def _menu(self):
        menu_options = ["COLLECT", "PREPROCESS", "TRAIN", "QUIT"]
        try:
            while True:
                action = inquirer.select(
                    message=self.prompt,
                    choices=menu_options,
                    keybindings=self.inquirer_keybindings,
                    mandatory=False,
                ).execute()

                # Exit if selection is skipped
                if action == None:
                    sys.exit(0)

                # Execute method according to the selected action
                getattr(self, action.lower())()
        except KeyboardInterrupt:
            sys.exit(1)

    def collect(self):
        options = ["ADD", "REMOVE", "INSPECT", "MANAGE", "RETURN"]

        data_dir_path_abs = pathtr(model_config.json["data_struct_paths"]["raw_data"])
        data_collector = DataCollector(data_dir_path_abs)

        # Helper functions
        def dataset_exists(dataset_name):
            dataset_names = [
                dataset["dataset_dir_name"]
                for dataset in model_config.json["collection"]["dataset_dirs"].values()
            ]
            if dataset_name in dataset_names:
                return True

            return False

        def dataset_name_to_id(dataset_name):
            for id, dataset in model_config.json["collection"]["dataset_dirs"].items():
                if dataset["dataset_dir_name"] == dataset_name:
                    return id
            return False

        # Menu functions
        def add_dataset():
            # Get dataset name
            dataset_name = (
                inquirer.text("Dataset name:", mandatory=True).execute().strip()
            )

            # Set data list to dataset
            data_list_url = (
                inquirer.text("Data List Url:", mandatory=True).execute().strip()
            )

            # Check if url is valid
            base_url = model_config.json["collection"]["base_url_data_list"]
            is_url_valid = data_collector.check_url(data_list_url, base_url)

            if not is_url_valid:
                console.print(f"({data_list_url}) is not valid", style="info")
                return

            # Check if list file already exists
            data_list_stem = is_url_valid["list_file_stem"]
            if data_list_stem not in data_collector.get_lists():
                file_size = is_url_valid["size"]
                size_string = ""
                if file_size != None:
                    size_string = f"({file_size} bytes)"
                console.print(f"Data list not present in database")
                choice = inquirer.confirm(
                    f"Do you want to download it? {size_string}",
                    default=True,
                    mandatory=True,
                ).execute()

                if not choice:
                    console.print(
                        "An existing data list is needed for a new dataset",
                        style="danger",
                    )
                    return

                data_list_stem = data_collector.download_data_list(data_list_url)

            # Create dataset
            dataset_dir_name = data_collector.create_dataset()

            dataset_details = {
                "dataset_dir_name": dataset_name,
                "data_list_stem": data_list_stem,
            }

            # Map dir name in json file
            model_config.json["collection"]["dataset_dirs"][
                dataset_dir_name
            ] = dataset_details
            model_config.save()

        def remove_item():
            dataset_id = inquirer.text("Item id:", mandatory=True).execute().strip()

            # Check if datasets exists
            if dataset_id not in model_config.json["collection"]["dataset_dirs"]:
                data_list_id = dataset_id
                if data_list_id not in data_collector.get_lists():
                    console.print(
                        f"Dataset or data list id ({dataset_id}) does not exist",
                        style="warning",
                    )
                    return

                # Check if data list is not in used
                for dataset in model_config.json["collection"]["dataset_dirs"].values():
                    if dataset["data_list_stem"] == data_list_id:
                        console.print(
                            f"Data list ({data_list_id}) is being used by other datasets",
                            style="warning",
                        )
                        return

                data_collector.remove_data_list(data_list_id)
                return

            # Delete data list if not present in other datasets?
            dataset_list_data_stem = model_config.json["collection"]["dataset_dirs"][
                dataset_id
            ]["data_list_stem"]

            other_stems = {
                v.get("data_list_stem")
                for k, v in model_config.json["collection"]["dataset_dirs"].items()
                if k != dataset_id
            }

            if dataset_list_data_stem not in other_stems:
                console.print(
                    "The data list associated with the dataset is not longer in use by other datasets",
                    style="info",
                )
                choice = inquirer.confirm(
                    "Do you want to delete it?", mandatory=True, default=True
                ).execute()

                if choice:
                    data_collector.remove_data_list(dataset_list_data_stem)

            model_config.json["collection"]["dataset_dirs"].pop(dataset_id)
            model_config.save()

            data_collector.remove_dataset(dataset_id)

        def inspect_dataset():
            console.print(Rule("Datasets"))
            if model_config.json["collection"]["dataset_dirs"] == {}:
                console.print("There are no datasets")
            else:
                datasets_table = Table()
                datasets_table.add_column("name", justify="center")
                datasets_table.add_column("id", justify="center")

                for key, item in model_config.json["collection"][
                    "dataset_dirs"
                ].items():
                    datasets_table.add_row(item["dataset_dir_name"], key)

                console.print(datasets_table)

            console.print(Rule("Data lists"))
            data_lists = data_collector.get_lists()
            if not len(data_lists):
                console.print("There are no data lists")
            else:
                data_lists_table = Table()
                data_lists_table.add_column("id", justify="center")
                data_lists_table.add_column("used in", justify="center")

                for data_list_stem in data_lists:
                    list_used_in = []
                    for dataset in model_config.json["collection"][
                        "dataset_dirs"
                    ].values():
                        if dataset["data_list_stem"] == data_list_stem:
                            list_used_in.append(dataset["dataset_dir_name"])

                    if not len(list_used_in):
                        list_used_in = None
                    data_lists_table.add_row(data_list_stem, Pretty(list_used_in))

                console.print(data_lists_table)

        def manage_dataset():
            dataset_name = (
                inquirer.text("Dataset name:", mandatory=True).execute().strip()
            )

            if not dataset_exists(dataset_name):
                console.print(
                    f"Dataset ({dataset_name}) does not exist", style="warning"
                )
                return

            manage_dataset_options = ["DOWNLOAD", "DELETE"]
            manage_choice = (
                inquirer.select(
                    f"({dataset_name}) " + self.prompt,
                    choices=manage_dataset_options,
                    keybindings=self.inquirer_keybindings,
                    mandatory=False,
                )
                .execute()
                .lower()
            )

            if not manage_choice:
                return

            base_url = "https://data.commoncrawl.org/"  # change to config file
            dataset_dir = dataset_name_to_id(dataset_name)
            dataset_list_file_stem = model_config.json["collection"]["dataset_dirs"][
                dataset_dir
            ]["data_list_stem"]
            data_list_paths = data_collector.get_data_list(dataset_list_file_stem)
            data_list_ids = data_collector.list_file_to_id(base_url, data_list_paths)
            data_list_ids_downloaded = data_collector.downloaded_dataset_files(
                dataset_dir
            )
            data_list_ids_missing = list(
                set(data_list_ids) - set(data_list_ids_downloaded)
            )

            files_table = Table(show_header=False)
            files_table.add_column()
            files_table.add_row("Avalilable files", str(len(data_list_paths)))
            files_table.add_row("Downloaded files", str(len(data_list_ids_downloaded)))
            files_table.add_row("Missing files", str(len(data_list_ids_missing)))
            console.print(files_table)

            match manage_choice:
                case "download":
                    number_to_download = int(
                        inquirer.text(
                            f"Number of files to download (1 - {len(data_list_ids_missing)}):",
                            validate=NumberValidator(1, len(data_list_ids_missing)),
                            mandatory=True,
                        ).execute()
                    )

                    full_time = datetime.now().strftime("%H:%M:%S")
                    file_word = "file" if number_to_download == 1 else "files"
                    console.print(
                        f"[{full_time}] Downloading {number_to_download} new {file_word}"
                    )

                    number_downloaded = number_to_download
                    with Progress() as progress:
                        overall_progress = progress.add_task(
                            "Progress", total=number_to_download
                        )
                        for path in data_list_paths:
                            if number_downloaded == 0:
                                break

                            url = base_url + path
                            file_details = data_collector.check_url(url, base_url)
                            file_id = file_details["list_file_stem"]
                            file_size = file_details["size"]

                            if file_id in data_list_ids_missing:
                                # Download
                                task_msg = f"Downloading {file_id} ({file_size} bytes)"
                                data_collector.download_dataset(
                                    base_url, path, dataset_dir, progress, task_msg
                                )
                                progress.update(overall_progress, advance=1)
                                number_downloaded -= 1

                case "delete":
                    number_to_delete = int(
                        inquirer.text(
                            f"Number of files to delete (1 - {len(data_list_ids_downloaded)}):",
                            validate=NumberValidator(1, len(data_list_ids_downloaded)),
                            mandatory=True,
                        ).execute()
                    )
                    for i in range(number_to_delete):
                        file_id = data_list_ids_downloaded[i]
                        data_collector.remove_dataset_file(dataset_dir, file_id)

        actions = {
            "ADD": add_dataset,
            "REMOVE": remove_item,
            "INSPECT": inspect_dataset,
            "MANAGE": manage_dataset,
            "RETURN": self.return_menu,
        }

        while True:
            action = inquirer.select(
                message=self.prompt,
                choices=options,
                keybindings=self.inquirer_keybindings,
                mandatory=False,
            ).execute()

            # Return to main menu if skipped
            if action == None:
                return

            # Call corresponding function in the dictionary
            result = actions[action]()

            # Return or continue according to the return value of the function
            if result == "return":
                return

    def preprocess(self):
        options = ["PREPROCESS", "LIST", "RETURN"]

        def preprocess():
            pass

        def list_preprocess_data():
            pass

        actions = {
            "PREPROCESS": preprocess,
            "LIST": list_preprocess_data,
            "RETURN": self.return_menu,
        }

        while True:
            action = inquirer.select(
                message=self.prompt,
                choices=options,
                keybindings=self.inquirer_keybindings,
                mandatory=False,
            ).execute()

            # Return to main menu if skipped
            if action == None:
                return

            # Call corresponding function in the dictionary
            result = actions[action]()

            # Return or continue according to the return value of the function
            if result == "return":
                return

    def train(self):
        pass

    def return_menu(self):
        return "return"

    def quit(self):
        sys.exit(0)


modelmgr = Modelmgr()
