class Setup:
    def __init__(self, dir_struct_paths):
        self.dir_struct_paths = dir_struct_paths

    def check_dir_struct(self):
        missing_paths = [path for path in self.dir_struct_paths if not path.exists()]
        return False if not missing_paths else missing_paths

    def create_dir_struct(self):
        for dir in self.dir_struct_paths:
            dir.mkdir(parents=True, exist_ok=True)
