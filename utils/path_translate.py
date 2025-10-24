from pathlib import Path


def pathtr(rel_path):
    working_dir = Path().cwd()
    abs_path = working_dir / rel_path
    return abs_path
