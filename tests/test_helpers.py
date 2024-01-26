import shutil
from unittest.mock import Mock


def remove_file_path(path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass
