import shutil
from unittest.mock import Mock


def remove_file_path(path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass


def create_spark_mock():
    spark_mock = Mock()
    type(spark_mock).read = spark_mock
    spark_mock.option.return_value = spark_mock
    return spark_mock
