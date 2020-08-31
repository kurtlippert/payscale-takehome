from app import (
    get_storage_path,
    get_file_size_in_bytes,
    get_file_mime_type
)
import os


def test_get_storage_path():
    storage_path = get_storage_path('data/processed')
    assert storage_path == os.path.join(
        os.path.dirname(__file__), 'data/processed')


def test_get_file_size():
    file_path = os.path.join(
        os.path.dirname(__file__), 'data/employee-test.csv')
    file_size_in_bytes = get_file_size_in_bytes(file_path)
    assert file_size_in_bytes == 36911


def test_get_file_mime_type():
    xlsx_file_path = os.path.join(
        os.path.dirname(__file__), 'data/employee-test.xlsx')
    mime_type = get_file_mime_type(xlsx_file_path)
    assert mime_type in [
       'application/xlsx',
       'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ]
