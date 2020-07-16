from pathlib import Path
import os
import pytest
from L3MinioCache import PickleL2Cache


@pytest.fixture(autouse=True)
def run_around_tests():
    Path('./local_storage').mkdir(exist_ok=True)
    yield


def test_simple_file_path():
    storage = PickleL2Cache('./local_storage', '')
    assert str(storage.get_path("some_name")).endswith(str(Path("local_storage") / "some_name"))


def test_simple_file_exists():
    storage = PickleL2Cache('./local_storage')
    assert not storage.exists("some_name")
    storage.dump("some_name", "data")
    assert storage.exists("some_name")
    os.remove(storage.get_path("some_name"))
    assert not storage.exists("some_name")


def test_file_check():
    storage = PickleL2Cache('./local_storage')
    storage.dump("some_name", "data")
    data = storage.load("some_name")
    assert data == "data"
    os.remove(storage.get_path("some_name"))


def test_file_b_check():
    storage = PickleL2Cache('./local_storage')
    storage.dump("some_name", "data")
    with open(storage.get_path("some_name"), 'rb') as f:
        data = storage.load_b(f)
    assert data == "data"
    os.remove(storage.get_path("some_name"))