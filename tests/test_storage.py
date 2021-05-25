import boto3
from moto import mock_s3
import pytest
import json
from tap_ask_nicely.storage import *


@pytest.fixture
def bucket_name():
    return "my-bucket"


@pytest.fixture
def s3_config(aws_credentials, bucket_name):
    return {"protocol": "s3", "credentials": aws_credentials, "bucket": bucket_name}


@pytest.fixture
def raw_file_data():
    return [1, 2, 3, 5]


@pytest.fixture
def file_path():
    return "test-file-path.json"


@pytest.fixture
def bucket(s3, s3_config, bucket_name, file_path, raw_file_data):
    bucket = s3.create_bucket(Bucket=bucket_name)
    bucket.put_object(Body=json.dumps(raw_file_data), Key=file_path)
    yield


@pytest.fixture
def s3_handler(bucket, s3_config):
    return S3Handler(s3_config)


@pytest.fixture
def local_file_handler():
    return LocalFileHandler({})


def test_create_source_handler(s3_config):
    assert type(create_source_handler(s3_config)) == S3Handler
    assert type(create_source_handler({})) == LocalFileHandler


def test_s3_handler_creates_valid_session(s3_handler, s3_config):
    assert type(s3_handler._session) == boto3.Session
    assert s3_handler._s3 is not None
    assert s3_handler._config == s3_config


def test_s3_handler_read_file(s3_handler, file_path, raw_file_data):
    data = s3_handler.read_file(file_path)
    assert data == raw_file_data


def test_s3_handler_write_file(s3, s3_handler, bucket_name, file_path):
    raw_data = [10, 11]
    s3_handler.write_file(file_path, raw_data)

    obj = s3.Object(bucket_name, file_path)
    obj_data = json.loads(obj.get()["Body"].read())

    assert obj_data == raw_data


def test_local_file_handler_read_file(
    local_file_handler, raw_file_data, file_path, tmpdir
):
    # Create a test file
    full_path = tmpdir.join(file_path)
    with open(full_path, "w") as fp:
        fp.write(json.dumps(raw_file_data))

    data = local_file_handler.read_file(full_path)
    assert data == raw_file_data


def test_local_file_handler_write_file(
    local_file_handler, raw_file_data, file_path, tmpdir
):
    full_path = tmpdir.join(file_path)
    local_file_handler.write_file(full_path, raw_file_data)

    with open(full_path, "r") as fp:
        assert json.loads(fp.read()) == raw_file_data


def test_storage_handler_read_file(raw_file_data, file_path, s3_config, bucket, tmpdir):
    # create testing file for local reads
    full_path = tmpdir.join(file_path)
    with open(full_path, "w") as fp:
        fp.write(json.dumps(raw_file_data))

    s3_storage_handler = StorageHandler(s3_config)
    local_file_storage_handler = StorageHandler({})
    assert type(s3_storage_handler._source_handler) == S3Handler
    assert type(local_file_storage_handler._source_handler) == LocalFileHandler

    assert s3_storage_handler.read_file(file_path) == raw_file_data
    assert local_file_storage_handler.read_file(full_path) == raw_file_data


def test_storage_handler_write_file(
    raw_file_data, file_path, s3_config, s3, bucket, bucket_name, tmpdir
):
    # create testing file for local reads
    full_path = tmpdir.join(file_path)

    s3_storage_handler = StorageHandler(s3_config)
    local_file_storage_handler = StorageHandler({})
    new_data = [10, 11]

    s3_storage_handler.write_file(file_path, new_data)
    local_file_storage_handler.write_file(full_path, new_data)

    s3_data = json.loads(s3.Object(bucket_name, file_path).get()["Body"].read())

    assert s3_data == new_data
    with open(full_path, "r") as fp:
        assert json.loads(fp.read()) == new_data
