import boto3
from moto import mock_s3
import pytest
import json
from tap_ask_nicely.storage import *


@pytest.fixture
def bucket_name():
    return "my-bucket"


@pytest.fixture
def s3_config(aws_credentials, bucket_name, file_path):
    return {
        "protocol": "s3",
        "credentials": aws_credentials,
        "bucket": bucket_name,
        "file_path": file_path,
    }


@pytest.fixture
def bucket(s3, bucket_name, file_path, raw_file_data):
    bucket = s3.create_bucket(Bucket=bucket_name)
    bucket.put_object(Body=json.dumps(raw_file_data), Key=file_path)
    yield


@pytest.fixture
def s3_handler(bucket, s3_config):
    return S3Handler(s3_config)


@pytest.fixture
def local_file_handler(config):
    return LocalFileHandler(config)


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
    local_file_handler, raw_file_data, local_file_path
):
    # Create a test file
    with open(local_file_path, "w") as fp:
        fp.write(json.dumps(raw_file_data))

    data = local_file_handler.read_file(local_file_path)
    assert data == raw_file_data


def test_local_file_handler_write_file(
    local_file_handler, raw_file_data, local_file_path
):
    local_file_handler.write_file(local_file_path, raw_file_data)

    with open(local_file_path, "r") as fp:
        assert json.loads(fp.read()) == raw_file_data


def test_storage_handler_read_file(
    raw_file_data, file_path, s3_config, config, bucket, local_file_path
):
    # create testing file for local reads
    with open(local_file_path, "w") as fp:
        fp.write(json.dumps(raw_file_data))

    s3_storage_handler = StorageHandler(s3_config)
    local_file_storage_handler = StorageHandler(config)
    assert type(s3_storage_handler._source_handler) == S3Handler
    assert type(local_file_storage_handler._source_handler) == LocalFileHandler

    assert s3_storage_handler.read_file(file_path) == raw_file_data
    assert local_file_storage_handler.read_file(local_file_path) == raw_file_data


def test_storage_handler_write_file(
    raw_file_data,
    file_path,
    local_file_path,
    s3_config,
    config,
    s3,
    bucket,
    bucket_name,
):
    s3_storage_handler = StorageHandler(s3_config)
    local_file_storage_handler = StorageHandler(config)

    s3_storage_handler.write_file(file_path, raw_file_data)
    local_file_storage_handler.write_file(local_file_path, raw_file_data)

    s3_data = json.loads(s3.Object(bucket_name, file_path).get()["Body"].read())

    assert s3_data == raw_file_data
    with open(local_file_path, "r") as fp:
        assert json.loads(fp.read()) == raw_file_data
