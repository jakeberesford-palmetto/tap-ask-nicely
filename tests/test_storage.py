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
