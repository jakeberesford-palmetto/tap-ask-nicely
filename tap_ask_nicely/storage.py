import boto3
import json


def create_source_handler(config):
    protocol = config.get("protocol")
    if protocol == "s3":
        return S3Handler(config)
    else:
        return LocalFileHandler(config)


class StorageHandler:
    def __init__(self, config):
        self._config = config
        self._source_handler = create_source_handler(self._config)

    def read_file(self, file_path):
        pass

    def write_file(self, file_path, data):
        pass


class S3Handler:
    def __init__(self, config):
        self._session = boto3.Session(
            aws_access_key_id=config["credentials"]["aws_access_key_id"],
            aws_secret_access_key=config["credentials"]["aws_secret_access_key"],
        )
        self._s3 = self._session.resource("s3")
        self._config = config

    def read_file(self, file_path):
        object = self._s3.Object(self._config["bucket"], file_path)
        return json.loads(object.get()["Body"].read())

    def write_file(self, file_path, data):
        obj = self._s3.Object(self._config["bucket"], file_path)
        obj.put(Body=json.dumps(data))


class LocalFileHandler:
    def __init__(self, config):
        self._config = config

    def read_file(self, file_path):
        with open(file_path) as f:
            return json.load(f)

    def write_file(self, file_path, data):
        with open(file_path, "w") as f:
            f.write(json.dumps(data))
