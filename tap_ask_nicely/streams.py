import singer
from typing import Iterator

LOGGER = singer.get_logger()


class Stream:
    tap_stream_id = None
    key_properties = []
    replication_method = ""
    valid_replication_keys = []
    replication_key = "last_updated_at"
    object_type = ""
    selected = True

    def __init__(self, client, state):
        self.client = client
        self.state = state

    def sync(self, *args, **kwargs):
        raise NotImplementedError("Sync of child class not implemented")


class IncrementalStream(Stream):
    replication_method = "INCREMENTAL"


class FullTableStream(Stream):
    replication_method = "FULL_TABLE"


class Response(IncrementalStream):
    tap_stream_id = "response"
    key_properties = ["response_id"]
    replication_key = "start_time_utc"
    valid_replication_keys = ["start_time_utc"]
    object_type = "RESPONSE"

    def sync(self, **kwargs) -> Iterator[dict]:
        pass


STREAMS = {
    "responses": Response,
}
