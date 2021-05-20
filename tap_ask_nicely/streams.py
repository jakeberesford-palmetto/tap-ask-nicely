import singer
import singer.utils as utils
from typing import Generator

LOGGER = singer.get_logger()


class Stream:
    tap_stream_id = None
    key_properties = []
    replication_method = ""
    valid_replication_keys = []
    replication_key = ""
    object_type = ""
    selected = True

    def __init__(self, client, state):
        self.client = client
        self.state = state

    def sync(self, *args, **kwargs):
        raise NotImplementedError("Sync of child class not implemented")


class Unsubscribed(Stream):
    tap_stream_id = "unsubscribed"
    key_properties = ["id"]
    object_type = "UNSUBSCRIBED"
    replication_method = "FULL_TABLE"

    def sync(self) -> Generator[dict, None, None]:
        response = self.client.fetch_unsubscribed()
        unsubscribes = response.get("data")
        for unsubscribe in unsubscribes:
            yield unsubscribe


class Response(Stream):
    tap_stream_id = "response"
    key_properties = ["response_id"]
    replication_key = "start_time_utc"
    valid_replication_keys = ["start_time_utc"]
    object_type = "RESPONSE"
    replication_method = "INCREMENTAL"


    def sync(self, **kwargs) -> Generator[dict, None, None]:
        page = 1
        page_size = 1000
        response_length = page_size
        start_time_utc = singer.get_bookmark(
            self.state,
            self.tap_stream_id,
            self.replication_key,
            default="1970-01-01T00:00:00Z",
        )
        end_time_utc = utils.strftime(utils.now())

        while response_length >= page_size:
            res = self.client.fetch_responses(
                page, page_size, start_time_utc, end_time_utc
            )
            responses = res.get("data", [])
            for response in responses:
                yield response
            page = page + 1
            response_length = len(responses)
            
        # Bookmarking is done in the Sync method
        # singer.write_bookmark(
        #     self.state,
        #     self.tap_stream_id,
        #     self.replication_key,
        #     end_time_utc,
        # )


STREAMS = {
    "unsubscribed": Unsubscribed,
    "responses": Response
    }
