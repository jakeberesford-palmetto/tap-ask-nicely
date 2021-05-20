import singer
import singer.utils as utils
from typing import Generator

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

        contact_ids = set(
            singer.get_bookmark(
                self.state,
                "globals",
                "contact_ids",
                default=[],
            )
        )
        while response_length >= page_size:
            res = self.client.fetch_responses(
                page, page_size, start_time_utc, end_time_utc
            )
            records = res.get("data", [])
            for record in records:
                yield record
                contact_ids.add(record["contact_id"])
            page = page + 1
            response_length = len(records)
        singer.write_bookmark(
            self.state,
            "globals",
            "contact_ids",
            list(contact_ids),
        )
        singer.write_bookmark(
            self.state,
            self.tap_stream_id,
            self.replication_key,
            end_time_utc,
        )


class Contact(FullTableStream):
    tap_stream_id = "contact"
    key_properties = ["id"]
    object_type = "CONTACT"

    def sync(self, **kwargs) -> Generator[dict, None, None]:
        contact_ids = singer.get_bookmark(
            self.state, "globals", "contact_ids", default=set()
        )
        for contact_id in contact_ids:
            response = self.client.fetch_contact(contact_id)
            yield {**response["data"], **{"customproperty_c": None}}


STREAMS = {
    "response": Response,
    "contact": Contact,
}
