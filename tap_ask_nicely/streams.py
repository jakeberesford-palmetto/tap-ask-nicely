import singer

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

    def sync(self):
        response = self.client.fetch_unsubscribed()
        unsubscribes = response.get("data")
        for unsubscribe in unsubscribes:
            yield unsubscribe


class Audit(Stream):
    tap_stream_id = "audit"
    key_properties = []
    object_type = "AUDIT"
    replication_method = "FULL_TABLE"

    def sync(self):
        response = self.client.fetch_unsubscribed()
        unsubscribes = response.get("data")
        for unsubscribe in unsubscribes:
            yield unsubscribe


STREAMS = {"unsubscribed": Unsubscribed}
