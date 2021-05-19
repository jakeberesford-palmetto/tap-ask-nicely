import pytest
from tap_ask_nicely.streams import Response


@pytest.fixture
def response_stream(client):
    return Response(client, {})


def test_responses_stream_initialization(response_stream):
    assert response_stream.tap_stream_id == "response"
    assert response_stream.key_properties == ["response_id"]
    assert response_stream.replication_method == "INCREMENTAL"
    assert response_stream.valid_replication_keys == ["start_time_utc"]
    assert response_stream.replication_key == "start_time_utc"
    assert response_stream.object_type == "RESPONSE"
    assert response_stream.selected
