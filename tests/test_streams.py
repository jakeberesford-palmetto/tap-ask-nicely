import pytest
from tap_ask_nicely.streams import Response, Contact, STREAMS
import singer
import singer.utils as utils


def vcr_responses_ignore_end_time_utc(r1, r2):
    r1_uri = r1.uri.split("/")
    r2_uri = r2.uri.split("/")

    assert r1_uri[0:9] == r2_uri[0:9]


@pytest.fixture
def response_stream(client):
    return Response(client, {})


def test_response_stream_initialization(response_stream):
    assert response_stream.tap_stream_id == "response"
    assert response_stream.key_properties == ["response_id"]
    assert response_stream.replication_method == "INCREMENTAL"
    assert response_stream.valid_replication_keys == ["start_time_utc"]
    assert response_stream.replication_key == "start_time_utc"
    assert response_stream.object_type == "RESPONSE"
    assert response_stream.selected
    assert response_stream.tap_stream_id in STREAMS
    assert STREAMS[response_stream.tap_stream_id] == Response


# @pytest.mark.vcr()
# Need to ignore the end_time_utc section of the URI still before using VCR
def test_response_stream_sync(response_stream):
    for record in response_stream.sync():
        assert "response_id" in record
        assert "person_id" in record
        assert "contact_id" in record
        assert "name" in record
        assert "email" in record
        assert "answer" in record
        assert "answerlabel" in record
        assert "data" in record
        assert "comment" in record
        assert "note" in record
        assert "status" in record
        assert "dontcontact" in record
        assert "sent" in record
        assert "opened" in record
        assert "responded" in record
        assert "lastemailed" in record
        assert "created" in record
        assert "segment" in record
        assert "question_type" in record
        assert "published" in record
        assert "publishedname" in record
        assert "publishedavatar" in record
        assert "deliverymethod" in record
        assert "survey_template" in record
        assert "theme" in record
        assert "life_cycle" in record
        assert "customproperty_c" in record
        assert "topic_c" in record
        assert "workflow_zendesk_detractor_c" in record
        assert "workflow_custom_alerts_mary_c" in record
        assert "workflow_email_alert_c" in record
        assert "dashboard" in record
        assert "email_token" in record
    finished_sync_bookmark = singer.get_bookmark(
        response_stream.state,
        response_stream.tap_stream_id,
        response_stream.replication_key,
    )
    finished_sync_datetime_obj = utils.strptime_to_utc(finished_sync_bookmark)
    now_obj = utils.now()
    assert finished_sync_datetime_obj.date() == now_obj.date()
