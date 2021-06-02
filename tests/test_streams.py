from datetime import datetime, timedelta
import os
from tests.conftest import config
import pytest
import json
from tap_ask_nicely.streams import (
    Response,
    Contact,
    Unsubscribed,
    SentStatistics,
    HistoricalStats,
    NpsScore,
    STREAMS,
)

import singer
import singer.utils as utils


def vcr_responses_ignore_end_time_utc(r1, r2):
    r1_uri = r1.uri.split("/")
    r2_uri = r2.uri.split("/")

    assert r1_uri[0:9] == r2_uri[0:9]


@pytest.fixture
def response_stream(client, state, config):
    return Response(client, state, config)


@pytest.fixture
def contact_stream(client, state, config):
    return Contact(client, state, config)


@pytest.fixture
def unsubscribed_stream(client, state, config):
    return Unsubscribed(client, state, config)


@pytest.fixture
def sent_statistics_stream(client, state, config):
    return SentStatistics(client, state, config)


@pytest.fixture
def historical_stats_stream(client, state, config):
    return HistoricalStats(client, state, config)

@pytest.fixture
def nps_score_stream(client, state, config):
    return NpsScore(client, state, config)


def test_unsubscribed_stream_properties(unsubscribed_stream):
    assert unsubscribed_stream.tap_stream_id == "unsubscribed"
    assert unsubscribed_stream.key_properties == ["id"]
    assert unsubscribed_stream.replication_method == "FULL_TABLE"
    assert unsubscribed_stream.object_type == "UNSUBSCRIBED"
    assert unsubscribed_stream.tap_stream_id in STREAMS
    assert STREAMS[unsubscribed_stream.tap_stream_id] == Unsubscribed


@pytest.mark.vcr()
def test_unsubscribed_stream_sync(unsubscribed_stream):
    for unsubscribed in unsubscribed_stream.sync():
        assert "id" in unsubscribed
        assert "email" in unsubscribed
        assert "unsubscribetime" in unsubscribed
        assert "emailstate" in unsubscribed
        assert "emailreason" in unsubscribed


def test_response_stream_properties(response_stream):
    assert response_stream.tap_stream_id == "response"
    assert response_stream.key_properties == ["response_id"]
    assert response_stream.replication_method == "INCREMENTAL"
    assert response_stream.valid_replication_keys == ["start_time_utc"]
    assert response_stream.replication_key == "start_time_utc"
    assert response_stream.object_type == "RESPONSE"
    assert response_stream.tap_stream_id in STREAMS
    assert STREAMS[response_stream.tap_stream_id] == Response


# @pytest.mark.vcr()
# Need to ignore the end_time_utc section of the URI still before using VCR
def test_response_stream_sync(response_stream, local_file_path):
    contact_ids = set()
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
        contact_ids.add(record["contact_id"])

    finished_sync_bookmark = singer.get_bookmark(
        response_stream.state,
        response_stream.tap_stream_id,
        response_stream.replication_key,
    )
    finished_sync_datetime_obj = utils.strptime_to_utc(finished_sync_bookmark)
    now_obj = utils.now()
    assert finished_sync_datetime_obj.date() == now_obj.date()

    file_contact_ids = []

    with open(local_file_path, "r") as f:
        file_contact_ids = json.loads(f.read())

    assert len(contact_ids) == len(file_contact_ids)
    for contact_id in file_contact_ids:
        assert contact_id in contact_ids


def test_contact_stream_properties(contact_stream):
    assert contact_stream.tap_stream_id == "contact"
    assert contact_stream.key_properties == ["id"]
    assert contact_stream.replication_method == "FULL_TABLE"
    assert contact_stream.object_type == "CONTACT"
    assert contact_stream.tap_stream_id in STREAMS
    assert STREAMS[contact_stream.tap_stream_id] == Contact


# @pytest.mark.vcr()
# Need to ignore the end_time_utc section of the URI still before using VCR
def test_contact_stream_sync(response_stream, contact_stream, local_file_path):
    # response stream must be ran first to get all contacts
    list(response_stream.sync())

    num_records = 0
    for record in contact_stream.sync():
        assert "id" in record
        assert "lastemailed" in record
        assert "active" in record
        assert "created" in record
        assert "scheduled" in record
        assert "firstname" in record
        assert "lastname" in record
        assert "name" in record
        assert "email" in record
        assert "company_id" in record
        assert "segment" in record
        assert "unsubscribetime" in record
        assert "source" in record
        assert "sourceid" in record
        assert "importedattime" in record
        assert "intercomid" in record
        assert "imported" in record
        assert "profiled" in record
        assert "profile" in record
        assert "avatar" in record
        assert "publishedavatar" in record
        assert "publishedavatarcolor" in record
        assert "deleted" in record
        assert "emailstate" in record
        assert "intercomsegmentid" in record
        assert "surveycount" in record
        assert "delayminutes" in record
        assert "emailreason" in record
        assert "scheduler_id" in record
        assert "elastic_at" in record
        assert "life_cycle" in record
        assert "sfaccountid" in record
        assert "dynamicscontactid" in record
        assert "responsetime" in record
        assert "channel" in record
        assert "sflastsenthash" in record
        assert "sflastsenttime" in record
        assert "sfcontactid" in record
        assert "integrationsegment_id" in record
        assert "importer_id" in record
        assert "sfcaseid" in record
        assert "sfleadid" in record
        assert "applicationid" in record
        assert "mc_contact_identifier" in record
        assert "event" in record
        assert "triggeremail" in record
        assert "obeyrules" in record
        assert "thendeactivate" in record
        assert "customproperty_c" in record
        num_records = num_records + 1
    file_contact_ids = []

    with open(local_file_path, "r") as f:
        file_contact_ids = json.loads(f.read())

    assert num_records == len(file_contact_ids)


@pytest.mark.vcr()
def test_sent_statistics(sent_statistics_stream):
    for sent_stat in sent_statistics_stream.sync():
        assert "nps" in sent_stat
        assert "sent" in sent_stat
        assert "delivered" in sent_stat
        assert "opened" in sent_stat
        assert "responded" in sent_stat
        assert "promoters" in sent_stat
        assert "passives" in sent_stat
        assert "detractors" in sent_stat
        assert "responserate" in sent_stat


# @pytest.mark.vcr()
def test_historical_stats(historical_stats_stream):
    last_sync_date = datetime.strftime((datetime.now() - timedelta(days=1)), "%Y-%m-%d")
    state = {"bookmarks": {"historical_stats": {"last_sync_date": last_sync_date}}}
    historical_stats_stream.state = state

    for historical_stat in historical_stats_stream.sync():
        assert "year" in historical_stat
        assert "month" in historical_stat
        assert "day" in historical_stat
        assert "weekday" in historical_stat
        assert "sent" in historical_stat
        assert "delivered" in historical_stat
        assert "opened" in historical_stat
        assert "responded" in historical_stat
        assert "promoters" in historical_stat
        assert "passives" in historical_stat
        assert "detractors" in historical_stat
        assert "nps" in historical_stat
        assert "comments" in historical_stat
        assert "comment_length" in historical_stat
        assert "comment_responded_percent" in historical_stat
        assert "surveys_responded_percent" in historical_stat
        assert "surveys_delivered_responded_percent" in historical_stat
        assert "surveys_opened_responded_percent" in historical_stat

    assert state["bookmarks"][historical_stats_stream.tap_stream_id][
        "last_sync_date"
    ] == datetime.strftime(datetime.now(), "%Y-%m-%d")

def test_nps_score(nps_score_stream):
    for nps in nps_score_stream.sync():
        assert "NPS" in nps
        