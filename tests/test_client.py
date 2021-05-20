import os
import pytest
from dotenv import load_dotenv
from tap_ask_nicely.client import AskNicelyClient

load_dotenv()
config = {"subdomain": os.getenv("SUBDOMAIN"), "api_key": os.getenv("API_KEY")}

@pytest.mark.vcr()
def test_fetch_unsubscribed():
    client = AskNicelyClient(config)

    unsubscribed_data = client.fetch_unsubscribed()
    for unsubscribed in unsubscribed_data["data"]:
        assert "id" in unsubscribed
        assert "email" in unsubscribed
        assert "unsubscribetime" in unsubscribed
        assert "emailstate" in unsubscribed
        assert "emailreason" in unsubscribed


@pytest.mark.vcr()
def test_fetch_responses():
    client = AskNicelyClient(config)
    page = 1
    page_size = 50
    # Uses a UNIX timestamp
    start_time_utc = "2018-01-01T00:00:00Z"
    end_time_utc = "2020-01-01T00:00:00Z"
    res = client.fetch_responses(page, page_size, start_time_utc, end_time_utc)

    responses = res["data"]

    for response in responses:
        assert "response_id" in response
        assert "person_id" in response
        assert "contact_id" in response
        assert "name" in response
        assert "email" in response
        assert "answer" in response
        assert "answerlabel" in response
        assert "data" in response
        assert "comment" in response
        assert "note" in response
        assert "status" in response
        assert "dontcontact" in response
        assert "sent" in response
        assert "opened" in response
        assert "responded" in response
        assert "lastemailed" in response
        assert "created" in response
        assert "segment" in response
        assert "question_type" in response
        assert "published" in response
        assert "publishedname" in response
        assert "publishedavatar" in response
        assert "deliverymethod" in response
        assert "survey_template" in response
        assert "theme" in response
        assert "life_cycle" in response
        assert "customproperty_c" in response
        assert "topic_c" in response
        assert "workflow_zendesk_detractor_c" in response
        assert "workflow_custom_alerts_mary_c" in response
        assert "workflow_email_alert_c" in response
        assert "dashboard" in response
        assert "email_token" in response
