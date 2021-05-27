from email.mime.text import MIMEText
import pytest
import os
from datetime import datetime, timezone, timedelta
from tap_ask_nicely.utils import SlackMessenger, SendgridMessenger, GmailMessenger


# @pytest.fixture(autouse=True)
# def mock_env_vars():
#     with mock.patch.dict(os.environ, {"SLACK_WEBHOOK_ADDRESS": "test/webhook/address"}):
#         yield


# @pytest.fixture
# def slack_messenger():
#     return SlackMessenger()


# def test_build_url(slack_messenger):
#     assert (
#         slack_messenger.build_url()
#         == "https://hooks.slack.com/services/test/webhook/address"
#     )


# def test_send_message(slack_messenger):
#     response = slack_messenger.send_message(
#         run_id=123,
#         start_time="Now",
#         run_time=60,
#         record_count=20,
#         status="Success",
#         comment=":red_circle:",
#     )

#     breakpoint()

#     assert response.status_code == 200


@pytest.fixture
def gmail_messenger():
    return GmailMessenger()


@pytest.fixture
def test_data():
    data = {
        "runid": 12345,
        "stream_name": "Test Data Stream",
        "start_time": datetime.now(timezone.utc),
        "run_time": 60,
        "end_time": datetime.now(timezone.utc) + timedelta(hours=1),
        "record_count": 98765,
        "status": "Success",
        "comment": "🟢",
    }

    return data


def test_gmail_messenger(gmail_messenger, test_data):
    gm = gmail_messenger
    part1, part2 = gm.create_message(test_data)

    assert isinstance(part1, MIMEText)
    assert isinstance(part2, MIMEText)

    gm.message.attach(part1)
    gm.message.attach(part2)

    assert len(gm.message._payload) == 2
    assert isinstance(gm.message._payload[0], MIMEText)
    assert isinstance(gm.message._payload[1], MIMEText)

    response = gm.send_message(test_data)

    assert isinstance(response, dict)
    assert len(response) == 0
