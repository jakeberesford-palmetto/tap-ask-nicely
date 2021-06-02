from email.mime.text import MIMEText
import pytest
from pytest_mock import mocker
from unittest.mock import MagicMock, Mock
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
def test_data():
    data = {
        "run_id": 12345,
        "start_time": datetime.now(timezone.utc),
        "run_time": 60,
        "record_count": 98765,
        "status": "",
        "comments": "",
    }

    return data


def test_gmail_messenger(test_data):
    data = test_data
    gm = GmailMessenger(data)
    part1, part2, message = gm.create_message()

    assert isinstance(part1, MIMEText)
    assert isinstance(part2, MIMEText)

    message.attach(part1)
    message.attach(part2)

    assert len(message._payload) == 2
    assert isinstance(message._payload[0], MIMEText)
    assert isinstance(message._payload[1], MIMEText)

    response = gm.send_message()

    assert response == "Email sent successfully."

    gm.send_message = MagicMock(return_value="There was an issue sending the email: {error}")

    assert gm.send_message() == "There was an issue sending the email: {error}"


def test_sendgrid_messenger(test_data):
    sg = SendgridMessenger(test_data)
    response = sg.send_message()

    assert response == "Email sent successfully."

    sg.send_message = MagicMock(return_value="There was an issue sending the email: {error}")

    assert sg.send_message() == "There was an issue sending the email: {error}"