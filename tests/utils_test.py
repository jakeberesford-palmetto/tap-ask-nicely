import pytest
import os
from unittest import mock
from tap_ask_nicely.utils import SlackMessenger


# @pytest.fixture(autouse=True)
# def mock_env_vars():
#     with mock.patch.dict(os.environ, {"SLACK_WEBHOOK_ADDRESS": "test/webhook/address"}):
#         yield


def test_build_url():
    slack_messenger = SlackMessenger()

    assert (
        slack_messenger.build_url()
        == "https://hooks.slack.com/services/test/webhook/address"
    )


def test_send_message():
    slack_messenger = SlackMessenger()

    response = slack_messenger.send_message(
        run_id=123,
        start_time="Now",
        run_time=60,
        record_count=20,
        status="Success",
        comment=":red_circle:",
    )

    breakpoint()

    assert response.status_code == 200
