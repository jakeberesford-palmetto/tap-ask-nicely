import os
import pytest
from dotenv import load_dotenv
from tap_ask_nicely.client import AskNicelyClient
from tap_ask_nicely.streams import Unsubscribed

load_dotenv()
config = {"subdomain": os.getenv("SUBDOMAIN"), "api_key": os.getenv("API_KEY")}
client = AskNicelyClient(config)


@pytest.mark.vcr()
def test_unsubscribed():
    stream = Unsubscribed(client=client, state={})

    unsubscribed_data = stream.sync()
    for unsubscribed in unsubscribed_data:
        assert "id" in unsubscribed
        assert "email" in unsubscribed
        assert "unsubscribetime" in unsubscribed
        assert "emailstate" in unsubscribed
        assert "emailreason" in unsubscribed
