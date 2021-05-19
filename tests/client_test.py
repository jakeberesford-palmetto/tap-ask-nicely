import os
import pytest
from dotenv import load_dotenv
from tap_ask_nicely.client import AskNicelyClient

load_dotenv()
config = {
    'subdomain': os.environ('SUBDOMAIN'),
    'api_key': os.environ('API_KEY')
}

@pytest.mark.vcr()
def test_fetch_unsubscribed():
    client = AskNicelyClient(config)

    unsubscribed_data = client.fetch_unsubscribed()
    for unsubscribed in unsubscribed_data:
        assert 'id' in unsubscribed
        assert 'email' in unsubscribed
        assert 'unsubscribedtime' in unsubscribed
        assert 'emailstate' in unsubscribed
        assert 'emailreason' in unsubscribed
