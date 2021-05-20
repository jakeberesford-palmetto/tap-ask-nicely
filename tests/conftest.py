import pytest
from tap_ask_nicely.client import AskNicelyClient

import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def test_config():
    return {"subdomain": os.getenv("SUBDOMAIN"), "api_key": os.getenv("API_KEY")}


@pytest.fixture
def client(test_config):
    return AskNicelyClient(test_config)
