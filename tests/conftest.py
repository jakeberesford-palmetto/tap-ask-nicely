import pytest
from tap_ask_nicely.client import AskNicelyClient

import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def config():
    return {
        "subdomain": os.getenv("SUBDOMAIN"),
        "api_key": os.getenv("API_KEY"),
        "start_date": "2020-01-01",
    }


@pytest.fixture
def client(config):
    return AskNicelyClient(config)


@pytest.fixture
def state():
    return {}
