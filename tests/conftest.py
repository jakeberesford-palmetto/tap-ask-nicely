import pytest
from tap_ask_nicely.client import AskNicelyClient

import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def base_url():
    return os.getenv("base_url")


@pytest.fixture
def api_key():
    return os.getenv("api_key")


@pytest.fixture
def client(base_url, api_key):
    return AskNicelyClient(base_url, api_key)
