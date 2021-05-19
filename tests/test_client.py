from tap_ask_nicely.client import AskNicelyClient
import pytest


def test_initialization(base_url, api_key, client):
    assert type(client) == AskNicelyClient
    assert client._base_url == base_url
    assert client._api_key == api_key
