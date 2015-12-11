# Socialcasting - API helper for Socialcast
import json

import requests
# noinspection PyUnresolvedReferences
from requests.exceptions import HTTPError
import os

API_BASE = os.getenv('SOCIALCAST_API', "http://demo.socialcast.com/api/")
username = os.getenv('SOCIALCAST_USERNAME', 'emily@socialcast.com')
password = os.getenv('SOCIALCAST_PASSWORD', 'demo')


def get(query, params=None):
    """Make an API call with specified query and parameters."""
    req = requests.get(API_BASE + query, params=params, auth=(username, password))
    req.raise_for_status()
    return req.json()


def get_streams():
    """Get all streams available to this user."""
    return get("streams.json")


def get_company_stream_id():
    """Get the id for the Company Stream, a list of all public messages."""
    result = get("streams.json")
    stream_id = [s for s in result["streams"] if s["name"] == "Company Stream"][0]["id"]
    return stream_id


def get_raw_response(page=1, per_page=500):
    """Get the raw response from the API for a single page of messages from company stream."""
    params = {'page': page, 'per_page': per_page}
    result = get("streams/" + str(get_company_stream_id()) + "/messages.json", params)
    return result


def get_messages(page=1, per_page=500):
    """Get a single page of messages from the company stream."""
    response = get_raw_response(page, per_page)
    return response['messages']


def get_all_messages(storage_dir="../data"):
    """
    Retrieve all messages from the Company Stream and store as JSON files in the
    specified directory.

    Args:
        storage_dir: Directory to store JSON files.

    """
    page = 1
    per_page = 500
    while page is not None:
        raw = get_raw_response(page, per_page)
        storage_path = os.path.join(storage_dir, 'messages-' + str(page).zfill(4) + '.json')
        print(storage_path)
        with open(storage_path, "w") as f:
            f.write("\n".join([json.dumps(m) for m in raw['messages']]))
        print("Saved page " + str(page))
        page = raw["messages_next_page"]
    return
