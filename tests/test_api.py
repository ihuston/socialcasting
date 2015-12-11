# Tests for Socialcasting
import pytest
import responses

import socialcasting as sc
from fixtures import stream_response, first_page_response, second_page_response

SOCIALCAST_API = "http://demo.socialcast.com/api/"
COMPANY_STREAM_ID = 234234


def test_api_url_exists():
    assert sc.API_BASE == SOCIALCAST_API


@responses.activate
def test_get_streams():
    responses.add(responses.GET, SOCIALCAST_API + 'streams.json',
                  body=stream_response)

    result = sc.get_streams()
    assert len(result) == 1
    assert len(result['streams']) == 1


@responses.activate
def test_get_company_stream_id():
    responses.add(responses.GET, SOCIALCAST_API + 'streams.json',
                  body=stream_response)

    result = sc.get_company_stream_id()
    assert result == COMPANY_STREAM_ID


@responses.activate
def test_raise_if_not_200():
    responses.add(responses.GET, SOCIALCAST_API + 'not_an_endpoint',
                  body='{}', status=404)

    with pytest.raises(sc.requests.HTTPError):
        sc.get('not_an_endpoint')


@responses.activate
def test_first_page_of_messages():
    responses.add(responses.GET, SOCIALCAST_API + 'streams.json',
                  body=stream_response)
    responses.add(responses.GET, SOCIALCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=1&per_page=500',
                  body=first_page_response,
                  match_querystring=True)

    result = sc.get_messages(page=1)
    assert len(result) == 1
    assert result[0]['id'] == 1


@responses.activate
def test_all_messages(tmpdir):
    responses.add(responses.GET, SOCIALCAST_API + 'streams.json',
                  body=stream_response)
    responses.add(responses.GET, SOCIALCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=1&per_page=500',
                  body=first_page_response,
                  match_querystring=True)
    responses.add(responses.GET, SOCIALCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=2&per_page=500',
                  body=second_page_response,
                  match_querystring=True)

    result = sc.get_all_messages(str(tmpdir))
    file_list = tmpdir.listdir()
    print(file_list)
    assert len(file_list) == 2
    assert sorted([f.basename for f in file_list]) == ['messages-0001.json', 'messages-0002.json']


