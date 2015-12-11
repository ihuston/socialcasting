# Tests for Socialcasting
import pandas as pd
import pandas.util.testing as pdt

import socialcasting as sc
import pytest
import json
import responses

SCAST_API = "http://demo.socialcast.com/api/"
COMPANY_STREAM_ID = 234234

# fixtures

stream_response = """{"streams": [{"custom_stream": false,
                           "default": false,
                           "id": 234234,
                           "last_interacted_at": 1449487471,
                           "name": "Company Stream",
                           "new_updates": false}]}"""

python_msg = {"action": "asked a question",
              "body": "",
              "category_id": None,
              "comments": [],
              "comments_count": 0,
              "created_at": "2015-12-07T11:24:31+00:00",
              "group": {},
              "groups": [],
              "id": 1,
              "last_interacted_at": 1449487471,
              "permalink_url": "https://demo.socialcast.com/messages/28211388-welcome-adam",
              "thumbnail_url": None,
              "title": "Welcome, Adam",
              "updated_at": "2015-12-07T11:24:31+00:00",
              "url": "https://demo.socialcast.com/api/messages/28211388-welcome-adam"
              }

page_response = json.dumps({'messages': [python_msg]})
first_page_response = page_response[:-1] + ', "messages_next_page":2}'
second_page_response = page_response[:-1] + ', "messages_next_page":null}'

# tests

def test_api_url_exists():
    assert sc.API_BASE == SCAST_API


@responses.activate
def test_get_streams():
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body=stream_response)

    result = sc.get_streams()
    assert len(result) == 1
    assert len(result['streams']) == 1


@responses.activate
def test_get_company_stream_id():
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body=stream_response)

    result = sc.get_company_stream_id()
    assert result == COMPANY_STREAM_ID


@responses.activate
def test_raise_if_not_200():
    responses.add(responses.GET, SCAST_API + 'not_an_endpoint',
                  body='{}', status=404)

    with pytest.raises(sc.requests.HTTPError):
        sc.get('not_an_endpoint')


@responses.activate
def test_first_page_of_messages():
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body=stream_response)
    responses.add(responses.GET, SCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=1&per_page=500',
                  body=first_page_response,
                  match_querystring=True)

    result = sc.get_messages(page=1)
    assert len(result) == 1
    assert result[0]['id'] == 1


@responses.activate
def test_all_messages(tmpdir):
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body=stream_response)
    responses.add(responses.GET, SCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=1&per_page=500',
                  body=first_page_response,
                  match_querystring=True)
    responses.add(responses.GET, SCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=2&per_page=500',
                  body=second_page_response,
                  match_querystring=True)

    result = sc.get_all_messages(str(tmpdir))
    file_list = tmpdir.listdir()
    print(file_list)
    assert len(file_list) == 2
    assert sorted([f.basename for f in file_list]) == ['messages-0001.json', 'messages-0002.json']


def test_base_df():
    msg_list = [python_msg]
    result = sc.make_dataframe(msg_list)
    assert isinstance(result, pd.DataFrame)
    pdt.assert_index_equal(result.columns, pd.Index(['title', 'action', 'body', 'comments', 'comments_count',
                                                     'created_at', 'updated_at'], dtype='object'))
    assert result.loc[1]['title'] == 'Welcome, Adam'


def test_message_counts():
    m1 = python_msg.copy()
    m1['body'] = ""
    m1['id'] = 1
    m2 = python_msg.copy()
    m2['body'] = "First!"
    m2['id'] = 2
    m3 = python_msg.copy()
    m3['body'] = "@Second, I should also mention the third thing"
    m3['id'] = 3

    msg_list = [m1, m2, m3]
    print(msg_list)
    df = sc.make_dataframe(msg_list)
    phrases_by_name = {'topic_a': 'first|one|uno',
                       'topic_b': 'second|two|duo',
                       'topic_c': 'third|three|tre'}
    result = sc.add_counts_by_name(df, phrases_by_name)

    assert result.loc[1]['topic_a_related'] == False
    assert result.loc[2]['topic_a_related'] == True
    assert result.loc[3]['topic_a_related'] == False
    assert result.loc[1]['topic_b_related'] == False
    assert result.loc[2]['topic_b_related'] == False
    assert result.loc[3]['topic_b_related'] == True
    assert result.loc[1]['topic_c_related'] == False
    assert result.loc[2]['topic_c_related'] == False
    assert result.loc[3]['topic_c_related'] == True


def test_daily_counts():
    df = pd.DataFrame([{'action': 'asked a question',
                        'topic_a_related': False,
                        'topic_b_related': False,
                        'created_at': pd.Timestamp('2015-12-02 21:03:50'),
                        'updated_at': pd.Timestamp('2015-12-05 20:48:03')},
                       {'action': 'posted an idea',
                        'topic_a_related': False,
                        'topic_b_related': True,
                        'created_at': pd.Timestamp('2015-12-05 19:38:33'),
                        'updated_at': pd.Timestamp('2015-12-05 19:38:33')},
                       {'action': 'posted a link',
                        'topic_a_related': False,
                        'topic_b_related': True,
                        'created_at': pd.Timestamp('2015-12-05 20:45:24'),
                        'updated_at': pd.Timestamp('2015-12-05 20:45:24')},
                       {'action': 'made an announcement',
                        'topic_a_related': False,
                        'topic_b_related': False,
                        'created_at': pd.Timestamp('2015-12-05 21:13:46'),
                        'updated_at': pd.Timestamp('2015-12-05 21:13:46')},
                       {'action': 'made a group announcement',
                        'topic_a_related': True,
                        'topic_b_related': False,
                        'created_at': pd.Timestamp('2015-12-06 05:30:03'),
                        'updated_at': pd.Timestamp('2015-12-06 05:30:03')},
                       {'action': 'joined the community',
                        'topic_a_related': False,
                        'topic_b_related': False,
                        'created_at': pd.Timestamp('2015-12-06 06:30:03'),
                        'updated_at': pd.Timestamp('2015-12-06 05:30:03')}])
    result = sc.daily_counts(df)
    assert len(result) == 5

    assert result.iloc[0]['topic_a_related'] == 0
    assert result.iloc[1]['topic_a_related'] == 0
    assert result.iloc[2]['topic_a_related'] == 0
    assert result.iloc[3]['topic_a_related'] == 0
    assert result.iloc[4]['topic_a_related'] == 1

    assert result.iloc[0]['topic_b_related'] == 0
    assert result.iloc[1]['topic_b_related'] == 0
    assert result.iloc[2]['topic_b_related'] == 0
    assert result.iloc[3]['topic_b_related'] == 2
    assert result.iloc[4]['topic_b_related'] == 0

    assert result.iloc[0]['total_messages'] == 1
    assert result.iloc[1]['total_messages'] == 0
    assert result.iloc[2]['total_messages'] == 0
    assert result.iloc[3]['total_messages'] == 3
    assert result.iloc[4]['total_messages'] == 1
