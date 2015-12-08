# Tests for Direach
import pandas as pd
import pandas.util.testing as pdt

from direach import direach
import pytest
import responses

SCAST_API = "http://gopivotal-com.socialcast.com/api/"
COMPANY_STREAM_ID = 6712311


def test_api_url_exists():
    assert direach.API_BASE == SCAST_API


@responses.activate
def test_get_streams():
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body="""{"streams": [{"custom_stream": false,
                           "default": true,
                           "id": 6712307,
                           "last_interacted_at": 1449487471,
                           "name": "Home Stream",
                           "new_updates": false}]}""")

    result = direach.get_streams()
    assert len(result) == 1
    assert len(result['streams']) == 1


@responses.activate
def test_get_company_stream_id():
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body="""{"streams": [{"custom_stream": false,
                           "default": false,
                           "id": 6712311,
                           "last_interacted_at": 1449487471,
                           "name": "Company Stream",
                           "new_updates": false}]}""")

    result = direach.get_company_stream_id()
    assert result == COMPANY_STREAM_ID


@responses.activate
def test_raise_if_not_200():
    responses.add(responses.GET, SCAST_API + 'not_an_endpoint',
                  body='{}', status=404)

    with pytest.raises(direach.requests.HTTPError):
        direach.get('not_an_endpoint')


@responses.activate
def test_first_page_of_messages():
    responses.add(responses.GET, SCAST_API + 'streams.json',
                  body="""{"streams": [{"custom_stream": false,
                           "default": false,
                           "id": 6712311,
                           "last_interacted_at": 1449487471,
                           "name": "Company Stream",
                           "new_updates": false}]}""")
    responses.add(responses.GET, SCAST_API + 'streams/' + str(COMPANY_STREAM_ID) + '/messages.json?page=1&per_page=500',
                  body="""{"messages": [{"action": "joined the community",
                           "body": "",
                           "category_id": null,
                           "comments": [],
                           "comments_count": 0,
                           "created_at": "2015-12-07T11:24:31+00:00",
                           "group": {},
                           "groups": [],
                           "id": 1,
                           "last_interacted_at": 1449487471,
                           "permalink_url": "https://gopivotal-com.socialcast.com/messages/28211388-welcome-adam",
                           "thumbnail_url": null,
                           "title": "Welcome, Adam",
                           "updated_at": "2015-12-07T11:24:31+00:00",
                           "url": "https://gopivotal-com.socialcast.com/api/messages/28211388-welcome-adam"
                           }]}""",
                  match_querystring=True)

    result = direach.get_messages(page=1)
    assert len(result) == 1
    assert result[0]['id'] == 1


def test_base_df():
    msg_list = [{"action": "joined the community",
                 "body": "",
                 "category_id": None,
                 "comments": [],
                 "comments_count": 0,
                 "created_at": "2015-12-07T11:24:31+00:00",
                 "group": {},
                 "groups": [],
                 "id": 1,
                 "last_interacted_at": 1449487471,
                 "permalink_url": "https://gopivotal-com.socialcast.com/messages/28211388-welcome-adam",
                 "thumbnail_url": None,
                 "title": "Welcome, Adam",
                 "updated_at": "2015-12-07T11:24:31+00:00",
                 "url": "https://gopivotal-com.socialcast.com/api/messages/28211388-welcome-adam"
                 }]
    result = direach.make_dataframe(msg_list)
    assert isinstance(result, pd.DataFrame)
    pdt.assert_index_equal(result.columns, pd.Index(['title', 'action', 'body', 'comments', 'comments_count',
                                                     'created_at', 'updated_at'], dtype='object'))
    assert result.loc[1]['title'] == 'Welcome, Adam'


def test_message_counts():
    msg_list = [{"action": "joined the community",
                 "body": "",
                 "category_id": None,
                 "comments": [],
                 "comments_count": 0,
                 "created_at": "2015-12-07T11:24:31+00:00",
                 "group": {},
                 "groups": [],
                 "id": 1,
                 "last_interacted_at": 1449487471,
                 "permalink_url": "https://gopivotal-com.socialcast.com/messages/28211388-welcome-adam",
                 "thumbnail_url": None,
                 "title": "Welcome, Adam",
                 "updated_at": "2015-12-07T11:24:31+00:00",
                 "url": "https://gopivotal-com.socialcast.com/api/messages/28211388-welcome-adam"
                 },
                {"action": "asked a question",
                 "body": "CF and Spring",
                 "category_id": None,
                 "comments": [],
                 "comments_count": 0,
                 "created_at": "2015-12-07T11:24:31+00:00",
                 "group": {},
                 "groups": [],
                 "id": 2,
                 "last_interacted_at": 1449487471,
                 "permalink_url": "https://gopivotal-com.socialcast.com/messages/28211388-welcome-adam",
                 "thumbnail_url": None,
                 "title": "Welcome, Adam",
                 "updated_at": "2015-12-07T11:24:31+00:00",
                 "url": "https://gopivotal-com.socialcast.com/api/messages/28211388-welcome-adam"
                 },
                {"action": "asked a question",
                 "body": "@ChampionsGPDB data science",
                 "category_id": None,
                 "comments": [],
                 "comments_count": 0,
                 "created_at": "2015-12-07T11:24:31+00:00",
                 "group": {},
                 "groups": [],
                 "id": 3,
                 "last_interacted_at": 1449487471,
                 "permalink_url": "https://gopivotal-com.socialcast.com/messages/28211388-welcome-adam",
                 "thumbnail_url": None,
                 "title": "Welcome, Adam",
                 "updated_at": "2015-12-07T11:24:31+00:00",
                 "url": "https://gopivotal-com.socialcast.com/api/messages/28211388-welcome-adam"
                 }]
    df = direach.make_dataframe(msg_list)
    result = direach.add_phrase_counts(df)

    assert result.loc[1]['cf_related'] == False
    assert result.loc[2]['cf_related'] == True
    assert result.loc[3]['cf_related'] == False
    assert result.loc[1]['bds_related'] == False
    assert result.loc[2]['bds_related'] == False
    assert result.loc[3]['bds_related'] == True
    assert result.loc[1]['spring_related'] == False
    assert result.loc[2]['spring_related'] == True
    assert result.loc[3]['spring_related'] == False
    assert result.loc[1]['ds_related'] == False
    assert result.loc[2]['ds_related'] == False
    assert result.loc[3]['ds_related'] == True


def test_daily_counts():
    df = pd.DataFrame({'bds_related': {28115834: False,
                                       28182055: False,
                                       28182889: False,
                                       28183158: False,
                                       28188416: True},
                       'cf_related': {28115834: False,
                                      28182055: True,
                                      28182889: True,
                                      28183158: False,
                                      28188416: False},
                       'spring_related': {28115834: False,
                                      28182055: False,
                                      28182889: True,
                                      28183158: False,
                                      28188416: True},
                       'ds_related': {28115834: False,
                                      28182055: False,
                                      28182889: False,
                                      28183158: False,
                                      28188416: True},
                       'created_at': {28115834: pd.Timestamp('2015-12-02 21:03:50'),
                                      28182055: pd.Timestamp('2015-12-05 19:38:33'),
                                      28182889: pd.Timestamp('2015-12-05 20:45:24'),
                                      28183158: pd.Timestamp('2015-12-05 21:13:46'),
                                      28188416: pd.Timestamp('2015-12-06 05:30:03')},
                       'updated_at': {28115834: pd.Timestamp('2015-12-05 20:48:03'),
                                      28182055: pd.Timestamp('2015-12-05 19:38:33'),
                                      28182889: pd.Timestamp('2015-12-05 20:45:24'),
                                      28183158: pd.Timestamp('2015-12-05 21:13:46'),
                                      28188416: pd.Timestamp('2015-12-06 05:30:03')}})
    result = direach.daily_counts(df)

    assert len(result) == 5

    assert result.iloc[0]['bds_related'] == 0
    assert result.iloc[1]['bds_related'] == 0
    assert result.iloc[2]['bds_related'] == 0
    assert result.iloc[3]['bds_related'] == 0
    assert result.iloc[4]['bds_related'] == 1

    assert result.iloc[0]['cf_related'] == 0
    assert result.iloc[1]['cf_related'] == 0
    assert result.iloc[2]['cf_related'] == 0
    assert result.iloc[3]['cf_related'] == 2
    assert result.iloc[4]['cf_related'] == 0

    assert result.iloc[0]['spring_related'] == 0
    assert result.iloc[1]['spring_related'] == 0
    assert result.iloc[2]['spring_related'] == 0
    assert result.iloc[3]['spring_related'] == 1
    assert result.iloc[4]['spring_related'] == 1

    assert result.iloc[0]['ds_related'] == 0
    assert result.iloc[1]['ds_related'] == 0
    assert result.iloc[2]['ds_related'] == 0
    assert result.iloc[3]['ds_related'] == 0
    assert result.iloc[4]['ds_related'] == 1

    assert result.iloc[0]['total_messages'] == 1
    assert result.iloc[1]['total_messages'] == 0
    assert result.iloc[2]['total_messages'] == 0
    assert result.iloc[3]['total_messages'] == 3
    assert result.iloc[4]['total_messages'] == 1
