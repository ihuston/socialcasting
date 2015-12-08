# Direach - API helper for Socialcast
import pandas as pd
import requests
from requests.exceptions import HTTPError
import os

API_BASE = "http://gopivotal-com.socialcast.com/api/"

password = os.getenv('SOCIALCAST_PASSWORD', None)


def get(query, params=None):
    req = requests.get(API_BASE + query, params=params, auth=("ihuston@pivotal.io", password))
    req.raise_for_status()
    return req.json()


def get_streams():
    return get("streams.json")


def get_company_stream_id():
    result = get("streams.json")
    stream_id = [s for s in result["streams"] if s["name"] == "Company Stream"][0]["id"]
    return stream_id


def get_messages(page=1, per_page=500):
    params = {'page': page, 'per_page': per_page}
    result = get("streams/" + str(get_company_stream_id()) + "/messages.json", params)
    return result['messages']


def make_dataframe(msg_list):
    base_df = pd.DataFrame(msg_list)
    new_df = base_df.loc[:, ['title', 'action', 'body', 'comments', 'comments_count', 'id']]
    new_df['created_at'] = pd.to_datetime(base_df['created_at'])
    new_df['updated_at'] = pd.to_datetime(base_df['updated_at'])
    new_df.set_index('id', inplace=True)
    return new_df


def add_phrase_counts(df):
    df['cf_related'] = df.body.str.contains('cf|cloud foundry|pcf|bosh|diego', case=False)
    df['bds_related'] = df.body.str.contains('gpdb|bds|big data suite|greenplum|hawq|phd|hdb|hadoop|gemfire', case=False)
    df['spring_related'] = df.body.str.contains('spring|xd|spring boot', case=False)
    df['ds_related'] = df.body.str.contains('data science|machine learning', case=False)
    return df


def daily_counts(df):
    df['total_messages'] = 1
    grp = df.groupby(df.created_at.dt.date)[['cf_related',
                                             'bds_related',
                                             'spring_related',
                                             'ds_related',
                                             'total_messages']].sum()
    with_date_index = grp.set_index(pd.DatetimeIndex(grp.index))
    with_date_index = with_date_index.resample('1D', how='sum')
    with_date_index.fillna(0, inplace=True)
    return with_date_index
