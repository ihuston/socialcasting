# Socialcasting - API helper for Socialcast
import json

import pandas as pd
import requests
# noinspection PyUnresolvedReferences
from requests.exceptions import HTTPError
import os

API_BASE = os.getenv('SOCIALCAST_API', "http://demo.socialcast.com/api/")
username = os.getenv('SOCIALCAST_USERNAME', 'emily@socialcast.com')
password = os.getenv('SOCIALCAST_PASSWORD', 'demo')


def get(query, params=None):
    req = requests.get(API_BASE + query, params=params, auth=(username, password))
    req.raise_for_status()
    return req.json()


def get_streams():
    return get("streams.json")


def get_company_stream_id():
    result = get("streams.json")
    stream_id = [s for s in result["streams"] if s["name"] == "Company Stream"][0]["id"]
    return stream_id


def get_raw_response(page=1, per_page=500):
    params = {'page': page, 'per_page': per_page}
    result = get("streams/" + str(get_company_stream_id()) + "/messages.json", params)
    return result


def get_messages(page=1, per_page=500):
    response = get_raw_response(page, per_page)
    return response['messages']


def get_all_messages(storage_dir="../data"):
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


def make_dataframe(msg_list):
    base_df = pd.DataFrame(msg_list)
    new_df = base_df.loc[:, ['title', 'action', 'body', 'comments', 'comments_count', 'id']]
    new_df['created_at'] = pd.to_datetime(base_df['created_at'])
    new_df['updated_at'] = pd.to_datetime(base_df['updated_at'])
    new_df.set_index('id', inplace=True)
    return new_df


def add_counts_by_name(df, phrases_by_name):
    for name, phrases in phrases_by_name.items():
        df[name + '_related'] = df.body.str.contains(phrases, case=False)
    return df


def daily_counts(df):
    ndf = pd.DataFrame(df, copy=True)
    message_actions = ['asked a question', 'posted a link', 'gave thanks', 'posted an image',
                       'made a group announcement', 'made an announcement', 'posted an idea',
                       'created a poll', '^$']
    ndf['total_messages'] = 0
    ndf.loc[ndf['action'].str.contains("|".join(message_actions)), 'total_messages'] = 1

    cols_to_include = [c for c in ndf.columns if c.endswith('related')]
    cols_to_include.append('total_messages')

    grp = ndf.groupby(ndf.created_at.dt.date)[cols_to_include].sum()
    with_date_index = grp.set_index(pd.DatetimeIndex(grp.index))
    with_date_index = with_date_index.resample('1D', how='sum')
    with_date_index.fillna(0, inplace=True)
    return with_date_index

