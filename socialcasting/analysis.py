# Socialcasting analysis

import pandas as pd


def make_dataframe(msg_list):
    """Convert a message list into a Pandas DataFrame"""
    base_df = pd.DataFrame(msg_list)
    new_df = base_df.loc[:, ['title', 'action', 'body', 'comments', 'comments_count', 'id']]
    new_df['created_at'] = pd.to_datetime(base_df['created_at'])
    new_df['updated_at'] = pd.to_datetime(base_df['updated_at'])
    new_df.set_index('id', inplace=True)
    return new_df


def flag_topics(df, phrases_by_name):
    """
    Flag phrases in the messages as specified by a regular expression.

    Args:
        df: DataFrame containing messages as created by make_dataframe().
        phrases_by_name: dict of regex expressions used to identify topics.

    Returns:
         DataFrame with new columns, named after each key in phrases_by_name
         with the suffix '_related' which flag whether the topic is mentioned
         in the message.

    """
    for name, phrases in phrases_by_name.items():
        df[name + '_related'] = df.body.str.contains(phrases, case=False)
    return df


def daily_counts(df):
    """
    Make daily counts of the phrases identified and the total number of messages.
    Days in the range without any messages are included with zero counts.

    Args:
        df: DataFrame with phrases identified by flag_topics

    Returns:
        DataFrame with counts per day of each phrase.
    """
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