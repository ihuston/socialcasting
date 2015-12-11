import pandas as pd
import json
from pandas.util import testing as pdt

import socialcasting.analysis
from fixtures import python_msg


def test_load_all_messages(tmpdir):
    f = tmpdir.join("messages-0001.json")
    f.write(json.dumps(python_msg))
    result = socialcasting.analysis.load_all_messages(str(tmpdir))
    assert len(result) == 1


def test_base_df():
    msg_list = [python_msg]
    result = socialcasting.analysis.transform_dataframe(msg_list)
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
    df = socialcasting.analysis.transform_dataframe(msg_list)
    phrases_by_name = {'topic_a': 'first|one|uno',
                       'topic_b': 'second|two|duo',
                       'topic_c': 'third|three|tre'}
    result = socialcasting.analysis.flag_topics(df, phrases_by_name)

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
    result = socialcasting.analysis.daily_counts(df)
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