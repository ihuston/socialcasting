# Socialcasting

A simple Python helper for the Socialcast API

**Author: Ian Huston**

This small package provides a simple interface to the [Socialcast API](https://socialcast.github.io/socialcast/).

To specify your API endpoint and your credentials use the SOCIALCAST_API, SOCIALCAST_USERNAME
and SOCIALCAST_PASSWORD environmental variables.

The following high level commands are useful:

* `get_all_messages(storage_dir)`
  Get all the messages from the company stream and store as JSON.
* `transform_dataframe(msg_list)`
  Convert a raw message list into a Pandas DataFrame.
* `flag_topics(df, topics_by_name)`
  Given a dictionary of regex expressions, add flags identifying which contain these topics.
* `daily_counts(df)`
  Make daily counts of the number of messages in each topic.

## Requirements
Main requirements: [Pandas](http://pandas.pydata.org), [Requests](http://python-requests.org)

Test requirements: [Pytest](http://pytest.org), [Responses](https://github.com/getsentry/responses)

As Pandas depends on [NumPy](http://numpy.org), the recommend way to install is using [Conda](http://conda.pydata.org).
You can create a new conda environment and install this package with the following:
```
$ git clone https://github.com/ihuston/socialcasting.git
$ cd socialcasting
$ conda create -n socialcasting python=3 numpy pandas requests
$ source activate socialcasting
$ python setup.py install
```

## Installation
Clone this repo and run `python setup.py install`.

## Testing
Use Pytest or run `python setup.py test`.

## License
MIT License, see LICENSE.txt
