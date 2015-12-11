import json

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
stream_response = """{"streams": [{"custom_stream": false,
                           "default": false,
                           "id": 234234,
                           "last_interacted_at": 1449487471,
                           "name": "Company Stream",
                           "new_updates": false}]}"""
page_response = json.dumps({'messages': [python_msg]})
first_page_response = page_response[:-1] + ', "messages_next_page":2}'
second_page_response = page_response[:-1] + ', "messages_next_page":null}'