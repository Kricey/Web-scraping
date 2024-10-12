import requests
import json

BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAANJPvgEAAAAAiuD32jKrY6OjpkC5qihH6j5FNys%3DQsIo7g7Lo6zPR6v15KxkPyD0qhnxnmPKZZf97Qoeg9PQciDZRY'

# endpoint
url = "https://api.twitter.com/2/tweets/search/recent?query=conversation_id%3A1833728804579111268%20lang%3Aen&max_results=10&sort_order=recency&tweet.fields=attachments,author_id,conversation_id,created_at,in_reply_to_user_id,public_metrics&expansions=author_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id&user.fields=description,id,name,public_metrics"

headers = {
    'Authorization': f'Bearer {BEARER_TOKEN}',
}
# GET
response = requests.get(url, headers=headers)

# check request
if response.status_code == 200:
    data = response.json()

    file_path = '/Users/apple/Desktop/1833851081849213088-page1.json'
    
    # save response to the file_path
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"Saved to {file_path}")
else:
    print(f"Error code: {response.status_code}")
    print(response.text)