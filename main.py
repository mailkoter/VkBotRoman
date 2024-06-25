import vk_api
import psycopg2
from datetime import datetime
from telegram import Bot
from time import sleep
vk_access_token = 'vk1.a.rE-fU4XnMuVXitULN96Qkyw_WGXxUOhhp5deRcRZLo4nSrOCJUFxbN-YTwHWqotxpOD7UxM8mUhdBBxXyodvgGkgcDasl3ThEZVQ7sUNBbPWTLTPU7UZeDKIUf6gPuiSZsjqQ3duy_8BKA9xbEvdJsbu5tQphZhE7ihReNxB91RqN-cyS8xY9kdqEiAKbtVxuwKpRqOJJbH67MsJSikeXA'
telegram_token = '7485405025:AAEGRX3B5k9c5lUjf4N191izTU1lOhRkYik'
channel_groups = {
    'Group1': {'channels': ['russian_bali'], 'telegram_chat_id': ''},
    'Group2': {'channels': ['job_gelendzhik_kabardinka'], 'telegram_chat_id': ''}
}
stop_words = ['спам', 'реклама', 'неинтересно']
key_words = ['работа', 'вакансия', 'требуется']
vk_session = vk_api.VkApi(token=vk_access_token)
vk = vk_session.get_api()
telegram_bot = Bot(token=telegram_token)
conn = psycopg2.connect(
    dbname='social_monitor',
    user='monadmin',
    password='bigdata',
    host='66.151.32.243',
    port='5432'
)
cur = conn.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    group_name VARCHAR(255),
    post_id VARCHAR(255) UNIQUE,
    datetime TIMESTAMP,
    post_link TEXT,
    text TEXT,
    sender_link TEXT,
    image_link TEXT
)
''')
conn.commit()
def get_last_posts(group_id, count=5):
    try:
        if group_id.isdigit():
            owner_id = f'-{group_id}'
            response = vk.wall.get(owner_id=owner_id, count=count)
        else:
            domain = group_id
            response = vk.wall.get(domain=domain, count=count)
        posts = response['items']
        return posts
    except vk_api.exceptions.ApiError as e:
        print(f"Произошла ошибка: {e}")
        return []
def format_datetime(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
def contains_stop_words(text, stop_words):
    return any(word in text.lower() for word in stop_words)
def contains_key_words(text, key_words):
    return any(word in text.lower() for word in key_words)
def send_telegram_message(chat_id, message):
    telegram_bot.send_message(chat_id=chat_id, text=message)
def process_channel_groups(channel_groups):
    for group_name, data in channel_groups.items():
        channels = data['channels']
        telegram_chat_id = data['telegram_chat_id']       
        for channel in channels:
            last_posts = get_last_posts(channel)
            for post in last_posts:
                post_id = f"{post['owner_id']}_{post['id']}"               
                cur.execute("SELECT 1 FROM posts WHERE post_id = %s", (post_id,))
                if cur.fetchone():
                    continue
                text = post.get('text', '')
                if contains_stop_words(text, stop_words):
                    continue
                if not contains_key_words(text, key_words):
                    continue
                post_data = {
                    'datetime': format_datetime(post['date']) if 'date' in post else 'N/A',
                    'group_name': group_name,
                    'post_link': f"https://vk.com/wall{post['owner_id']}_{post['id']}" if 'owner_id' in post and 'id' in post else 'N/A',
                    'text': text,
                    'sender_link': f"https://vk.com/id{post['from_id']}" if 'from_id' in post else 'N/A',
                    'image_link': ''
                }
                if 'attachments' in post:
                    for attachment in post['attachments']:
                        if attachment['type'] == 'photo':
                            sizes = attachment['photo']['sizes']
                            largest_photo = max(sizes, key=lambda size: size['width'])
                            post_data['image_link'] = largest_photo['url']
                            break
                cur.execute('''
                    INSERT INTO posts (group_name, post_id, datetime, post_link, text, sender_link, image_link)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (
                    post_data['group_name'],
                    post_id,
                    post_data['datetime'],
                    post_data['post_link'],
                    post_data['text'],
                    post_data['sender_link'],
                    post_data['image_link']
                ))
                conn.commit()
                message = f"Новое сообщение в группе {group_name}:\\n\\n{post_data['text']}\\n\\nСсылка: {post_data['post_link']}"
                send_telegram_message(telegram_chat_id, message)
if __name__ == "__main__":
    while True:
        process_channel_groups(channel_groups)
        sleep(60)