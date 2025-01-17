from datetime import datetime
import requests
from .settings import RESTRICT_MOVING_TIME, MOVE_TIMES


def can_be_moved():
    # time restrictions
    if not RESTRICT_MOVING_TIME:
        return True
    r = requests.get("http://worldtimeapi.org/api/timezone/America/New_York")
    r_json = r.json()
    local_time_str = r_json['datetime']
    local_time = datetime.strptime(local_time_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    week_day = r_json['day_of_week']
    if week_day == 6 or week_day == 7:
        return True
    if local_time.hour >= MOVE_TIMES['start'] and local_time.hour <= MOVE_TIMES['stop']:
        return True
    return False


def is_night_time():
    """Check if the current time is between processing_start_time and processing_end_time."""
    from datetime import datetime, time

    now = datetime.now().time()
    start_time = time(MOVE_TIMES['start'], 0)
    end_time = time(MOVE_TIMES['stop'], 0)

    # Check if the current time is within the range
    if start_time <= now or now < end_time:
        return True
    return False


# Post a message to a channel
def post_message(channel, text):
    url = "https://slack.com/api/chat.postMessage"
    payload = {
        "channel": channel,
        "text": text,
    }
    response = requests.post(url, json=payload, headers=HEADERS)
    data = response.json()
    if data.get("ok"):
        print("Message posted successfully!")
        return data["ts"]  # Return the timestamp of the message
    else:
        print("Failed to post message:", data.get("error"))
        return None


# Fetch replies from a thread in Slack
def fetch_thread_replies(channel, thread_ts):
    """
    usage:
    BOT_TOKEN = "xoxb-your-bot-token"
    HEADERS = {
        "Authorization": f"Bearer {BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    # Specify the channel ID where the bot will post
    channel_id = "C12345678"

    # Post a new message
    thread_ts = post_message(channel_id, "Hello! This is a test message.")

    if thread_ts:
        # Simulate fetching and responding to replies
        print("Waiting for replies...")

        import time
        time.sleep(10)  # Simulate waiting for replies

        replies = fetch_thread_replies(channel_id, thread_ts)
        for reply in replies:
            user = reply.get("user")
            text = reply.get("text")
            ts = reply.get("ts")
            if user and text:
                print(f"Reply from {user}: {text}")
    """
    url = "https://slack.com/api/conversations.replies"
    params = {
        "channel": channel,
        "ts": thread_ts,
    }
    response = requests.get(url, params=params, headers=HEADERS)
    data = response.json()
    if data.get("ok"):
        return data["messages"]  # List of messages in the thread
    else:
        print("Failed to fetch replies:", data.get("error"))
        return []