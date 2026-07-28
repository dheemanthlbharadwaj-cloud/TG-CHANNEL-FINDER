"""
Run this ONCE on your own computer (not GitHub) to log in and get a
string session. It will ask for your phone number + OTP code.

pip install telethon
python generate_session.py
"""
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID = 0        # <-- fill in from my.telegram.org
API_HASH = ""      # <-- fill in from my.telegram.org

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print("\nYour session string (save as GitHub secret TG_SESSION_STRING):\n")
    print(client.session.save())
