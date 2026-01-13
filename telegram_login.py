from telethon.sync import TelegramClient
from telethon.sessions import StringSession

print("--- Telegram Login Setup ---")
api_id = "33188279"
api_hash = "92c42d66d871e0997d4463b16fae15e8"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\nSuccessfully logged in!")
    print("Here is your Session String (copy the entire line below):")
    print(client.session.save())
    print("\nKeep this safe! Add it as TELEGRAM_SESSION_STRING in GitHub Secrets.")
