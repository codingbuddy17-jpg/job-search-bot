from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import pandas as pd
import os
import datetime

async def scrape_telegram_jobs(api_id, api_hash, session_string, channels, search_terms):
    """
    Scrapes Telegram channels for messages containing specific keywords.
    """
    print("--- Starting Telegram Scraping ---")
    jobs = []
    
    try:
        async with TelegramClient(StringSession(session_string), api_id, api_hash) as client:
            # 1. Identify relevant channels from user's dialogs
            print("Scanning your channels for relevant ones...")
            target_channels = []
            async for dialog in client.iter_dialogs():
                if dialog.is_channel or dialog.is_group:
                    name = dialog.name.lower()
                    # User requested to scan ALL channels/groups
                    print(f"Adding channel to scan list: {dialog.name}")
                    target_channels.append(dialog)
            
            if not target_channels:
                print("No relevant channels found in your chat list. Please join some Job channels first!")
                return pd.DataFrame()

            # 2. Scrape them
            for entity in target_channels:
                print(f"Scraping channel: {entity.name}...")
                try:
                    # Fetch last 200 messages
                    msg_count = 0
                    async for message in client.iter_messages(entity, limit=200):
                        msg_count += 1
                        if message.text:
                            text = message.text.lower()
                            # Check if any search term is in the message
                            for term in search_terms:
                                if term.lower() in text:
                                    # Found a match!
                                    post_date = message.date.strftime("%Y-%m-%d")
                                    # Construct a link if possible (public channels)
                                    job_link = f"https://t.me/{entity.username}/{message.id}" if hasattr(entity, 'username') and entity.username else "Private Group/Channel"
                                    
                                    job = {
                                        "job_url": job_link,
                                        "title": f"Telegram: {entity.name}", 
                                        "company": "Telegram",
                                        "location": "See Post",
                                        "date_posted": post_date,
                                        "job_type": term, 
                                        "description": message.text[:300] + "..." 
                                    }
                                    jobs.append(job)
                                    break 
                    print(f"  -> Scanned {msg_count} messages in {entity.name}")
                except Exception as e:
                    print(f"Error scraping {entity.name}: {e}")
                    
    except Exception as e:
        print(f"Telegram connection error: {e}")

    print(f"Found {len(jobs)} Telegram jobs.")
    return pd.DataFrame(jobs)
