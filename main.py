import csv
import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime, timedelta
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram_scraper import scrape_telegram_jobs
import asyncio

# --- Configuration ---
SHEET_ID = "1-RhzHDWvh2nctnjNzHTaRZ-7A5G5fKZA4OPtGjLzki8"
LOCATIONS = ["Hyderabad", "Chennai", "Bangalore"]
RESULTS_WANTED = 20
HOURS_OLD = 72 

# Configuration for Telegram
TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH")
TELEGRAM_SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING")

# List of search configurations: (Search Term, Sheet Name)
SEARCH_CONFIGS = [
    # Medical Coding
    {"term": "Medical coding", "sheet_name": "Medical Coding"},
    {"term": "Inpatient coding", "sheet_name": "Medical Coding"},
    {"term": "Inpatient coder", "sheet_name": "Medical Coding"},
    {"term": "IP-DRG coding", "sheet_name": "Medical Coding"},
    {"term": "IP-DRG coder", "sheet_name": "Medical Coding"},
    {"term": "Clinical coder", "sheet_name": "Medical Coding"},
    {"term": "Clinical coding", "sheet_name": "Medical Coding"},
    
    # CDI
    {"term": "CDI Clinical Documentation", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "Clinical Documentation Improvement", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "CDI specialist", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "CDI associate", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "CDI coder", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "Clinical Documentation Integrity", "sheet_name": "CDI_Clinical_Doc"}
]

def connect_to_sheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_file = os.path.join(os.path.dirname(__file__), "google_credentials.json")
    
    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"Credentials file not found at {creds_file}")
        
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
        
    return worksheet

def remove_expired_jobs(sheet_name):
    """
    Removes jobs from the sheet that are older than HOURS_OLD.
    """
    print(f"🧹 Clearing expired jobs from '{sheet_name}' (> {HOURS_OLD} hours old)...")
    try:
        sheet = connect_to_sheet(sheet_name)
        data = sheet.get_all_records()
        
        if not data:
            print("   -> Sheet is empty, nothing to clean.")
            return

        headers = sheet.row_values(1)
        if 'date_posted' not in headers:
            print("   -> Cannot clean: 'date_posted' column missing.")
            return

        # We need to preserve the header row
        valid_rows = [headers]
        deleted_count = 0
        
        today = datetime.now().date()
        
        for row in data:
            date_str = str(row.get('date_posted', ''))
            is_expired = False
            
            # Attempt to parse date
            # JobSpy usually returns YYYY-MM-DD
            try:
                job_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                age_days = (today - job_date).days
                if age_days * 24 > HOURS_OLD:
                    is_expired = True
            except ValueError:
                # If date format is weird (e.g. "Just now" or empty), we KEEP it to be safe
                pass
            
            if not is_expired:
                valid_rows.append(list(row.values()))
            else:
                deleted_count += 1
        
        if deleted_count > 0:
            print(f"   -> Removing {deleted_count} expired rows...")
            sheet.clear()
            sheet.update(valid_rows)
            print("   -> Cleanup complete.")
        else:
            print("   -> No expired jobs found.")

    except Exception as e:
        print(f"   ❌ Error during cleanup: {e}")

def fetch_jobs(search_term):
    all_jobs = pd.DataFrame()
    
    # Define sites to scrape. We split them to isolate errors (especially LinkedIn).
    # site_groups = [["indeed", "glassdoor", "naukri"], ["linkedin"]]
    # Actually, let's just do them in one go but with better logging? 
    # JobSpy suggests doing them together is faster/better, but for debugging, splitting is useful.
    # Let's try splitting LinkedIn out.
    
    site_configs = [
        {"sites": ["indeed", "glassdoor", "naukri"], "name": "Standard"},
        {"sites": ["linkedin"], "name": "LinkedIn"}
    ]

    for location in LOCATIONS:
        print(f"Fetching jobs for '{search_term}' in '{location}'...")
        
        for config in site_configs:
            sites = config["sites"]
            group_name = config["name"]
            
            try:
                print(f"   -> Scraping {group_name} ({', '.join(sites)})...")
                jobs = scrape_jobs(
                    site_name=sites,
                    search_term=search_term,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=HOURS_OLD, 
                    country_indeed='India', 
                    country_glassdoor='India',
                )
                
                count = len(jobs)
                print(f"   -> Found {count} jobs from {group_name}")
                
                if not jobs.empty:
                    jobs['date_posted'] = jobs['date_posted'].astype(str)
                    all_jobs = pd.concat([all_jobs, jobs], ignore_index=True)
                    
            except Exception as e:
                print(f"   ❌ Error scraping {group_name} for {location}: {e}")

    # 2. Telegram Scraping (Only run if credentials exist)
    if TELEGRAM_API_ID and TELEGRAM_SESSION_STRING:
        print(f"Fetching Telegram jobs for '{search_term}'...")
        try:
            tg_jobs = asyncio.run(scrape_telegram_jobs(
                TELEGRAM_API_ID, 
                TELEGRAM_API_HASH, 
                TELEGRAM_SESSION_STRING, 
                [], 
                [search_term]
            ))
            if not tg_jobs.empty:
                print(f"   -> Found {len(tg_jobs)} Telegram jobs")
                all_jobs = pd.concat([all_jobs, tg_jobs], ignore_index=True)
            else:
                print("   -> No Telegram jobs found.")
        except Exception as e:
            print(f"   ❌ Error scraping Telegram: {e}")

    print(f"Total jobs found for '{search_term}': {len(all_jobs)}")
    return all_jobs

def update_sheet(jobs_df, sheet_name):
    # 1. Cleanup old jobs first
    remove_expired_jobs(sheet_name)

    if jobs_df.empty:
        print(f"No NEW jobs found for '{sheet_name}'.")
        return

    sheet = connect_to_sheet(sheet_name)
    
    # Get existing data to prevent duplicates
    existing_data = sheet.get_all_records()
    existing_links = set(str(row['job_url']) for row in existing_data if 'job_url' in row)
    
    print(f"Checking for duplicates against {len(existing_links)} existing links...")

    # Clean up dataframe
    jobs_df = jobs_df.fillna('')
    jobs_df = jobs_df.astype(str)
    
    new_jobs = []
    for _, row in jobs_df.iterrows():
        # Ensure we have a URL to check against
        url = str(row.get('job_url', ''))
        if url and url not in existing_links:
            job_data = row.tolist()
            new_jobs.append(job_data)
            existing_links.add(url)
    
    if new_jobs:
        # If sheet is empty/new, add headers
        if not existing_data and sheet.row_count > 0:
             # Check if header row exists, if not add it
             current_headers = sheet.row_values(1)
             if not current_headers:
                 headers = jobs_df.columns.tolist()
                 sheet.insert_row(headers, 1)
        
        sheet.append_rows(new_jobs)
        print(f"✅ Added {len(new_jobs)} new jobs to sheet '{sheet_name}'.")
    else:
        print(f"No NEW jobs found for '{sheet_name}' (all duplicates).")

if __name__ == "__main__":
    print("--- Starting Job Bot ---")
    for config in SEARCH_CONFIGS:
        term = config["term"]
        sheet_name = config["sheet_name"]
        
        print(f"\nProcessing: {term} -> {sheet_name}")
        jobs = fetch_jobs(term)
        update_sheet(jobs, sheet_name)
    print("\n--- Bot Finished ---")
