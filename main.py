import csv
import re
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
    {"term": "Clinical Documentation Integrity", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "CDIP", "sheet_name": "CDI_Clinical_Doc"},
    {"term": "CDI Coding", "sheet_name": "CDI_Clinical_Doc"},
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

# --- Relevance Filter ---
# Positive keywords: at least ONE must appear in the job title or description
POSITIVE_KEYWORDS = [
    "medical coding", "medical coder",
    "inpatient coding", "inpatient coder",
    "ip-drg", "ip drg", "drg coding", "drg coder",
    "clinical coding", "clinical coder",
    "clinical documentation", "cdi", "cdip",
    "hcc", "risk adjustment",
    "icd-10", "icd 10", "icd10",
    "cpc", "ccs",
    "ahima", "aapc",
    "health information management", "him ",
    "revenue cycle",
    "outpatient coding", "outpatient coder",
    "diagnosis coding", "procedure coding",
    "coding specialist", "coding analyst",
    "coding auditor", "coding educator",
    "chart review", "medical record",
]

# Negative keywords: if ANY appears in the job TITLE, the job is excluded
NEGATIVE_TITLE_KEYWORDS = [
    "software engineer", "software developer", "software development",
    "web developer", "frontend", "front-end", "backend", "back-end",
    "full stack", "fullstack", "full-stack",
    "devops", "data engineer", "data scientist",
    "machine learning", "ml engineer", "ai engineer",
    "python developer", "java developer", "javascript",
    "react", "angular", "node.js", "vue.js",
    "cloud engineer", "sre", "site reliability",
    "qa engineer", "test engineer", "automation engineer",
    "embedded", "firmware", "hardware engineer",
    "mechanical engineer", "civil engineer", "electrical engineer",
    "network engineer", "system administrator", "sysadmin",
    "cybersecurity", "information security",
    "database administrator", "dba",
    "ui/ux", "ux designer", "ui designer",
    "product manager", "scrum master",
    "blockchain", "crypto",
]

def filter_relevant_jobs(jobs_df, search_term):
    """
    Filters a DataFrame of jobs to keep only healthcare coding/CDI relevant results.
    Uses positive keyword matching + negative keyword exclusion on title.
    """
    if jobs_df.empty:
        return jobs_df
    
    original_count = len(jobs_df)
    kept_indices = []
    
    for idx, row in jobs_df.iterrows():
        title = str(row.get('title', '')).lower()
        description = str(row.get('description', '')).lower()
        combined_text = title + " " + description
        
        # Step 1: Check if title contains any negative (engineering) keyword
        is_engineering = False
        for neg_kw in NEGATIVE_TITLE_KEYWORDS:
            if neg_kw in title:
                is_engineering = True
                break
        
        if is_engineering:
            continue
        
        # Step 2: Check if title or description contains at least one positive keyword
        has_positive = False
        for pos_kw in POSITIVE_KEYWORDS:
            if pos_kw in combined_text:
                has_positive = True
                break
        
        # Also accept if the original search term itself is in the title
        if not has_positive:
            if search_term.lower() in title:
                has_positive = True
        
        if has_positive:
            kept_indices.append(idx)
    
    filtered_df = jobs_df.loc[kept_indices].reset_index(drop=True)
    removed_count = original_count - len(filtered_df)
    
    if removed_count > 0:
        print(f"   🔍 Relevance filter: Kept {len(filtered_df)}/{original_count} jobs ({removed_count} irrelevant removed)")
    else:
        print(f"   🔍 Relevance filter: All {original_count} jobs are relevant")
    
    return filtered_df


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

    print(f"Total jobs found for '{search_term}' (before filter): {len(all_jobs)}")
    
    # Apply relevance filter to remove engineering/irrelevant jobs
    all_jobs = filter_relevant_jobs(all_jobs, search_term)
    
    print(f"Total relevant jobs for '{search_term}': {len(all_jobs)}")
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
