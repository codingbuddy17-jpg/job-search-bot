import csv
import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Configuration ---
SHEET_ID = "1-RhzHDWvh2nctnjNzHTaRZ-7A5G5fKZA4OPtGjLzki8"
LOCATIONS = ["Hyderabad", "Chennai", "Bangalore"]
RESULTS_WANTED = 20
HOURS_OLD = 72

# List of search configurations: (Search Term, Sheet Name)
SEARCH_CONFIGS = [
    {"term": "Medical coding", "sheet_name": "Medical Coding"},
    {"term": "CDI Clinical Documentation", "sheet_name": "CDI_Clinical_Doc"} 
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

def fetch_jobs(search_term):
    all_jobs = pd.DataFrame()
    
    for location in LOCATIONS:
        print(f"Fetching jobs for '{search_term}' in '{location}'...")
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "glassdoor", "naukri"],
                search_term=search_term,
                location=location,
                results_wanted=RESULTS_WANTED,
                hours_old=HOURS_OLD, 
                country_indeed='India', 
                country_glassdoor='India',
            )
            print(f"Found {len(jobs)} jobs in {location}")
            all_jobs = pd.concat([all_jobs, jobs], ignore_index=True)
        except Exception as e:
            print(f"Error fetching jobs for {location}: {e}")
            
    print(f"Total jobs found for '{search_term}': {len(all_jobs)}")
    return all_jobs

def update_sheet(jobs_df, sheet_name):
    if jobs_df.empty:
        print(f"No jobs found for {sheet_name}.")
        return

    sheet = connect_to_sheet(sheet_name)
    
    # Get existing data to prevent duplicates
    existing_data = sheet.get_all_records()
    existing_links = set(row['job_url'] for row in existing_data if 'job_url' in row)
    
    # Clean up dataframe
    jobs_df = jobs_df.fillna('')
    jobs_df = jobs_df.astype(str)
    
    new_jobs = []
    for _, row in jobs_df.iterrows():
        if row['job_url'] not in existing_links:
            job_data = row.tolist()
            new_jobs.append(job_data)
            existing_links.add(row['job_url'])
    
    if new_jobs:
        if not existing_data and sheet.row_count > 0:
             headers = jobs_df.columns.tolist()
             sheet.insert_row(headers, 1)
        
        sheet.append_rows(new_jobs)
        print(f"Added {len(new_jobs)} new jobs to sheet '{sheet_name}'.")
    else:
        print(f"No NEW jobs found for '{sheet_name}'.")

if __name__ == "__main__":
    for config in SEARCH_CONFIGS:
        try:
            print(f"--- Processing: {config['term']} ---")
            jobs = fetch_jobs(config['term'])
            update_sheet(jobs, config['sheet_name'])
        except Exception as e:
            print(f"An error occurred processing {config['term']}: {e}")
    print("Done!")

