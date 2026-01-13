import csv
import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Configuration ---
SHEET_ID = "1-RhzHDWvh2nctnjNzHTaRZ-7A5G5fKZA4OPtGjLzki8"  # From user
SEARCH_TERM = "Medical coding"
LOCATIONS = ["Hyderabad", "Chennai", "Bangalore"] 
RESULTS_WANTED = 20
HOURS_OLD = 72

def connect_to_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_file = os.path.join(os.path.dirname(__file__), "google_credentials.json")
    
    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"Credentials file not found at {creds_file}")
        
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def fetch_jobs():
    all_jobs = pd.DataFrame()
    
    for location in LOCATIONS:
        print(f"Fetching jobs for '{SEARCH_TERM}' in '{location}'...")
        try:
            # Scrape jobs from multiple sources including Naukri
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "glassdoor", "naukri"],
                search_term=SEARCH_TERM,
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
            
    print(f"Total jobs found: {len(all_jobs)}")
    return all_jobs

def update_sheet(jobs_df):
    if jobs_df.empty:
        print("No jobs found to update.")
        return

    sheet = connect_to_sheets()
    
    # Get existing data to prevent duplicates
    existing_data = sheet.get_all_records()
    existing_links = set(row['job_url'] for row in existing_data if 'job_url' in row)
    
    # Clean up dataframe for Google Sheets (convert NaNs to empty strings)
    jobs_df = jobs_df.fillna('')
    
    new_jobs = []
    for _, row in jobs_df.iterrows():
        if row['job_url'] not in existing_links:
            # Format row as list for GSpread
            # We explicitly select columns we want to ensure order, or just dump all
            # Let's align with the columns usually returned by jobspy
            job_data = row.tolist()
            new_jobs.append(job_data)
            existing_links.add(row['job_url']) # Add to set to prevent internal duplicates in this batch
    
    if new_jobs:
        # If sheet is empty (no headers), add headers first
        if not existing_data and sheet.row_count > 0:
             headers = jobs_df.columns.tolist()
             sheet.insert_row(headers, 1)
        
        # Append new rows
        sheet.append_rows(new_jobs)
        print(f"Added {len(new_jobs)} new jobs to the sheet.")
    else:
        print("No NEW jobs found (all duplicates).")

if __name__ == "__main__":
    try:
        jobs = fetch_jobs()
        update_sheet(jobs)
        print("Done!")
    except Exception as e:
        print(f"An error occurred: {e}")

