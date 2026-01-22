import time
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from groq import Groq
import gspread
from google.oauth2.service_account import Credentials
# --- 1. SETUP & CONFIGURATION ---
import json
# Load API Keys from .env file
load_dotenv()


GROQ_API_KEY = os.getenv("GROQ_API_KEY")
# Initialize Groq Client
client = Groq(api_key=GROQ_API_KEY)
# Import Sheet Name from your existing file
try:
    from upload_to_sheets import GOOGLE_SHEET_NAME
except ImportError:
    GOOGLE_SHEET_NAME = "Company_data" # Default fallback
    print(f"⚠️ Warning: Could not import sheet name. Using default: {GOOGLE_SHEET_NAME}")

# --- 2. GOOGLE SHEETS CONNECTION ---

def connect_to_sheet():
    """
    Connects to Google Sheets using your existing credentials file.
    """
    # Path matches your upload_to_sheets.py configuration
    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    try:
        service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        gc = gspread.authorize(creds)
        return gc
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        print(f"Check if this file exists: {SERVICE_ACCOUNT_FILE}")
        sys.exit(1)

# --- 3. THE AI BRAIN (Strategic Analysis) ---

def generate_smart_summary(row_data):
    """
    Reads the full row data and generates a strategic summary.
    """
    # --- Data Extraction (Matching your flattened headers) ---
    company = row_data.get("company_profile_company_name", row_data.get("Company", "Unknown"))
    industry = row_data.get("company_profile_industry", "Unknown")
    
    # Financials
    revenue = row_data.get("financials_estimated_revenue_usd", "Unknown")
    employees = row_data.get("financials_employees", "Unknown")
    
    # User Inputs (Lead Score)
    score = (
        row_data.get("lead_scoring_lead_score")
        or row_data.get("Lead Score")
        or "N/A"
    )

    breakout = (
        row_data.get("lead_scoring_rank_breakout")
        or row_data.get("Rank (Breakout)")
        or "No details"
    )


    # Critical Signals (News, Tech, Funding)
    # The 'news' column often contains raw text about funding/launches
    news_data = row_data.get("news", "")
    
    # --- Intelligent Prompt ---
    prompt = f"""
    Act as a Strategic Sales Director for a boutique Salesforce/IT Agency.
    Our ideal 'Dream Clients' are **Small-to-Mid Sized Companies** (Agile, Fast Decisions).
    We also target Enterprises, but view them as long-term plays.

    **Lead Profile:**
    - Company: {company} ({industry})
    - Score: {score} / 100
    - Details: {breakout}
    - Data: {employees} Employees | Revenue: {revenue}
    - News Signals: {news_data[:300]}

    **CLASSIFICATION & STRATEGY RULES:**
    1. **Mid-Market (The Sweet Spot):** (Revenue $10M-$1B OR 50-1000 Emp).
       -> LABEL: "🌟 DREAM CLIENT (Mid-Market)"
       -> STRATEGY: Pitch "End-to-End Automation & Scaling". They need speed.
    
    2. **Small Business:** (Revenue < $10M OR < 50 Emp).
       -> LABEL: "🚀 HIGH POTENTIAL (SMB)"
       -> STRATEGY: Pitch "We become your Tech Team". They lack internal resources.

    3. **Enterprise:** (Revenue > $1B OR > 1000 Emp).
       -> LABEL: "🏢 ENTERPRISE (Big Ticket)"
       -> STRATEGY: Pitch "Specialized Staff Augmentation / Niche Consulting". (Note: Longer sales cycle).

    4. **Funded Startup:** (Any size with recent funding news).
       -> LABEL: "🔥 HOT LEAD (Funded)"
       -> STRATEGY: Pitch "Rapid Deployment for Growth".

    **YOUR TASK:**
    Write a 2-sentence summary following this strict format:

    "[LABEL] - Score {score}/15. [Why they fit our agency]. [Killer Pitch]."

    **Examples:**
    - "🌟 DREAM CLIENT (Mid-Market) - Score 12/15. Perfect fit as they are scaling fast ($50M Rev) and need to automate chaos. Pitch our 'Salesforce Growth Package' for immediate impact."
    - "🏢 ENTERPRISE (Big Ticket) - Score 15/15. Massive scale ($1B+) means high reliability needs. Pitch 'Dedicated Support Teams' to assist their internal IT dept."
    """

    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile", 
            temperature=0.5,
        )
        return chat_completion.choices[0].message.content.strip()
    
    except Exception as e:
        print(f"⚠️ AI Error for {company}: {e}")
        return "Analysis Failed"

# --- 4. MAIN EXECUTION ---

def process_sheet_smartly():
    print(f"🔌 Connecting to Google Sheet: '{GOOGLE_SHEET_NAME}'...")
    
    gc = connect_to_sheet()
    
    try:
        # Open the sheet
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1 # First tab
    except Exception as e:
        print(f"❌ Could not open sheet '{GOOGLE_SHEET_NAME}': {e}")
        return

    # Load Data into Pandas
    all_data = worksheet.get_all_records()
    df = pd.DataFrame(all_data)

    if df.empty:
        print("⚠️ Sheet is empty.")
        return

    print(f"🚀 Found {len(df)} companies. Starting Strategic Analysis...")

    output_col = "AI Strategic Summary"
    updates_made = 0

    for index, row in df.iterrows():
        # Identify Company
        company_name = row.get("company_profile_company_name", row.get("Company", "Unknown"))

        # --- SKIP LOGIC ---
        # 1. Skip if already analyzed
        existing_summary = str(row.get(output_col, ""))
        if len(existing_summary) > 10:
            print(f"⏭️  Skipping {company_name}: Already analyzed.")
            continue
        
        # 2. Skip if no Score (Cannot explain what doesn't exist)
        score_val = (
            row.get("lead_scoring_lead_score")
            or row.get("Lead Score")
        )

        breakout_val = (
            row.get("lead_scoring_rank_breakout")
            or row.get("Rank (Breakout)")
        )

        if not score_val:
            print(f"⏭️  Skipping {company_name}: No Lead Score found.")
            continue


        # --- GENERATE & UPDATE ---
        print(f"🧠 Analyzing {company_name}...")
        summary = generate_smart_summary(row)

        try:
            # Find or Create Column
            try:
                col_idx = worksheet.find(output_col).col
            except:
                print(f"➕ Adding new column: {output_col}")
                worksheet.add_cols(1)
                new_col_idx = len(df.columns) + 1
                worksheet.update_cell(1, new_col_idx, output_col)
                col_idx = new_col_idx

            # Update Cell (Row + 2 adjustment)
            worksheet.update_cell(index + 2, col_idx, summary)
            updates_made += 1
            
            # Rate Limit Safety
            time.sleep(1.5)

        except Exception as e:
            print(f"❌ Failed to update {company_name}: {e}")

    print(f"✅ Success! Updated {updates_made} companies with Strategic Summaries.")



def run_ai_strategic_layer():
    """
    Wrapper function for pipeline usage
    """
    process_sheet_smartly()



