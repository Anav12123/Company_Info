import time
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from groq import Groq
import gspread
from google.oauth2.service_account import Credentials
import json

# --- 1. SETUP & CONFIGURATION ---

# Load API Keys from .env file
load_dotenv()

# --- 🔄 CHANGE 1: DYNAMIC KEY LOADING ---
def get_api_keys(prefix):
    """
    Finds all env variables starting with PREFIX_ (e.g., GROQ_API_KEY_1)
    Returns a list of keys.
    """
    keys = []
    i = 1
    while True:
        key = os.getenv(f"{prefix}_{i}")
        if key:
            keys.append(key)
            i += 1
        else:
            break
    # Fallback: Check for plain key
    if not keys and os.getenv(prefix):
        keys.append(os.getenv(prefix))
    return keys

# Load all Groq keys
GROQ_KEYS = get_api_keys("GROQ_API_KEY")

if not GROQ_KEYS:
    # Fallback logic
    if os.getenv("GROQ_API_KEY"):
        GROQ_KEYS = [os.getenv("GROQ_API_KEY")]
    else:
        raise ValueError("[CRITICAL ERROR] No GROQ_API_KEYs found in .env file")

# Import Sheet Name
try:
    from upload_to_sheets import GOOGLE_SHEET_NAME
except ImportError:
    GOOGLE_SHEET_NAME = "Company_data" 
    print(f"[WARNING] Could not import sheet name. Using default: {GOOGLE_SHEET_NAME}")

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
    # --- Data Extraction ---
    company = row_data.get("company_profile_company_name", row_data.get("Company", "Unknown"))
    industry = row_data.get("company_profile_industry", "Unknown")
    revenue = row_data.get("financials_estimated_revenue_usd", "Unknown")
    employees = row_data.get("financials_employees", "Unknown")
    
    # User Inputs
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

    # --- 🔄 CHANGE 2: ROTATION LOGIC ---
    for index, api_key in enumerate(GROQ_KEYS):
        try:
            # Initialize client with the current key inside loop
            client = Groq(api_key=api_key)

            chat_completion = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile", 
                temperature=0.5,
            )
            return chat_completion.choices[0].message.content.strip()
    
        except Exception as e:
            print(f"⚠️ Groq Key {index+1} Failed for {company}: {e}")
            
            # Switch to next key if available
            if index < len(GROQ_KEYS) - 1:
                print("🔄 Switching to next API Key...")
                time.sleep(1)
            else:
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
        existing_summary = str(row.get(output_col, "")).strip()

        is_failed_previous_run = any(x in existing_summary.lower() for x in ["analysis failed", "error", "failed to update"])
       
        if len(existing_summary) > 10 and not is_failed_previous_run:
            print(f"[SKIP] {company_name}: Already analyzed.")
            continue
        
        # 2. Skip if no Score (Cannot explain what doesn't exist)
        score_val = (
            row.get("lead_scoring_lead_score")
            or row.get("Lead Score")
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
