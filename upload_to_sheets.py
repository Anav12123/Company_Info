# ============================================================
# UPLOAD STRUCTURED COMPANY INTELLIGENCE JSONs TO GOOGLE SHEETS
# ============================================================

import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

STRUCTURED_DATA_DIR = BASE_DIR / "structured_data"


# ============================================================
# CONFIGURATION
# ============================================================

# STRUCTURED_DATA_DIR = "structured_data"   # Folder with cleaned JSONs
GOOGLE_SHEET_NAME = "Company_data"
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")


# ============================================================
# JSON FLATTENER
# ============================================================


MAX_CELL_CHARS = 49000  # keep buffer below 50k

def truncate_cell(value):
    if isinstance(value, str) and len(value) > MAX_CELL_CHARS:
        return value[:MAX_CELL_CHARS] + "… [TRUNCATED]"
    return value


def flatten_json(data, parent_key="", sep="_"):
    """
    Recursively flattens nested JSON.
    Dict -> parent_child
    List -> comma separated string
    """
    items = {}

    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep))

        elif isinstance(value, list):
            # Convert list items to readable string
            cleaned_list = []
            for v in value:
                if isinstance(v, dict):
                    cleaned_list.append(
                        "; ".join(f"{k}:{str(val)}" for k, val in v.items())
                    )
                else:
                    cleaned_list.append(str(v))

            items[new_key] = " | ".join(cleaned_list)

        else:
            items[new_key] = value

    return items

# ============================================================
# LOAD ALL STRUCTURED JSON FILES
# ============================================================
from pathlib import Path

def load_structured_data(folder_path):
    folder_path = Path(folder_path)
    rows = []

    if not folder_path.exists():
        print(f"⚠️ Structured data directory not found: {folder_path}")
        return pd.DataFrame()

    files = list(folder_path.glob("*.json"))

    print(f"📂 Found {len(files)} structured JSON files")

    for file in files:
        try:
            with file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            flat = flatten_json(data)
            rows.append(flat)

        except Exception as e:
            print(f"❌ Failed to process {file.name}: {e}")

    return pd.DataFrame(rows)


# ============================================================
# UPLOAD TO GOOGLE SHEETS
# ============================================================
def upload_to_google_sheets(df, sheet_name, creds_file):
    creds_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if not creds_path:
        raise ValueError("❌ GOOGLE_SERVICE_ACCOUNT_JSON not set")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1
    print("📄 Google Sheet opened")

    # Existing data
    existing = sheet.get_all_records()
    existing_df = pd.DataFrame(existing)

    df = df.fillna("").applymap(truncate_cell)

    key_col = "company_profile_company_name"

    if existing_df.empty:
        # First upload
        sheet.update([df.columns.tolist()] + df.values.tolist())
        print("✅ First upload completed")
        return

    existing_df = existing_df.fillna("")

    # Merge (UPSERT)
    merged = pd.concat([existing_df, df]) \
        .drop_duplicates(subset=[key_col], keep="last")

    sheet.clear()
    sheet.update(
        [merged.columns.tolist()] + merged.values.tolist()
    )

    print("✅ Sheet updated (UPSERT complete)")


# ============================================================
# MAIN RUNNER
# ============================================================

if __name__ == "__main__":
    print("🚀 Starting upload process...")

    df = load_structured_data(STRUCTURED_DATA_DIR)

    if df.empty:
        print("⚠️ No data found to upload")
    else:
        upload_to_google_sheets(
            df=df,
            sheet_name=GOOGLE_SHEET_NAME,
            creds_file=SERVICE_ACCOUNT_FILE
        )

    print("🎉 Process completed")

def upload_structured_folder_to_sheets():
    df = load_structured_data(STRUCTURED_DATA_DIR)

    if df.empty:
        print("⚠️ No structured data found to upload")
        return

    upload_to_google_sheets(
        df=df,
        sheet_name=GOOGLE_SHEET_NAME,
        creds_file=SERVICE_ACCOUNT_FILE
    )

