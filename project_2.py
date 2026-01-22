import streamlit as st
import pandas as pd
import plotly.express as px
from serpapi import GoogleSearch
from datetime import datetime, timedelta
import logging
import requests
import time
import os
from company_intel import enrich_companies_from_list
from deep_company_research import run_deep_research_for_companies
from company_cleaner import clean_all_unstructured_reports
from upload_to_sheets import upload_structured_folder_to_sheets
from lead_scoring import run_ai_strategic_layer
import json



COMPANY_INTEL_FILE = os.path.join(
    "company_intel",
    "Final_Company_Data_by_simple_approach.json"
)

@st.cache_data
def load_company_intel():
    if os.path.exists(COMPANY_INTEL_FILE):
        with open(COMPANY_INTEL_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}



if 'show_leads' not in st.session_state:
    st.session_state.show_leads = False
# ================= LOGGER =================
def get_logger():
    logger = logging.getLogger("job_scrapper")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    try:
        handler = logging.FileHandler("app.log", encoding="utf-8")
    except:
        handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

logger = get_logger()
logger.info("Application started")

# ================= CONFIGURATION =================
SERPAPI_KEYS = [
    os.getenv("SERPAPI_KEY_1"),
    os.getenv("SERPAPI_KEY_2")
]

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

# ================= GL & HL MAPPING =================
COUNTRY_GL_HL_MAP = {
    "United States": ("us", "en"), "USA": ("us", "en"), "US": ("us", "en"), "America": ("us", "en"),
    "New York": ("us", "en"), "California": ("us", "en"),
    "United Kingdom": ("gb", "en"), "UK": ("gb", "en"), "Britain": ("gb", "en"), "England": ("gb", "en"),
    "London": ("gb", "en"),
    "UAE": ("ae", "en"), "United Arab Emirates": ("ae", "en"), "Dubai": ("ae", "en"), "Abu Dhabi": ("ae", "en"),
    "France": ("fr", "fr"), "Paris": ("fr", "fr"),
    "Germany": ("de", "de"), "Berlin": ("de", "de"), "Deutschland": ("de", "de"),
    "India": ("in", "en"), "Mumbai": ("in", "en"), "Delhi": ("in", "en"),
    "Bangalore": ("in", "en"), "Bengaluru": ("in", "en"), "Hyderabad": ("in", "en"),
    "Canada": ("ca", "en"), "Toronto": ("ca", "en"), "Vancouver": ("ca", "en"), "Montreal": ("ca", "en"),
    "Australia": ("au", "en"), "Sydney": ("au", "en"), "Melbourne": ("au", "en"),
    "Singapore": ("sg", "en"),
    "Netherlands": ("nl", "nl"), "Amsterdam": ("nl", "nl"), "Holland": ("nl", "nl"),
    "Switzerland": ("ch", "de"), "Zurich": ("ch", "de"), "Geneva": ("ch", "de"),
    "Sweden": ("se", "sv"), "Stockholm": ("se", "sv"),
    "Ireland": ("ie", "en"), "Dublin": ("ie", "en"),
    "Israel": ("il", "en"), "Tel Aviv": ("il", "en"),
    "Saudi Arabia": ("sa", "ar"), "Riyadh": ("sa", "ar"), "KSA": ("sa", "ar"),
    "Qatar": ("qa", "ar"), "Doha": ("qa", "ar"),
    "Oman": ("om", "ar"), "Muscat": ("om", "ar"),
    "Bahrain": ("bh", "ar"),
}

st.set_page_config(page_title="Intelligence Lead Dashboard", layout="wide")

# ================= CUSTOM CSS =================
st.markdown("""
<style>
/* App background */
.stApp { background-color: #f8f9fa; }

/* Sidebar background */
section[data-testid="stSidebar"] { 
    background-color: #1e1e1e !important; 
}

/* Sidebar labels & captions stay white */
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] span {
    color: white !important;
}

/* 🔥 SELECTED VALUE inside dropdowns (Timeline, Job Nature) */
section[data-testid="stSidebar"] div[data-baseweb="select"] > div {
    color: black !important;
    background-color: white !important;
}

/* Dropdown menu items */
section[data-testid="stSidebar"] ul li {
    color: black !important;
}

/* Slider value text */
section[data-testid="stSidebar"] .stSlider span {
    color: white !important;
}

/* Buttons */
.stButton>button {
    background-color: #10a37f;
    color: white;
    border-radius: 8px;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)


# ================= COUNTRY HELPERS =================


def get_high_score_companies(company_df, threshold=10):
    return (
        company_df[company_df["Lead Score"] >= threshold]["Company"]
        .dropna()
        .unique()
        .tolist()
    )

def detect_search_country(user_input):
    if not user_input:
        return "United States"
    text = user_input.lower()
    for c in COUNTRY_GL_HL_MAP:
        if c.lower() in text:
            return c
    return "United States"

def extract_country(location_str):
    if not location_str:
        return "Unknown"
    loc = location_str.lower()
    if "remote" in loc or "anywhere" in loc:
        return "Remote"
    parts = [p.strip() for p in location_str.split(",")]
    last = parts[-1].upper()
    if last in ["USA", "UK", "UAE", "KSA"]:
        return {"USA": "United States", "UK": "United Kingdom", "UAE": "UAE", "KSA": "Saudi Arabia"}.get(last, last.title())
    return last.title() if len(parts) > 1 else "Unknown"

# ================= SERPAPI (Google Jobs) - WITH FILTERS =================
def get_leads_serpapi(q, loc, date_f, type_f, limit):
    detected_country = detect_search_country(loc)
    gl, hl = COUNTRY_GL_HL_MAP.get(detected_country, ("us", "en"))

    all_jobs, seen = [], set()
    api_index, token = 0, None

    # Handle Chips (Filters)
    chips = []
    if date_f != "All":
        chips.append(f"date_posted:{date_f}")
    if type_f != "All":
        chips.append(f"employment_type:{type_f}")
    chips_q = ",".join(chips) if chips else None

    while len(all_jobs) < limit and api_index < len(SERPAPI_KEYS):
        params = {
            "engine": "google_jobs",
            "q": q,
            "location": loc,
            "gl": gl,
            "hl": hl,
            "chips": chips_q,
            "api_key": SERPAPI_KEYS[api_index],
            "no_cache": False  # 🔥 Parameter added here to force fresh results
        }
        
        if token:
            params["next_page_token"] = token
            # Note: docs suggest no_cache and next_page_token can be used, 
            # but usually, the first request is where no_cache matters most.

        try:
            search = GoogleSearch(params)
            res = search.get_dict()
            jobs = res.get("jobs_results", [])

            if not jobs:
                api_index += 1
                token = None
                continue

            for j in jobs:
                key = f"{j.get('title')}-{j.get('company_name')}-{j.get('location')}"
                if key in seen:
                    continue
                seen.add(key)

                all_jobs.append({
                    "Job Title": j.get("title"),
                    "Company": j.get("company_name"),
                    "Location": j.get("location"),
                    "Country": extract_country(j.get("location")),
                    "Type": j.get("job_type", "Not Specified"),
                    "Market Source": "Google Jobs (SerpAPI)",
                    "Posted": j.get("detected_extensions", {}).get("posted_at", "Recent"),
                    "Apply Link": j.get("apply_options", [{}])[0].get("link"),
                    "Job Description": j.get("description", "Not available (basic view)"),
                    "Company URL": j.get("via", "Not available")
                })

                if len(all_jobs) >= limit:
                    break

            # Handle Pagination
            token = res.get("serpapi_pagination", {}).get("next_page_token")
            if not token:
                api_index += 1
        except Exception as e:
            logger.error(f"SerpAPI error: {e}")
            api_index += 1

    return all_jobs
# ================= LINKEDIN (RapidAPI) - WITH JOB TYPE FILTER =================
import requests

def get_leads_linkedin(q, loc, date_f, type_f, limit):
    url = "https://jobs-api14.p.rapidapi.com/v2/linkedin/search"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jobs-api14.p.rapidapi.com"
    }

    # Map date_f to LinkedIn datePosted values (approximate; API supports day/week/month)
    date_map = {
        "All": None,
        "today": "day",
        "3days": "week",  # Closest match
        "week": "week",
        "month": "month"
    }
    date_posted = date_map.get(date_f)

    # Map type_f to LinkedIn employmentTypes
    type_map = {
        "FULLTIME": "fulltime",
        "CONTRACTOR": "contractor",
        "INTERN": "intern",
        "All": "fulltime;contractor;parttime;intern;temporary"
    }
    employment_types = type_map.get(type_f, type_map["All"])

    # Base params
    params = {
        "query": q,
        "location": loc or "Worldwide",
        "workplaceTypes": "remote;hybrid;onSite",
        "employmentTypes": employment_types,
        "experienceLevels": "intern;entry;associate;midSenior;director",  # Always include broad experience levels
        "limit": min(limit, 50)  # API limit per page; adjust as needed
    }

    if date_posted:
        params["datePosted"] = date_posted

    all_jobs, seen = [], set()
    next_token = None

    while len(all_jobs) < limit:
        if next_token:
            params["token"] = next_token

        try:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
            jobs = data.get("data", [])

            if not jobs:
                break

            for j in jobs:
                key = f"{j.get('title')}-{j.get('companyName')}-{j.get('location')}"
                if key in seen:
                    continue
                seen.add(key)

                all_jobs.append({
                    "Job Title": j.get("title"),
                    "Company": j.get("companyName", "Unknown"),
                    "Location": j.get("location"),
                    "Country": extract_country(j.get("location")),
                    "Type": j.get("employmentType", type_f if type_f != "All" else "Not Specified"),
                    "Market Source": "LinkedIn (RapidAPI)",
                    "Posted": j.get("postedTimeAgo", j.get("datePosted", "Unknown")),
                    "Apply Link": j.get("applyUrl", f"https://www.linkedin.com/jobs/view/{j.get('id')}"),  # Fallback to LinkedIn job URL
                    "Job Description": j.get("description", "Not available (use /job-details endpoint for full desc)"),
                    "Company URL": j.get("companyUrl", "Not available")  # Add this line
                })

                if len(all_jobs) >= limit:
                    break

            next_token = data.get("meta", {}).get("nextToken")
            if not next_token:
                break

            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"LinkedIn API error: {e}")
            break

    return all_jobs[:limit]

# ================= JSEARCH - WITH JOB TYPE & DATE FILTER (via query) =================
def get_leads_jsearch(q, loc, date_f, type_f, limit):
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    search_url = "https://jsearch.p.rapidapi.com/search"
    details_url = "https://jsearch.p.rapidapi.com/job-details"

    # Enhance query with filters
    query_parts = [q, f"in {loc}"]

    if type_f != "All":
        type_map = {
            "FULLTIME": "full time",
            "CONTRACTOR": "contract",
            "INTERN": "internship"
        }
        query_parts.append(type_map.get(type_f, ""))

    if date_f != "All":
        date_map = {
            "today": "today",
            "3days": "past 3 days",
            "week": "past week",
            "month": "past month"
        }
        query_parts.append(date_map.get(date_f, ""))

    query = " ".join([p for p in query_parts if p])

    search_params = {
        "query": query,
        "page": "1",
        "num_pages": str((limit // 10) + 2)
    }

    try:
        response = requests.get(search_url, headers=headers, params=search_params)
        data = response.json()
        if data.get("status") != "OK":
            return []
        jobs = data.get("data", [])[:limit * 2]  # Get extra to filter
    except Exception as e:
        logger.error(f"JSearch search error: {e}")
        return []

    results = []
    seen = set()

    for job in jobs:
        job_id = job.get("job_id")
        key = f"{job.get('job_title')}-{job.get('employer_name')}-{job.get('job_location')}"
        if key in seen:
            continue
        seen.add(key)

        city = job.get("job_city") or ""
        state = job.get("job_state") or ""
        country = job.get("job_country") or ""
        location_parts = [p for p in [city, state, country] if p]
        location = ", ".join(location_parts) if location_parts else "Remote/Unknown"

        description = "Not fetched"
        try:
            details_resp = requests.get(details_url, headers=headers, params={"job_id": job_id})
            details_data = details_resp.json()
            if details_data.get("status") == "OK" and details_data.get("data"):
                detail = details_data["data"][0]
                description = detail.get("job_description", "Not available")
                time.sleep(0.2)
        except:
            description = "Error fetching details"

        results.append({
            "Job Title": job.get("job_title"),
            "Company": job.get("employer_name"),
            "Location": job.get("job_location", location),
            "Country": extract_country(job.get("job_location", "")),
            "Type": job.get("job_employment_type", "Not Specified"),
            "Market Source": "JSearch (Enhanced Google Jobs)",
            "Posted": job.get("job_posted_at", "Recent"),
            "Apply Link": job.get("job_apply_link"),
            "Job Description": description,
            "Company URL": job.get("employer_website", "Not available")
        })

        if len(results) >= limit:
            break

    return results
import re
def normalize_revenue(rev):
    if not rev or not isinstance(rev, str):
        return None

    r = rev.lower().replace(",", "").strip()

    try:
        # INR Crores
        if "₹" in r and "cr" in r:
            return float(r.split("₹")[1].split("cr")[0].strip()) * 0.12

        # $14.37B or $14.37b
        if re.search(r"\$\d+(\.\d+)?b", r):
            return float(r.replace("$", "").replace("b", "")) * 1000

        # $670.4 million
        if "million" in r:
            return float(r.split()[0])

        # $55.3 billion
        if "billion" in r:
            return float(r.split()[0]) * 1000

    except:
        return None

    return None


def normalize_employee_count(val):
    """
    Converts employee ranges or strings to a numeric midpoint.
    """
    if val is None:
        return None

    if isinstance(val, int):
        return val

    if not isinstance(val, str):
        return None

    v = val.lower().replace(",", "").strip()

    # "5000+"
    if v.endswith("+"):
        return int(v[:-1])

    # "201-500"
    if "-" in v:
        try:
            low, high = v.split("-")
            return (int(low) + int(high)) // 2
        except:
            return None

    # "1000"
    try:
        return int(v)
    except:
        return None




def revenue_match_score(val, user_choice):
    if val is None or user_choice == "Any":
        return 0

    ranges = {
        "1M/yr - 50M/yr": (1, 50),
        "50M/yr - 1B/yr": (50, 1000),
        "1B+/yr": (1000, float("inf"))
    }

    low, high = ranges[user_choice]

    if low <= val <= high:
        return 5
    if low * 0.8 <= val <= high * 1.2:
        return 2.5
    return 0


def employee_match_score(val, user_choice):
    val = normalize_employee_count(val)

    if val is None or user_choice == "Any":
        return 0

    ranges = {
        "10 - 100": (10, 100),
        "100 - 999": (100, 999),
        "1000 - 5000": (1000, 5000),
        "5000+": (5000, float("inf"))
    }

    low, high = ranges[user_choice]

    if low <= val <= high:
        return 5
    if low * 0.8 <= val <= high * 1.2:
        return 2.5
    return 0


def final_lead_score(row, intel, revenue_q, size_q):
    score = 0

    # 1️⃣ Vacancy score
    score += row["Open_Roles"] * 5

    # 2️⃣ Intent score (keep existing)
    intent_bonus = {
        "CRM Migration": 25,
        "System Integration": 20,
        "Salesforce Optimization": 15,
        "Ongoing Salesforce Support": 10,
        "Salesforce Expansion": 10
    }
    score += intent_bonus.get(row["Detected Need"], 0)

    # 3️⃣ Company intelligence
    company = row["Company"]
    if company in intel:
        rev = normalize_revenue(intel[company].get("Annual Revenue"))
        emp = intel[company].get("Total Employee Count")

        score += revenue_match_score(rev, revenue_q)
        score += employee_match_score(emp, size_q)

    return min(score, 100)


# ================= LEAD INTELLIGENCE HELPERS =================
def detect_need(text):
    t = text.lower()

    if any(k in t for k in ["migration", "migrate", "transition", "move from"]):
        return "CRM Migration"

    if any(k in t for k in ["optimize", "optimization", "performance", "improve"]):
        return "Salesforce Optimization"

    if any(k in t for k in ["integration", "api", "erp", "sap", "oracle"]):
        return "System Integration"

    if any(k in t for k in ["admin", "support", "managed services"]):
        return "Ongoing Salesforce Support"

    return "Salesforce Expansion"
def final_lead_score_no_intent(row, intel, revenue_q, size_q):
    score = 0
    breakdown = []

    # 1️⃣ Vacancy scoring PER ROLE
    roles = row.get("Job_Roles", [])
    for role in roles:
        score += 5
        breakdown.append(f"+5 (Role: {role})")

    # 2️⃣ Revenue & Size match
    company = row["Company"]
    if company in intel:
        rev_raw = intel[company].get("Annual Revenue")
        emp = intel[company].get("Total Employee Count")

        rev = normalize_revenue(rev_raw)

        rev_score = revenue_match_score(rev, revenue_q)
        emp_score = employee_match_score(emp, size_q)

        if rev_score > 0:
            breakdown.append(f"+{rev_score} (Revenue: {rev_raw})")

        if emp_score > 0:
            breakdown.append(f"+{emp_score} (Employees: {emp})")

        score += rev_score + emp_score

    return min(score, 100), " | ".join(breakdown)




def calculate_lead_score(row):
    score = 0

    score += min(row["Open_Roles"] * 20, 60)

    intent_bonus = {
        "CRM Migration": 25,
        "System Integration": 20,
        "Salesforce Optimization": 15,
        "Ongoing Salesforce Support": 10,
        "Salesforce Expansion": 10
    }

    score += intent_bonus.get(row["Detected Need"], 0)
    return min(score, 100)

def update_structured_json_with_scores(company_df, structured_dir="structured_data"):
    structured_dir = Path(structured_dir)

    # ✅ SAFETY CHECK
    if not structured_dir.exists():
        print(f"⚠️ structured directory not found: {structured_dir}")
        print("⚠️ Skipping structured JSON score update")
        return

    score_map = {
        row["Company"].strip().lower(): {
            "lead_score": float(row["Lead Score"]),
            "rank_breakout": row["Rank (Breakout)"]
        }
        for _, row in company_df.iterrows()
    }

    updated = 0

    for file in structured_dir.glob("*_Structured.json"):
        try:
            with file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            meta_name = (
                data.get("meta", {})
                .get("company_name", "")
                .strip()
                .lower()
            )

            if not meta_name or meta_name not in score_map:
                continue

            data["lead_scoring"] = score_map[meta_name]

            with file.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            updated += 1

        except Exception as e:
            print(f"❌ Failed updating {file.name}: {e}")

    print(f"✅ Lead scoring injected into {updated} structured JSON files")





def load_uploaded_companies(uploaded_file):
    if uploaded_file is None:
        return []

    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        df.columns = [c.strip().lower() for c in df.columns]

        if "company" not in df.columns:
            st.error("❌ Uploaded file must contain a 'Company' column")
            return []

        companies = (
            df["company"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        return companies

    except Exception as e:
        st.error(f"❌ Failed to read uploaded file: {e}")
        return []


# ================= SIDEBAR =================
with st.sidebar:
    st.title("⚙️ Search Logic")
    provider = st.radio(
        "Job Source",
        ["SerpAPI (Google Jobs)", "LinkedIn (RapidAPI)", "JSearch (Enhanced Google Jobs)"],
        index=0
    )
    date_val = st.selectbox("📅 Timeline (Freshness)", ["All", "today", "3days", "week", "month"])
    type_val = st.selectbox("💼 Job Nature", ["All", "FULLTIME", "CONTRACTOR", "INTERN"])
    target = st.slider("🎯 Target Leads Count", 10, 100, 50)
    st.divider()
    st.caption("✅ Filters now work across ALL sources!")
# ================= MAIN UI =================
st.title("🚀 Market Intelligence Dashboard")
st.markdown("Automated Talent Acquisition & Market Analysis Tool")

st.markdown("## 📥 Alternative Input: Upload Company File")

uploaded_file = st.file_uploader(
    "Upload CSV or Excel file (must contain a 'Company' column)",
    type=["csv", "xlsx"],
    help="Uploaded companies will be used to generate deep company intelligence and uploaded to Google Sheets."
)

if uploaded_file:
    st.info(
        "ℹ️ The uploaded file will be used to generate company intelligence "
        "and automatically uploaded to Google Sheets."
    )

    if st.button("🚀 Generate Company Intelligence from Uploaded File"):
        companies = load_uploaded_companies(uploaded_file)

        if companies:
            progress_text = st.empty()
            progress_bar = st.progress(0)

            # STEP 1: Deep Research
            progress_text.text("🔍 Running deep company research...")
            run_deep_research_for_companies(companies)
            progress_bar.progress(50)

            # STEP 2: Cleaning
            progress_text.text("🧹 Cleaning & structuring company intelligence...")
            clean_all_unstructured_reports(
                unstructured_dir="Unstructured_data",
                structured_dir="structured_data"
            )
            progress_bar.progress(80)

            # STEP 3: Upload
            progress_text.text("📤 Uploading structured data to Google Sheets...")
            upload_structured_folder_to_sheets()
            progress_bar.progress(100)

            st.success("✅ Uploaded file processed and synced to Google Sheets!")



# 1. Initialize Session State for Data Storage
if 'df' not in st.session_state:
    st.session_state.df = None
if 'company_df' not in st.session_state:
    st.session_state.company_df = None
if 'show_leads' not in st.session_state:
    st.session_state.show_leads = False

c1, c2, c3, c4 = st.columns(4)

with c1:
    job_q = st.text_input(
        " Target Job Role",
        placeholder="e.g. Salesforce Developer"
    )

with c2:
    loc_q = st.text_input(
        " Market Location",
        placeholder="e.g. India, Germany, Dubai"
    )

with c3:
    revenue_q = st.selectbox(
        " Company Revenue Range",
        [
            "Any",
            "1M/yr - 50M/yr",
            "50M/yr - 1B/yr",
            "1B+/yr"
        ],
        index=0
    )

with c4:
    company_size_q = st.selectbox(
        " Company Employee Size",
        [
            "Any",
            "10 - 100",
            "100 - 999",
            "1000 - 5000",
            "5000+"
        ],
        index=0
    )

# ================= RUN SEARCH =================
if st.button("Generate Final Report"):
    if not job_q.strip() or not loc_q.strip():
        st.error("Please fill in both Job Role and Location.")
    else:
        with st.spinner(f"Fetching up to {target} jobs via {provider}..."):
            if provider.startswith("SerpAPI"):
                results = get_leads_serpapi(job_q, loc_q, date_val, type_val, target)
            elif provider.startswith("LinkedIn"):
                results = get_leads_linkedin(job_q, loc_q, date_val, type_val, target)
            else:
                results = get_leads_jsearch(job_q, loc_q, date_val, type_val, target)

        if not results:
            st.warning("No jobs found. Try broadening filters or switching source.")
            st.session_state.df = None
        else:
            # Create DataFrames
            df = pd.DataFrame(results)
            df["Company"] = df["Company"].astype(str).replace(
                {"None": "Unknown", "nan": "Unknown", "[]": "Unknown", "{}": "Unknown"}
            )
            
            # Aggregation
            company_df = (
                df.groupby("Company")
                .agg(
                    Job_Roles=("Job Title", lambda x: list(set(x))),
                    Open_Roles=("Job Title", "count"),
                    Locations=("Location", lambda x: ", ".join(set(x))),
                    Countries=("Country", lambda x: ", ".join(set(x))),
                    Job_Types=("Type", lambda x: ", ".join(set(x))),
                    Descriptions=("Job Description", lambda x: " ".join(x.astype(str))),
                )
                .reset_index()
            )
            company_df["Open_Roles"] = company_df["Job_Roles"].apply(len)
            company_df["Detected Need"] = company_df["Descriptions"].apply(detect_need)
            company_df["Why This Lead"] = company_df.apply(
                lambda r: f"Hiring {r['Open_Roles']} role(s) across {r['Countries']}, indicating {r['Detected Need'].lower()}.",
                axis=1
            )
            company_df["Lead Score"] = company_df.apply(calculate_lead_score, axis=1)
            company_df = company_df.sort_values("Lead Score", ascending=False)

            # 🔥 STORE IN SESSION STATE
            st.session_state.df = df
            st.session_state.company_df = company_df
            st.session_state.show_leads = False # Reset filter view on new search

# ================= DISPLAY LOGIC =================
# We check if st.session_state.df has data. If it does, we show it.
if st.session_state.df is not None:
    df = st.session_state.df
    company_df = st.session_state.company_df

    # --- METRICS ---
    m1, m2, m3 = st.columns(3)
    m1.metric("Leads Identified", len(df))
    m2.metric("Unique Locations", df["Location"].nunique())
    m3.metric("Active Source", provider.split("(")[0].strip())

    # --- VISUALS ---
    col1, col2 = st.columns(2)
    with col1:
        top_companies = df["Company"].value_counts().head(10).reset_index()
        top_companies.columns = ["Company", "Open Roles"]
        st.plotly_chart(px.bar(top_companies, x="Company", y="Open Roles", title="🏢 Top Hiring Companies"), use_container_width=True)
    
    with col2:
        top_locations = df["Location"].value_counts().head(10).reset_index()
        top_locations.columns = ["Location", "Job Count"]
        st.plotly_chart(px.bar(top_locations, x="Location", y="Job Count", title="📍 Top Locations"), use_container_width=True)

    # --- MAP ---
    st.markdown("### 🗺️ Global Job Distribution Map")
    country_counts = df.groupby("Country").size().reset_index(name="Job Count")
    country_counts = country_counts[~country_counts["Country"].isin(["Unknown", "Remote"])]
    st.plotly_chart(px.scatter_geo(country_counts, locations="Country", locationmode="country names", size="Job Count", projection="natural earth"), use_container_width=True)

    # --- DETAILED TABLE ---
    st.markdown("### 📋 Detailed Lead Inventory")
    st.dataframe(df[["Job Title", "Company", "Location", "Country", "Type", "Posted", "Apply Link", "Company URL"]], use_container_width=True)

    # --- FILTERING SECTION ---
    st.divider()
    # This button now works because the data is in session_state!
    if st.button("🔍 Start Filtering"):
        st.session_state.show_leads = True

    def get_companies_from_results(company_df):
        return (
            company_df["Company"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )


    if st.session_state.show_leads:
        with st.spinner("🧠 Fetching company revenue & size intelligence..."):
            companies = get_companies_from_results(company_df)
            enrich_companies_from_list(companies)

        with st.spinner("📊 Recalculating lead scores..."):
            intel = load_company_intel()

            scores = company_df.apply(
                lambda r: final_lead_score_no_intent(
                    r, intel, revenue_q, company_size_q
                ),
                axis=1,
                result_type="expand"   # 👈 IMPORTANT
            )

            company_df["Lead Score"] = scores[0].astype(float)
            company_df["Rank (Breakout)"] = scores[1]

            company_df = company_df.sort_values("Lead Score", ascending=False)
            update_structured_json_with_scores(company_df)
        st.success("✅ Intelligence enrichment & ranking complete")

        st.dataframe(
            company_df[
                ["Company", "Countries", "Open_Roles", "Detected Need", "Why This Lead", "Lead Score", "Rank (Breakout)"]
            ],
            use_container_width=True
        )

        if st.session_state.show_leads:
            qualified_companies = get_high_score_companies(company_df, threshold=10)

            st.markdown("### 🧠 Deep Company Intelligence")
            st.write(f"Qualified Companies: {len(qualified_companies)}")

            if st.button("🚀 Generate Deep Company Reports"):
                progress_text = st.empty()
                progress_bar = st.progress(0)

                # -------------------------------
                # STEP 1: Deep Research
                # -------------------------------
                progress_text.text("🔍 Running deep company research...")
                progress_bar.progress(10)

                run_deep_research_for_companies(qualified_companies)

                progress_bar.progress(50)

                # -------------------------------
                # STEP 2: Cleaning
                # -------------------------------
                progress_text.text("🧹 Cleaning & structuring company intelligence...")
                clean_all_unstructured_reports(
                    unstructured_dir="Unstructured_data",
                    structured_dir="structured_data"
                )

                progress_bar.progress(80)

                update_structured_json_with_scores(company_df)

                # -------------------------------
                # STEP 3: Upload to Sheets
                # -------------------------------
                progress_text.text("📤 Uploading structured data to Google Sheets...")
                upload_structured_folder_to_sheets()

                progress_bar.progress(70)
                progress_text.text("🧠 Generating AI Strategic Summaries...")
                run_ai_strategic_layer()

                progress_bar.progress(100)
                progress_text.text("✅ Pipeline completed successfully!")

                st.success("🎉 Deep research → structured data → Google Sheets upload completed!")

                with st.spinner("🧠 Generating AI Strategic Summaries..."):
                    run_ai_strategic_layer()

                st.success("🎯 AI Strategic summaries added to Google Sheets")





    # --- DOWNLOAD ---
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label="📥 Download Full Report (CSV)", data=csv, file_name=f"Report.csv", mime="text/csv")


