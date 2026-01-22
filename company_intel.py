import json
import time
import random
import requests
import os
import datetime
from bs4 import BeautifulSoup
from groq import Groq
from fake_useragent import UserAgent
 
# ==========================================
# 🟢 1. CONFIGURATION
# ==========================================
 
TARGET_COMPANIES = [
    
]
 
FINAL_OUTPUT_FILE = "company_intel/Final_Company_Data_by_simple_approach.json"
RAW_DEBUG_FILE = "raw_search_logs_by_simple_approach.txt"
# Ensure output directory exists
os.makedirs(os.path.dirname(FINAL_OUTPUT_FILE), exist_ok=True)

 
# # 🔑 API Key (Replace with yours if not in environment)
# GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
 
# # Initialize AI Client
# client = Groq(api_key=GROQ_API_KEY)

# --- 🔑 DYNAMIC KEY LOADER ---
def get_api_keys(prefix):
    keys = []
    i = 1
    while True:
        key = os.environ.get(f"{prefix}_{i}")
        if key:
            keys.append(key)
            i += 1
        else:
            break
    # Fallback: if without Number API key
    if not keys and os.environ.get(prefix):
        keys.append(os.environ.get(prefix))
    return keys

# Load all Groq Keys
GROQ_KEYS = get_api_keys("GROQ_API_KEY")


 
# ==========================================
# 🟢 2. HELPER FUNCTIONS
# ==========================================
 
def save_raw_log(company, query, raw_text):
    """Saves the raw text to a file so you can check it later."""
    with open(RAW_DEBUG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*50}\n🏢 {company} | 🔍 {query}\n{'-'*20}\n{raw_text}\n{'='*50}\n")
 
def save_json(data):
    """Saves the clean data to the JSON file."""
    with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
 
# ==========================================
# 🟢 3. SEARCH ENGINE (DuckDuckGo)
# ==========================================
 
def search_ddg(query, time_filter=None):
    """
    Searches DuckDuckGo.
    - query: The text to search.
    - time_filter: 'y' to get only results from the Past Year.
    """
    url = "https://html.duckduckgo.com/html/"
    ua = UserAgent()
   
    # Headers make us look like a real browser coming from Google
    headers = {
        "User-Agent": ua.random,
        "Referer": "https://www.google.com/",
        "Accept-Language": "en-US,en;q=0.9"
    }
   
    payload = {"q": query}
    if time_filter:
        payload["df"] = time_filter  # This activates "Past Year" filter
       
    try:
        print(f"      📡 Searching: '{query}'...")
        response = requests.post(url, data=payload, headers=headers, timeout=20)
       
        if response.status_code == 200:
            # Check for Blocks
            if "captcha" in response.text.lower() or "too many requests" in response.text.lower():
                return "BLOCK"
 
            soup = BeautifulSoup(response.text, "html.parser")
            results = soup.find_all("div", class_="result__body", limit=10) # Get top 10 results
           
            combined_text = ""
            for res in results:
                title = res.find("a", class_="result__a").get_text(strip=True)
                snippet = res.find("a", class_="result__snippet").get_text(strip=True)
                combined_text += f"Source: {title}\nSnippet: {snippet}\n{'-'*10}\n"
           
            return combined_text if combined_text.strip() else None
 
        elif response.status_code in [429, 403]:
            return "BLOCK"
           
        return None
 
    except Exception as e:
        print(f"      ⚠️ Network Error: {e}")
        return None
 
# ==========================================
# 🟢 4. AI ANALYST (Groq)
# ==========================================
def analyze_with_groq(company_name, raw_data):
    """
    Sends the gathered data to Groq to extract the single best answer.
    Uses API Key Rotation to ensure reliability.
    """
    # Define the system prompt with strict decision-making rules
    system_prompt = (
        "You are a Senior Financial Data Analyst. Your job is to determine the single most accurate "
        "Revenue and Employee count for a company based on search snippets.\n\n"
        
        "RULES FOR DECISION MAKING:\n"
        "1. **Revenue:**\n"
        "   - PRIORITY 1: If you see an INR (₹) figure from official sources (Tracxn, Zaubacorp, News) for FY24/25, USE IT. Convert to a clean string (e.g., '₹275 Cr').\n"
        "   - PRIORITY 2: If no INR figure exists, use the most credible USD figure (e.g., from RocketReach or Press Release). \n"
        "   - IGNORE: 'Growjo' or 'ZoomInfo' if they look like automated estimates (e.g., revenue per employee calculations).\n"
        "2. **Employees:**\n"
        "   - Trust 'RocketReach' or 'LinkedIn' snippets the most.\n"
        "   - Prefer exact numbers (e.g., 402) over ranges (e.g., 200-500).\n"
        "3. **Output Format:**\n"
        "   - Return ONLY a simple JSON object. No lists, no sources, no explanations.\n"
        "   - Keys must be exactly: 'Annual Revenue' and 'Total Employee Count'.\n\n"
        "FORMAT EXAMPLE:\n"
        "{\n"
        '  "Annual Revenue": "$3 million",\n'
        '  "Total Employee Count": 31\n'
        "}"
    )
    
    user_content = f"Target Company: {company_name}\n\nSearch Snippets:\n{raw_data}"

    # 🔄 ROTATION LOGIC: Iterate through all available API keys
    for index, api_key in enumerate(GROQ_KEYS):
        try:
            # Initialize the client with the current key in the loop
            client = Groq(api_key=api_key)
            
            completion = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                temperature=0,
                response_format={"type": "json_object"}
            )
            return json.loads(completion.choices[0].message.content)

        except Exception as e:
            print(f"      ⚠️ Groq Key {index+1} Failed: {e}")
            
            # Check if there are more keys available to try
            if index < len(GROQ_KEYS) - 1:
                print("      🔄 Switching to next API Key...")
                time.sleep(5) # Short pause before the next attempt
            else:
                # All keys have failed
                print("      ❌ All Groq API Keys failed.")
                return {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}

# ==========================================
# 🟢 5. MAIN LOGIC LOOP
# ==========================================
 
def main():
    # 1. Load existing data (Resume capability)
    final_data = {}
    if os.path.exists(FINAL_OUTPUT_FILE):
        try:
            with open(FINAL_OUTPUT_FILE, "r", encoding="utf-8") as f:
                final_data = json.load(f)
        except:
            pass
 
    # 2. Calculate dynamic previous year (e.g., 2025)
    current_year = datetime.datetime.now().year
    prev_year = current_year - 1
 
    print(f"🚀 Starting Extraction for {len(TARGET_COMPANIES)} Companies...")
    print(f"📅 Target Revenue Year: {prev_year}\n")
 
    for i, company in enumerate(TARGET_COMPANIES):
       
        # Skip if already done
        if company in final_data:
            print(f"⏭️  Skipping {company} (Already Done)")
            continue
 
        print(f"[{i+1}/{len(TARGET_COMPANIES)}] 🏢 Processing: {company}")
       
        # 🟢 DEFINING THE 2 SPECIFIC QUERIES
        # Q1: Employee focus (RocketReach)
        # Q2: Revenue focus (Annual + Year)
       
        queries = [
            # 1. Employee Query (RocketReach)
            {
                "text": f"site:rocketreach.co {{{company}}} employees size",
                "filter": None
            },
            # 2. Revenue Query (Time Filtered)
            {
                "text": f"{{{company}}} revenue annual {prev_year}",
                "filter": "y" # Past Year Filter
            }
        ]
 
        full_raw_data = ""
 
        # 🟢 RUNNING BOTH QUERIES ONE BY ONE
        for q in queries:
            query_text = q["text"]
            time_filter = q["filter"]
           
            snippet_text = None
           
            # Retry Loop (If blocked)
            for attempt in range(3):
                snippet_text = search_ddg(query_text, time_filter)
               
                if snippet_text == "BLOCK":
                    wait = random.uniform(30, 60)
                    print(f"      🛑 Blocked! Sleeping {wait:.1f}s...")
                    time.sleep(wait)
                    continue
               
                if snippet_text: break
                time.sleep(2) # Short wait between retries
           
            if snippet_text:
                # Add result to our data pile
                full_raw_data += f"\nQUERY: {query_text}\n{snippet_text}\n"
                save_raw_log(company, query_text, snippet_text)
            else:
                print(f"      🔸 No data found for query.")
 
            # ⏳ DELAY BETWEEN QUERIES (Safe Time)
            delay = random.uniform(5, 8)
            print(f"      ⏳ Waiting {delay:.1f}s...")
            time.sleep(delay)
 
        # 🟢 FINAL ANALYSIS
        if full_raw_data.strip():
            print(f"      🧠 Analyzing with Groq...")
            result = analyze_with_groq(company, full_raw_data)
            print(f"      ✅ RESULT: {json.dumps(result)}")
           
            # Save Data
            final_data[company] = result
            save_json(final_data)
        else:
            print(f"      ❌ NO DATA extracted.")
            final_data[company] = {"Annual Revenue": "Not Found", "Total Employee Count": "Not Found"}
            save_json(final_data)
 
        cooldown = random.uniform(10, 15)
        print(f"[SLEEP] Cooling down for {cooldown:.1f}s before next company...\n")
        time.sleep(cooldown)
 
    print("\n🎉 All Done! Check Final_Company_Data.json")

def enrich_companies_from_list(company_list):
    global TARGET_COMPANIES
    TARGET_COMPANIES = list(set(company_list))
    main()




