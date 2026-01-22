
import os
import json
import time
from datetime import datetime
from langchain_community.tools.tavily_search import TavilySearchResults # Updated import standard
# If using the older package, use: from langchain_tavily import TavilySearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================

# --- 🔑 DYNAMIC KEY LOADER FOR TAVILY ---
def get_api_keys(prefix):
    """
    Finds all env variables starting with PREFIX_ (e.g., TAVILY_API_KEY_1)
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

# Load all Tavily keys
TAVILY_KEYS = get_api_keys("TAVILY_API_KEY")

if not TAVILY_KEYS:
    print("❌ CRITICAL: No TAVILY_API_KEYs found in .env file!")


# Domains focused on SMEs, Startups, and Professional Updates
TARGET_DOMAINS = [
    "linkedin.com", "crunchbase.com", "clutch.co", "goodfirms.co",
    "g2.com", "yourstory.com", "inc42.com", "entrackr.com",
    "medium.com", "prlog.org", "businesswire.com", "finance.yahoo.com"
]

# ==========================================
# ROTATION WRAPPER
# ==========================================

def run_tavily_with_retry(query, max_results=5, domains=None):
    """
    Executes Tavily search with automatic key rotation.
    """
    tool_args = {
        "max_results": max_results,
        "search_depth": "advanced",
        "include_answer": True,
        "include_raw_content": True
    }
    if domains:
        tool_args["include_domains"] = domains

    for index, api_key in enumerate(TAVILY_KEYS):
        try:
            # Set Key
            os.environ["TAVILY_API_KEY"] = api_key
            
            # Re-initialize tool
            tool = TavilySearchResults(**tool_args)
            
            return tool.invoke(query)

        except Exception as e:
            print(f"      ⚠️ Tavily Key {index+1} Failed: {e}")
            if index < len(TAVILY_KEYS) - 1:
                print("      🔄 Switching to next Tavily Key...")
                time.sleep(10)
            else:
                print("      ❌ All Tavily Keys exhausted.")
                return []
    return []

# ==========================================
# DATA FETCHING
# ==========================================

def fetch_financial_data(company_name):
    """
    Fetches core financial details, funding, and registration info.
    Uses a 'general' search depth for broad coverage.
    """
    print(f"[*] Fetching financial & registration data for: {company_name}...")
    
    query = f"""
    Find detailed financial information for the company '{company_name}':
    1. Latest Annual Revenue .
    2. Total Funding raised, Investors, and Valuation.
    3. Employee Count and Company Size.
    4. Corporate Registration (CIN, Headquarters).
    5. Decision makers details(CEO,CTO etc..)
    
    """
    
    # Use Wrapper instead of direct call
    return run_tavily_with_retry(query, max_results=20)
    
def fetch_company_news(company_name):
    """
    Fetches recent news, PR, and hiring updates from specific professional domains.
    Includes logic to filter out irrelevant search noise.
    """
    print(f"[*] Fetching verified news & updates...")
    
    query = f"""
    Recent announcements for company "{company_name}":
    1. Partnerships, Awards, or New Client wins.
    2. Product launches or "We are hiring" posts.
    3. Investment news or leadership changes.
    """

    # Use Wrapper with domains
    raw_results = run_tavily_with_retry(query, max_results=10, domains=TARGET_DOMAINS)
    
    verified_sources = []
    
    try:
        # --- FILTERING LOGIC ---
        # Ensure the company name actually appears in the title or snippet
        short_name = company_name.split()[0].lower() # e.g., "AnavClouds"
        
        for item in raw_results:
            title = item.get('content', '').lower() # Tavily sometimes puts title in content, check structure
            snippet = item.get('content', '').lower()
            url = item.get('url', '')

            # Validation: Short name must exist in the content snippet to be relevant
            if short_name in snippet or short_name in title:
                verified_sources.append({
                    "title": url.split('/')[-1].replace('-', ' ').title(), # Fallback title from URL
                    "url": url,
                    "snippet": item.get('content', '')[:300] + "...",
                    "source_domain": url.split('/')[2]
                })
                
        print(f"    -> {len(raw_results)} raw results found.")
        print(f"    -> {len(verified_sources)} relevant articles kept after filtering.")
        
        return verified_sources

    except Exception as e:
        print(f"[!] Error fetching news: {str(e)}")
        return []

def generate_report(company_name):
    """
    Main orchestrator function.
    Aggregates data and saves to JSON.
    """
    print(f"\n{'='*60}")
    print(f"STARTING ANALYSIS: {company_name.upper()}")
    print(f"{'='*60}\n")
    
    # 1. Gather Data
    financial_data = fetch_financial_data(company_name)
    news_data = fetch_company_news(company_name)
    
    # 2. Structure Data for JSON
    report_payload = {
        "meta": {
            "company_name": company_name,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "Success"
        },
        "financial_intelligence": financial_data,
        "market_updates": news_data
    }
    
    # 3. Save to JSON
    # Create folder if it doesn't exist
    output_dir = "Unstructured_data"
    os.makedirs(output_dir, exist_ok=True)

    # Build file path
    filename = os.path.join(
        output_dir,
        f"{company_name.replace(' ', '_')}_Report.json"
    )

    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(report_payload, f, indent=4, ensure_ascii=False)
        print(f"\n[+] Data successfully exported to: {filename}")
    except IOError as e:
        print(f"[!] File Save Error: {e}")

# ==========================================
# ENTRY POINT
# ==========================================


def run_deep_research_for_companies(company_list):
    for company in company_list:
        try:
            generate_report(company)
            time.sleep(5)  # polite delay
        except Exception as e:
            print(f"❌ Failed research for {company}: {e}")
