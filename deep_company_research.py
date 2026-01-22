import os
import json
import time
from datetime import datetime
from langchain_community.tools.tavily_search import TavilySearchResults # Updated import standard
# If using the older package, use: from langchain_tavily import TavilySearch
 
# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = os.getenv("tav_api") # Ideally, load this from .env in production
os.environ["TAVILY_API_KEY"] = API_KEY

 
# Domains focused on SMEs, Startups, and Professional Updates
TARGET_DOMAINS = [
    "linkedin.com", "crunchbase.com", "clutch.co", "goodfirms.co",
    "g2.com", "yourstory.com", "inc42.com", "entrackr.com",
    "medium.com", "prlog.org", "businesswire.com", "finance.yahoo.com"
]
 
def fetch_financial_data(company_name):
    """
    Fetches core financial details, funding, and registration info.
    Uses a 'general' search depth for broad coverage.
    """
    print(f"[*] Fetching financial & registration data for: {company_name}...")
   
    tool = TavilySearchResults(
        max_results=20,
        search_depth="advanced",
        include_answer=True,
        include_raw_content=True
    )
 
    query = f"""
    Find detailed financial information for the company '{company_name}':
    1. Latest Annual Revenue .
    2. Total Funding raised, Investors, and Valuation.
    3. Employee Count and Company Size.
    4. Corporate Registration (CIN, Headquarters).
    5. Decision makers details(CEO,CTO etc..)
   
    """
   
    try:
        # Note: In newer LangChain versions, invoke expects a string or dict depending on version
        # We pass the query string directly for standard Tavily tools
        results = tool.invoke(query)
       
        # Structure the output for the report
        # Note: TavilySearchResults returns a list of docs, we extract the 'answer' if available internally
        # For simplicity in this script, we treat the list as sources.
        return results
       
    except Exception as e:
        print(f"[!] Error fetching financials: {str(e)}")
        return []
   
def fetch_company_news(company_name):
    """
    Fetches recent news, PR, and hiring updates from specific professional domains.
    Includes logic to filter out irrelevant search noise.
    """
    print(f"[*] Fetching verified news & updates...")
   
    tool = TavilySearchResults(
        max_results=10,
        search_depth="advanced",
        include_answer=True,
        include_raw_content=True,
        include_domains=TARGET_DOMAINS # Restricted to high-quality sources
    )
 
    query = f"""
    Recent announcements for company "{company_name}":
    1. Partnerships, Awards, or New Client wins.
    2. Product launches or "We are hiring" posts.
    3. Investment news or leadership changes.
    """
 
    verified_sources = []
   
    try:
        raw_results = tool.invoke(query)
       
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

 
#     # 4. Executive Summary (Terminal Output)
#     print_terminal_summary(company_name, financial_data, news_data)
 
# def print_terminal_summary(name, finance, news):
#     """
#     Prints a clean, readable summary to the console for quick review.
#     """
#     print("\n" + "="*60)
#     print(f"EXECUTIVE SUMMARY: {name}")
#     print("="*60)
   
#     print("\n[DATA SOURCES FOUND]")
#     print(f" - Financial Records: {len(finance)}")
#     print(f" - Verified News:     {len(news)}")
   
#     if finance:
#         print("\n[TOP FINANCIAL INSIGHT]")
#         # Taking the first result as the primary snippet
#         print(f" > {finance[0].get('content', 'No content')[:250]}...")
   
#     if news:
#         print("\n[LATEST UPDATES]")
#         for article in news[:3]:
#             print(f" - {article['source_domain']}: {article['snippet'][:100]}...")
#             print(f"   Link: {article['url']}")
#     else:
#         print("\n[NEWS] No specific press releases found on target domains.")
       
#     print("\n" + "="*60 + "\n")
 
# ==========================================
# ENTRY POINT
# ==========================================


def run_deep_research_for_companies(company_list):
    for company in company_list:
        try:
            generate_report(company)
            time.sleep(2)  # polite delay
        except Exception as e:
            print(f"❌ Failed research for {company}: {e}")
