# ============================================================
# COMPANY INTELLIGENCE EXTRACTION PIPELINE (INTEGRATED)
# ============================================================

import json
import re
import nltk
from difflib import SequenceMatcher
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


# -------------------------------------------------
# NLTK SETUP (RUN ONCE)
# -------------------------------------------------
try:
    nltk.data.find("tokenizers/punkt")
except LookupError:
    nltk.download("punkt")
    nltk.download("stopwords")
    nltk.download("wordnet")

stop_words = set(stopwords.words("english"))
lemmatizer = WordNetLemmatizer()

# -------------------------------------------------
# CONSTANTS & RULES
# -------------------------------------------------

BAD_DOMAINS = {
    "linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com",
    "datanyze.com", "zoominfo.com", "crunchbase.com", "pitchbook.com",
    "grammarly.com", "medium.com", "g2.com", "clutch.co", "glassdoor.com",
    "goodfirms.co", "wikipedia.org", "youtube.com", "google.com", "gmail.com",
    "yahoo.com", "outlook.com", "github.com", "upwork.com", "freelancer.com",
    "tracxn.com"
}

LEGAL_WORDS = {
    "technologies", "technology", "solutions", "software",
    "systems", "services", "private", "limited", "pvt",
    "ltd", "inc", "corp", "corporation", "llc",
    "group", "consulting", "global", "analytics",
    "ai", "data"
}

def safe_int(value):
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return int(value)
    except:
        return None


# -------------------------------------------------
# UTILITY FUNCTIONS
# -------------------------------------------------

def strip_markdown_and_urls(text):
    """Removes images and link syntax, leaving only display text."""
    if not text:
        return ""
    # Remove images: ![alt](url)
    text = re.sub(r"!\[.*?\]\(.*?\)", " ", text)
    # Convert links: [Text](url) -> Text
    text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
    # Remove raw URLs
    text = re.sub(r"https?:\/\/\S+", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def extract_brand_keyword(company_name: str) -> str:
    clean = re.sub(r"[^a-zA-Z ]", "", company_name.lower())
    words = clean.split()
    core = [w for w in words if w not in LEGAL_WORDS]
    return "".join(core if core else words)

# -------------------------------------------------
# EXTRACTION MODULES
# -------------------------------------------------

def find_closest_company_website(text, company_name):
    if not text or not company_name:
        return None

    brand = extract_brand_keyword(company_name)
    url_pattern = r'\b(?:https?://|www\.)?([a-zA-Z0-9-]+\.(?:com|ai|io|co|net|org))\b'

    candidates = []
    for m in re.finditer(url_pattern, text.lower()):
        domain = m.group(1)
        if any(b in domain for b in BAD_DOMAINS):
            continue

        stem = domain.split(".")[0]
        score = SequenceMatcher(None, brand, stem).ratio()
        candidates.append((score, domain))

    if candidates:
        candidates.sort(reverse=True)
        if candidates[0][0] > 0.45:
            return f"https://{candidates[0][1]}"
    return None

def extract_competitors(raw_text, company_name=None):
    competitors = set()
    text = raw_text.replace("\n", " ")

    # Case 1: Tracxn Style
    tracxn_pattern = re.compile(r"Top competitors? of .*? include(.*?)(?:\.|\n|Here is)", re.I)
    match = tracxn_pattern.search(text)
    if match:
        block = match.group(1)
        # Extract markdown links
        links = re.findall(r"\[([A-Za-z0-9&.\- ]{2,50})\]\(", block)
        competitors.update(links)
        # Extract plain text
        parts = re.split(r",| and ", block)
        for p in parts:
            p = strip_markdown_and_urls(p).strip()
            if 2 <= len(p) <= 50:
                competitors.add(p)

    # Case 2: RocketReach/General List Style
    rr_pattern = re.compile(r"Competitors\s*[:\-]?\s*(.*)", re.I)
    for line in raw_text.split("\n"):
        m = rr_pattern.search(line)
        if m:
            for c in m.group(1).split(","):
                name = c.strip()
                if 2 <= len(name) <= 50:
                    competitors.add(name)

    # Final Cleanup
    cleaned = set()
    for c in competitors:
        c = re.sub(r"[^A-Za-z0-9&.\- ]", "", c).strip()
        if not c or (company_name and c.lower() == company_name.lower()):
            continue
        if len(c.split()) > 6 or any(bad in c.lower() for bad in ["image", "logo", "extension"]):
            continue
        cleaned.add(c)
    return sorted(cleaned)

def extract_news(raw_text, company_name):
    news = []
    # Find the specific Tracxn news block
    section_match = re.search(
        r"News related to .*?\n[-]+\n(.*?)(?:Get curated news|View complete company profile|$)",
        raw_text, re.S | re.I
    )
    if not section_match:
        return news

    news_block = section_match.group(1)
    # Pattern for [Title](URL) Source • Date • [Related Companies]
    news_pattern = re.compile(
        r"\[([^\]]{10,200})\]\((https?:\/\/[^\)]+)\)"
        r"(?:(?:\s+)?([A-Za-z ]+))?•([A-Za-z]{3} \d{2}, \d{4})•([^\n]+)",
        re.I
    )

    for m in news_pattern.finditer(news_block):
        related_raw = m.group(5)
        related = re.findall(r"\[([A-Za-z0-9&.\- ]+)\]\(", related_raw)
        news.append({
            "title": m.group(1).strip(),
            "url": m.group(2).strip(),
            "source": m.group(3).strip() if m.group(3) else "Unknown",
            "date": m.group(4).strip(),
            "related_companies": list(set(related)) if related else [company_name]
        })
    return news

def extract_leadership(raw_text):
    leadership = {"founders": [], "board_members": [], "key_people": []}
    seen = set()
    ROLE_REGEX = re.compile(
        r"(co[- ]?founder|founder|chief executive officer|ceo|chief technology officer|cto|"
        r"chief financial officer|cfo|vice president|vp|director|board member|chairman|president)",
        re.I
    )

    for line in raw_text.split("\n"):
        clean = strip_markdown_and_urls(line)
        if not ROLE_REGEX.search(clean):
            continue

        # Find 2-4 capitalized words for the Name
        name_match = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b", clean)
        if not name_match:
            continue

        name = name_match.group(1)
        role_match = ROLE_REGEX.search(clean)
        role_text = clean[role_match.start():].split(".")[0].strip()

        if (name.lower(), role_text.lower()) in seen:
            continue
        seen.add((name.lower(), role_text.lower()))

        entry = {"name": name, "role": role_text}
        if "founder" in role_text.lower():
            leadership["founders"].append(entry)
        elif any(x in role_text.lower() for x in ["board", "director"]):
            leadership["board_members"].append(entry)
        else:
            leadership["key_people"].append(entry)
    return leadership

# -------------------------------------------------
# DATA CLEANING & HELPERS
# -------------------------------------------------

def clean_text_light(text):
    if not text: return ""
    text = re.sub(r"\!\[.*?\]\(.*?\)", " ", text)
    text = re.sub(r"https?:\/\/\S+", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def clean_text_heavy(text):
    tokens = nltk.word_tokenize(text.lower())
    return " ".join([lemmatizer.lemmatize(t) for t in tokens if t.isalpha() and t not in stop_words])

def extract_sentence_containing(text, keywords):
    for s in re.split(r"[.\n]", text):
        if any(k.lower() in s.lower() for k in keywords):
            return s.strip()
    return None

def extract_financials(text):
    data = {}
    if m := re.search(r"\$([\d\.]+)\s*(M|B)", text, re.I):
        data["estimated_revenue_usd"] = f"${m.group(1)}{m.group(2)}"
    if m := re.search(r"Employees?\s*[:\-]?\s*([\d,]+)", text, re.I):
        raw_emp = m.group(1).replace(",", "")
        emp = safe_int(raw_emp)

        if emp is not None:
            data["employees"] = emp

    return data

def extract_locations(text):
    common = ["India", "USA", "United States", "UK", "UAE", "Canada", "Australia"]
    return sorted({c for c in common if c.lower() in text.lower()})

def extract_emails(text):
    return sorted(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)))

def extract_phone_numbers(text):
    matches = re.findall(r"(?:\+?\d{1,3}[\s\-]?)?(?:\(?\d{2,4}\)?[\s\-]?)?\d{3,4}[\s\-]?\d{3,4}", text)
    return sorted({m for m in matches if 8 <= len(re.sub(r"\D", "", m)) <= 15})

# -------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------
import os
from pathlib import Path

def extract_company_intelligence(input_json, output_json):
    input_json = Path(input_json)
    output_json = Path(output_json)

    if not input_json.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_json}")

    with input_json.open("r", encoding="utf-8") as f:
        data = json.load(f)

    company_name = data.get("meta", {}).get("company_name", "")

    combined_raw_text = ""
    for r in data.get("financial_intelligence", []):
        combined_raw_text += "\n" + r.get("content", "")
        combined_raw_text += "\n" + r.get("raw_content", "")

    clean_light = clean_text_light(combined_raw_text)

    output = {
        "meta": data.get("meta", {}),
        "company_profile": {
            "company_name": company_name,
            "website": find_closest_company_website(combined_raw_text, company_name),
            "industry": extract_sentence_containing(clean_light, ["industry"]),
            "tagline": extract_sentence_containing(clean_light, ["specializing", "leader", "delivering"]),
        },
        "leadership_team": extract_leadership(combined_raw_text),
        "competitors": extract_competitors(combined_raw_text, company_name),
        "news": extract_news(combined_raw_text, company_name),
        "financials": extract_financials(clean_light),
        "locations": extract_locations(clean_light),
        "contact_information": {
            "emails": extract_emails(clean_light),
            "phone_numbers": extract_phone_numbers(clean_light)
        }
    }

    # ✅ Ensure output directory exists
    output_json.parent.mkdir(parents=True, exist_ok=True)

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    print(f"✅ Extracted structured intelligence → {output_json}")



def clean_all_unstructured_reports(
    unstructured_dir="Unstructured_data",
    structured_dir="structured_data"
):
    unstructured_dir = Path(unstructured_dir)
    structured_dir = Path(structured_dir)

    # ❗ SAFETY: unstructured dir may not exist yet
    if not unstructured_dir.exists():
        print(f"⚠️ Unstructured directory not found: {unstructured_dir}")
        print("⚠️ Skipping cleaning step")
        return

    structured_dir.mkdir(parents=True, exist_ok=True)

    files = list(unstructured_dir.glob("*_Report.json"))

    print(f"🧹 Cleaning {len(files)} unstructured reports...")

    for file in files:
        output_path = structured_dir / file.name.replace(
            "_Report.json", "_Structured.json"
        )

        try:
            extract_company_intelligence(
                input_json=file,
                output_json=output_path
            )
        except Exception as e:
            print(f"❌ Failed cleaning {file.name}: {e}")

    print("✅ All reports cleaned and structured.")
