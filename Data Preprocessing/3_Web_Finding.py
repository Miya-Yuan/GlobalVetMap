import pandas as pd
import os
import time
import socket
import tldextract
from random import uniform
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse
import tldextract
from blacklist_config import BLACKLIST

# --- CONFIGURATION ---
BASE_DIR = "C:/Users/myuan/Desktop/CHE"
INPUT_PATH = os.path.join(BASE_DIR, "VP_cleaned.csv")
SAVE_PATH = os.path.join(BASE_DIR, "VP_website_filled.csv")
AUTOSAVE_PATH = os.path.join(BASE_DIR, "website_autosave.csv")

MAX_WORKERS = 4
BATCH_SIZE = 20
GOOGLE_BLOCK_THRESHOLD = 3
country_code = "ch"

# --- GLOBAL STATE ---
google_block_count = 0
use_bing_only = False
NETLOC_BLOCKLIST = {
    # 🔗 Messaging platforms
    "wa.me", "web.whatsapp.com", "t.me", "telegram.me", "signal.org", "messenger.com",

    # 🔗 URL shorteners & redirectors
    "bit.ly", "tinyurl.com", "shorturl.at", "rebrand.ly", "goo.gl",
    "t.co", "fb.me", "lnkd.in", "shorte.st", "ow.ly", "buff.ly",        
    "smarturl.it", "is.gd", "v.gd", "s.id", "cutt.ly", "rb.gy"
}
non_html_extensions = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".zip", ".rar", ".mp4", ".mp3", ".avi", ".mov"
)
# --- CLEAN EXISTING URLS ---
def clean_and_filter_url(url):
    try:
        if pd.isna(url):
            return None

        url = str(url).strip().lower()
        if url in ("", "none", "null", "nan"):
            return None

        if not (url.startswith("http://") or url.startswith("https://")):
            return None

        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return None
        socket.gethostbyname(hostname)  # DNS check

        ext = tldextract.extract(hostname)
        suffix = ext.suffix
        # Extract final part of the suffix
        suffix_parts = suffix.split(".")
        last_suffix = suffix_parts[-1]
        if len(last_suffix) == 2 and last_suffix != country_code:
            return None  # Foreign ccTLD
        
        full_url = parsed.path + parsed.query
        if full_url.endswith(non_html_extensions):
            return f"{parsed.scheme}://{parsed.hostname}/"

        return url
    except Exception:
        return None

def is_blacklisted(url):
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower().strip()

        # Block exact domain/subdomain combinations like "wa.me"
        if netloc in NETLOC_BLOCKLIST:
            print(f"🧱 Blocked netloc: {netloc} from URL: {url}")
            return True

        extracted = tldextract.extract(url)
        domain = extracted.domain.lower().strip() 
        subdomain = extracted.subdomain.lower().strip() 
        # ⚠️ Only exact match
        if domain in BLACKLIST:
            print(f"🧱 Blocked exact domain: {domain}.{extracted.suffix} from URL: {url}")
            return True
        for part in subdomain.split("."):
            if part in BLACKLIST:
                print(f"🧱 Blocked subdomain part: {part} in {subdomain} from URL: {url}")
                return True

        return False
    except Exception:
        return False

# --- TYPING SIMULATION ---
def simulate_typing(element, text, delay_range=(0.05, 0.15)):
    for char in text:
        element.send_keys(char)
        time.sleep(uniform(*delay_range))

# --- GOOGLE SEARCH ---
def get_top_google_result(company_name, address, max_retries=3):
    global google_block_count
    query = f"{company_name} {address}"
    for attempt in range(max_retries):
        driver = None
        try:
            time.sleep(uniform(2.0, 4.5))

            opts = Options()
            opts.add_argument("headless=new")
            opts.add_argument("--window-size=1280,800")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--log-level=3")  # Suppress most Chrome logs
            opts.add_argument("--disable-logging")
            opts.add_argument("--silent") 
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)
            opts.add_experimental_option("prefs", {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.stylesheets": 2,
                "profile.managed_default_content_settings.javascript": 2
            })

            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            wait = WebDriverWait(driver, 10)

            driver.get("https://www.google.com")
            try:
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div[class='QS5gu sy4vM']"))).click()
            except TimeoutException:
                pass

            search_input = wait.until(EC.presence_of_element_located((By.NAME, "q")))
            search_input.clear()
            simulate_typing(search_input, query)
            search_input.submit()

            time.sleep(uniform(2.5, 4.0))

            if "sorry/index" in driver.current_url or "interstitial" in driver.page_source.lower():
                google_block_count += 1
                print(f"[BLOCKED] Google blocked access for query: {query} (block #{google_block_count})")
                raise ValueError("Google CAPTCHA or interstitial detected")
            else:
                google_block_count = 0  # reset on success

            result_links = driver.find_elements(By.CSS_SELECTOR, "div.yuRUbf > a")
            if not result_links:
                raise ValueError("No Google results")

            url = result_links[0].get_attribute("href").strip()
            if not url.startswith("http://") and not url.startswith("https://"):
                raise ValueError("Invalid scheme — must start with http or https")
            if is_blacklisted(url):
                raise ValueError("Blacklisted domain")

            return url

        except Exception:
            backoff = (attempt + 1) * 3
            time.sleep(backoff)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    return None

# --- BING SEARCH FALLBACK ---
def get_top_bing_result(company_name, address):
    query = f"{company_name} {address}"
    driver = None
    try:
        print(f"🔁 Using Bing for: {query}")
        time.sleep(uniform(2.0, 4.5))

        opts = Options()
        opts.add_argument("headless=new")
        opts.add_argument("--window-size=1280,800")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.stylesheets": 2,
            "profile.managed_default_content_settings.javascript": 2
        })

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        driver.get(f"https://www.bing.com/search?q={query}")
        time.sleep(uniform(2.0, 3.0))

        links = driver.find_elements(By.CSS_SELECTOR, "li.b_algo h2 a")
        if not links:
            return None

        url = links[0].get_attribute("href").strip()
        if not url.startswith("http://") and not url.startswith("https://"):
            return None
        if is_blacklisted(url):
                raise ValueError("Blacklisted domain")

        return url

    except Exception:
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

# --- COMBINED SEARCH ---
def safe_search(company_name, address):
    global google_block_count, use_bing_only
    if use_bing_only or google_block_count >= GOOGLE_BLOCK_THRESHOLD:
        if not use_bing_only:
            print(f"[INFO] Too many Google blocks — switching to Bing for all future queries.")
            use_bing_only = True
        return get_top_bing_result(company_name, address)

    url = get_top_google_result(company_name, address)
    if url:
        google_block_count = 0
        return url

    return get_top_bing_result(company_name, address)

# --- PROCESS ONE ROW ---
def process_row(row):
    if pd.isna(row["Name"]) or str(row["Name"]).strip() == "":
        return (row.name, None)
    if pd.notna(row["Website"]) and str(row["Website"]).strip() != "":
        return (row.name, row["Website"])
    url = safe_search(row["Name"], row["Address"])
    return (row.name, url if url else None)

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    start_time = time.time()

    df = pd.read_csv(INPUT_PATH)
    pre_clean_initial = df['Website'].notna().sum()
    df['Website'] = df['Website'].apply(clean_and_filter_url)
    post_clean_initial = df['Website'].notna().sum()
    print(f"🧹Website values before cleaning = {pre_clean_initial}, after cleaning = {post_clean_initial}")
    total_rows = len(df)

    all_indices = list(df.index)
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for batch_start in range(0, len(all_indices), BATCH_SIZE):
            batch_indices = all_indices[batch_start:batch_start + BATCH_SIZE]
            futures = [executor.submit(process_row, df.loc[idx]) for idx in batch_indices]

            for future in concurrent.futures.as_completed(futures):
                idx, url = future.result()
                if idx is not None:
                    df.at[idx, "Website"] = url
                completed += 1
                if completed % BATCH_SIZE == 0:
                    df.to_csv(AUTOSAVE_PATH, index=False)
                    print(f"💾 Autosave: {completed} rows processed and saved to: {AUTOSAVE_PATH}")

            print("🕐 Waiting before next batch...")
            time.sleep(uniform(10.0, 20.0))

    def blacklist_check(url):
        try:
            netloc = urlparse(url).netloc.lower().replace("www.", "")
            # Exact match for blocked netlocs
            if netloc in NETLOC_BLOCKLIST:
                return True
            # Substring match for broader blacklist
            return any(term in netloc for term in BLACKLIST)
        except Exception:
            return False
    
    pre_clean_filled = df['Website'].notna().sum()
    df["Website"] = df["Website"].apply(clean_and_filter_url)
    df["Website"] = df["Website"].where(df["Website"].apply(lambda x: not blacklist_check(x) if pd.notna(x) else True))
    post_clean_filled = df['Website'].notna().sum()
    print(f"🧹Website values before cleaning = {pre_clean_filled}, after cleaning = {post_clean_filled}")
    df.to_csv(SAVE_PATH, index=False)

    filled_count = post_clean_filled - post_clean_initial
    missing_websites = df['Website'].isna().sum()
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print(f"✅ Website entries filled during this run: {filled_count}")
    print(f"🚫 Website entries still missing: {missing_websites}")
    print(f"💾 Final data saved to: {SAVE_PATH}")
    print(f"⏱️ Total time elapsed: {minutes} minutes {seconds} seconds")
