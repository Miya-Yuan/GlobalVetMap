import pandas as pd
import os
import time
from random import randint
import concurrent.futures

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

from blacklist_config import BLACKLIST

# Start timer
start_time = time.time()

# Paths and config
BASE_DIR = "C:/Users/myuan/Desktop/CHE"
INPUT_PATH = os.path.join(BASE_DIR, "VP_cleaned.csv")
SAVE_PATH = os.path.join(BASE_DIR, "VP_website_filled.csv")
AUTOSAVE_PATH = os.path.join(BASE_DIR, "website_autosave.csv")

MAX_WORKERS = 4
SAVE_INTERVAL = 25  # Autosave frequency

# Load data
df = pd.read_csv(INPUT_PATH)
total_rows = len(df)

# Clean: Remove all Website entries that are not valid http/https links
def clean_invalid_urls(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    return value if value.startswith("http://") or value.startswith("https://") else None

df["Website"] = df["Website"].apply(clean_invalid_urls)
initial_missing = df["Website"].isna().sum()

# Google search logic
def get_lucky_url(company_name, address, retries=2):
    attempt = 0
    while attempt <= retries:
        driver = None
        try:
            time.sleep(randint(1, 3))
            opts = Options()
            opts.add_argument("headless=new")
            opts.add_argument("--window-size=1280,800")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--log-level=3")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_argument("--disable-software-rasterizer")
            opts.add_argument("--mute-audio")
            opts.add_argument("--disable-logging")
            opts.add_argument("--disable-voice-input")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)
            opts.add_experimental_option("prefs", {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.stylesheets": 2
            })

            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=opts
            )
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            wait = WebDriverWait(driver, 10)

            driver.get("https://www.google.com")
            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "div[class='QS5gu sy4vM']"))
                ).click()
            except TimeoutException:
                pass

            search_input = wait.until(EC.presence_of_element_located((By.NAME, "q")))
            search_input.clear()
            search_input.send_keys(f"{company_name} {address}")
            time.sleep(2)

            try:
                lucky_button = wait.until(EC.presence_of_element_located((By.NAME, "btnI")))
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", lucky_button)
                time.sleep(0.5)
                try:
                    lucky_button.click()
                except:
                    driver.execute_script("arguments[0].click();", lucky_button)
            except Exception:
                search_input.send_keys(Keys.RETURN)

            time.sleep(3)
            final_url = driver.current_url
            if "sorry/index" in final_url or "interstitial" in driver.page_source.lower():
                raise ValueError("Google CAPTCHA or block")

            final_url_lower = final_url.lower()
            if any(bad in final_url_lower for bad in BLACKLIST):
                raise ValueError("Blacklisted URL")

            # Reject any URL that is not HTTPS
            if not final_url.startswith("https://"):
                raise ValueError("Non-HTTPS URL rejected")

            return final_url

        except Exception:
            attempt += 1
            time.sleep(2)
            if attempt > retries:
                return None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

# Process one row
def process_row(row):
    if pd.isna(row["Name"]) or str(row["Name"]).strip() == "":
        return (row.name, None)
    if pd.notna(row["Website"]) and str(row["Website"]).strip() != "":
        return (row.name, row["Website"])
    url = get_lucky_url(row["Name"], row["Address"])
    return (row.name, url if url else None)

# Parallel execution with periodic autosaving
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_row, row) for _, row in df.iterrows()]
    completed = 0
    for future in concurrent.futures.as_completed(futures):
        idx, url = future.result()
        if idx is not None:
            df.at[idx, "Website"] = url
        completed += 1
        if completed % SAVE_INTERVAL == 0:
            df.to_csv(AUTOSAVE_PATH, index=False)
            print(f"💾 Autosave: {completed} rows processed and saved to: {AUTOSAVE_PATH}")

# Final save
df.to_csv(SAVE_PATH, index=False)

# Report
final_missing = df["Website"].isna().sum()
filled_count = initial_missing - final_missing
elapsed = time.time() - start_time
minutes = int(elapsed // 60)
seconds = int(elapsed % 60)

print(f"✅ Website entries filled during this run: {filled_count}")
print(f"🚫 Website entries still missing: {final_missing}")
print(f"💾 Final data saved to: {SAVE_PATH}")
print(f"⏱️ Total time elapsed: {minutes} minutes {seconds} seconds")
