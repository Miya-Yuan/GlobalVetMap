import os
from datetime import datetime
import pandas as pd
import numpy as np
import asyncio
import langid
import re
import unicodedata
import contextlib
from more_itertools import chunked
from rapidfuzz import fuzz
from bs4 import BeautifulSoup
from urllib.parse import urljoin,urlparse
from playwright.async_api import async_playwright
from service_config import SERVICE_CONFIG
from cookie_config import COOKIE_CONFIG
from ua_config import USER_AGENTS_FIREFOX
import random

# === Config ===
BASE_DIR = "C:/Users/myuan/Desktop"
COUNTRY_DIR = "CHE"
KEYWORD_DIR = "C:/Users/myuan/Desktop/VetMap/Keyword"
INPUT_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_website_filled.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_filtered.csv")
ANIMAL_KW_PATH = os.path.join(KEYWORD_DIR, "animal_keywords.csv")
CLINIC_KW_PATH = os.path.join(KEYWORD_DIR, "vet_keywords.csv")
NON_CLINIC_KW_PATH = os.path.join(KEYWORD_DIR, "nonclinic_keywords.csv")
#TEXT_OUT_DIR = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_service")
#os.makedirs(TEXT_OUT_DIR, exist_ok=True)

# === Helpers ===
def is_valid_url(url):
    if not isinstance(url, str):
        return False
    url = url.strip().lower()
    if url in ("", "none", "null", "nan"):
        return False
    return url.startswith("http://") or url.startswith("https://")

def normalize_text(text):
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    text = re.sub(r"[\u00A0\u200B]+", " ", text)  # remove non-breaking spaces
    text = text.replace("-", " ")
    text = re.sub(r"[^\w\s]", "", text)  # remove punctuation but preserve words
    text = re.sub(r"\s+", " ", text).strip()
    return text

def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").rstrip()

def get_homepage_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/"

def load_multilingual_keywords(csv_path):
    df = pd.read_csv(csv_path)
    if 'Language' not in df.columns or 'Keyword' not in df.columns:
        raise ValueError(f"{csv_path} must contain 'Language' and 'Keyword' columns.")
    df.dropna(subset=['Language', 'Keyword'], inplace=True)
    df['Keyword'] = df['Keyword'].apply(lambda k: normalize_text(str(k)))
    return df.groupby('Language')['Keyword'].apply(set).to_dict()

clinic_keywords_by_lang = load_multilingual_keywords(CLINIC_KW_PATH)
non_clinic_keywords_by_lang = load_multilingual_keywords(NON_CLINIC_KW_PATH)

non_html_extensions = (
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
        ".pdf", ".doc", ".docx", ".xls", ".xlsx",
        ".zip", ".rar", ".mp4", ".mp3", ".avi", ".mov"
    )

def is_probably_html(url):
    # Heuristic to determine if a URL does not point to binary/non-HTML content
    return not any(url.lower().endswith(ext) for ext in non_html_extensions)

def has_loose_match(text, keyword_list, threshold=85):
    text = normalize_text(text)
    return any(fuzz.partial_ratio(k, text) >= threshold for k in keyword_list)

def extract_service_links(soup, base_url, keywords):
    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a['href'].strip()
        if any(k in text for k in keywords):
            links.append(urljoin(base_url, href))
    return list(set(links))

def load_animal_keywords(path):
    df = pd.read_csv(path, encoding="utf-8")
    df['Category'] = df['Category'].str.strip().str.lower()
    df['Keyword'] = df['Keyword'].dropna().apply(normalize_text)
    out = {}
    for lang in df['Language'].unique():
        lang_dict = {
            cat: df[(df['Language'] == lang) & (df['Category'] == cat)]['Keyword'].tolist()
            for cat in df[df['Language'] == lang]['Category'].unique()
        }
        out[lang] = lang_dict
    return out

# === Handle Cookie Banners ===
async def handle_cookie_banner(page, cookie_keywords, max_attempts=2, delay=2, threshold=85):
    async def safe_click(el):
        try:
            await asyncio.wait_for(el.scroll_into_view_if_needed(), timeout=5)
            await asyncio.wait_for(el.click(timeout=3000), timeout=5)
            await page.wait_for_timeout(500)
            return True
        except Exception:
            return False

    async def try_dismiss_in_frame(frame):
        with contextlib.suppress(Exception):
            elements = await asyncio.wait_for(
                frame.locator("button, a, div, [role='button']").all(), timeout=10
            )
            for el in elements[:50]:
                with contextlib.suppress(Exception):
                    if not await asyncio.wait_for(el.is_visible(), timeout=2):
                        continue
                    text = (await asyncio.wait_for(el.inner_text(), timeout=3)).strip()
                    if not text:
                        continue
                    norm_text = normalize_text(text)
                    for kw in cookie_keywords:
                        norm_kw = normalize_text(kw)
                        if norm_kw in norm_text or fuzz.ratio(norm_kw, norm_text) >= threshold:
                            if await safe_click(el):
                                return True
        return False

    # Try click-based dismissal first
    for attempt in range(max_attempts):
        try:
            if await try_dismiss_in_frame(page.main_frame):
                return True
            for frame in page.frames:
                if frame != page.main_frame:
                    if await try_dismiss_in_frame(frame):
                        return True
        except Exception:
            pass
        await page.wait_for_timeout(delay * 1000)

    # JS fallback: remove overlays
    try:
        await page.evaluate("""
            (() => {
                const selectors = [
                    "div[class*='overlay']",
                    "div[class*='cookie']",
                    "div[class*='consent']",
                    "div[class*='popup']",
                    "div[class*='backdrop']",
                    "div[id*='overlay']",
                    "div[id*='cookie']",
                    "div[id*='consent']",
                    "div[id*='popup']",
                    "div[id*='backdrop']"
                ];
                selectors.forEach(selector => {
                    document.querySelectorAll(selector).forEach(el => el.remove());
                });
            })();
        """)
        return False
    except Exception:
        print("❌ Cookie popup not dismissed")
        return False

def classify_clinic_status(text, clinic_keywords_by_lang, non_clinic_keywords_by_lang, lang_code):
    def has_fuzzy_match(text, keywords, threshold=85):
        return any(fuzz.partial_ratio(k, text) >= threshold for k in keywords)

    # Collect clinic and non-clinic keywords for detected language + English fallback
    clinic_kw = set(clinic_keywords_by_lang.get(lang_code, set())) | set(clinic_keywords_by_lang.get("en", set()))
    non_clinic_kw = set(non_clinic_keywords_by_lang.get(lang_code, set())) | set(non_clinic_keywords_by_lang.get("en", set()))

    found_clinic = has_fuzzy_match(text, clinic_kw)
    found_non_clinic = has_fuzzy_match(text, non_clinic_kw)

    if found_clinic and found_non_clinic:
        return "yes"  # Clinic takes priority
    elif found_clinic:
        return "yes"
    elif found_non_clinic:
        return "no"
    else:
        return "uncertain"

async def fetch_text(context, url, cookie_keywords):
    page = None
    try:
        page = await asyncio.wait_for(context.new_page(), timeout=30)
        try:
            await asyncio.wait_for(page.goto(url, timeout=30000), timeout=60)
            await asyncio.sleep(1)
            await asyncio.wait_for(handle_cookie_banner(page, cookie_keywords), timeout=30)
            await asyncio.wait_for(page.wait_for_load_state("networkidle"), timeout=45)
            content = await asyncio.wait_for(page.content(), timeout=60)
            soup = BeautifulSoup(content, "html.parser")
            text = normalize_text(soup.get_text(separator=" ", strip=True))
            return text, soup
        finally:
            # Ensure the page is closed only once and suppress any errors
            with contextlib.suppress(Exception):
                await page.close()
    except Exception:
        with contextlib.suppress(Exception):
            if page:
                await page.close()
        return None, None
    
    
async def process_row(context, row, animal_keywords_all, clinic_name, needs_spec=True):
    url = str(row.get("Website", "")).strip()
    if not is_valid_url(url):
        return None

    if not is_probably_html(url):
        url = get_homepage_url(url)

    cookie_keywords = COOKIE_CONFIG.get("en", {}).get("COOKIE_BUTTON_KEYWORDS", [])
    text, soup = await fetch_text(context, url, cookie_keywords)

    if not text:
        url = get_homepage_url(url)
        text, soup = await fetch_text(context, url, cookie_keywords)
        if not text:
            return None

    lang_code, _ = langid.classify(text)
    lang = lang_code if lang_code in animal_keywords_all else "en"
    animal_keywords = {}

    # Merge detected language + English keywords per category
    if lang in animal_keywords_all:
        for category in ["small animals", "large animals", "horses"]:
            lang_kw = set(animal_keywords_all[lang].get(category, []))
            en_kw   = set(animal_keywords_all.get("en", {}).get(category, []))
            animal_keywords[category] = lang_kw.union(en_kw)
    else:
        # fallback to English only
        animal_keywords = animal_keywords_all.get("en", {})

    service_keywords = SERVICE_CONFIG.get(lang, SERVICE_CONFIG.get("en", {})).get("SERVICE_PAGE_KEYWORDS", [])
    service_links = extract_service_links(soup, url, service_keywords)

    combined_text = text
    for link in service_links:
        service_text, _ = await fetch_text(context, link, cookie_keywords)
        if service_text:
            combined_text += " " + service_text
    '''
    # Save the full text to a .txt file using sanitized clinic name
    filename = sanitize_filename(clinic_name) + ".txt"
    filepath = os.path.join(TEXT_OUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(combined_text)
    '''
    # Keyword classification
    spec = []
    if needs_spec:
        if "small animals" in animal_keywords and has_loose_match(combined_text, animal_keywords["small animals"]):
            spec.append("small animals")
        if "large animals" in animal_keywords and has_loose_match(combined_text, animal_keywords["large animals"]):
            spec.append("large animals")
        if "horses" in animal_keywords and has_loose_match(combined_text, animal_keywords["horses"]):
            spec.append("horses")

    # Clinic / non-clinic keyword classification
    clinic_status = classify_clinic_status(combined_text, clinic_keywords_by_lang, non_clinic_keywords_by_lang, lang_code)

    # Final specialization value: detected or existing
    final_spec = ", ".join(spec) if needs_spec and spec else str(row.get("Specialization", "")).lower()

    result = {
        "Name": clinic_name,
        "Website": url,
        "Clinic": clinic_status,
        "Specialization": ", ".join(spec) if needs_spec and spec else row.get("Specialization", np.nan),
        "Small Animals": int("small animals" in final_spec),
        "Large Animals": int("large animals" in final_spec),
        "Horses": int("horses" in final_spec),
        "Specialization_Reason": "no_species_match" if needs_spec and not spec else "match" if needs_spec else "skipped"
    }
    if needs_spec and not spec:
        print(f"⚠️ {clinic_name}: No species keywords matched")
    return result

async def process_batch(batch_idx, batch, df, animal_keywords_all):
    async def init_browser():
        user_agent = random.choice(USER_AGENTS_FIREFOX)
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1280, "height": 800},
            locale="en-US"
        )
        return browser, context

    async with async_playwright() as p:
        browser, context = await init_browser()

        for i, row in batch:
            from datetime import datetime
            current_time = datetime.now().strftime('%H:%M:%S')
            clinic_name = str(row.get("Name", "unknown")).strip()
            print(f"🔍 {current_time} - Processing {i}: {clinic_name}")

            # Skip if Clinic is already valid
            clinic_val = str(row["Clinic"]) if pd.notna(row["Clinic"]) else ""
            clinic_val = clinic_val.strip().lower()
            if clinic_val in {"yes", "no"}:
                print(f"⏭️  {clinic_name}: Skipped (Clinic already set to '{clinic_val}')")
                continue

            needs_spec = bool(row.get("Needs_Spec", True))
            out = None

            for attempt in range(3):
                try:
                    out = await asyncio.wait_for(
                        process_row(context, row, animal_keywords_all, clinic_name, needs_spec=needs_spec),
                        timeout=120
                    )
                    break
                except Exception:
                    try:
                        await context.close()
                        await browser.close()
                    except:
                        pass
                    try:
                        browser, context = await init_browser()
                    except:
                        break
                    await asyncio.sleep(5)

            df["Name_norm"] = df["Name"].apply(sanitize_filename)
            target = sanitize_filename(clinic_name)
            idx = df[df["Name_norm"] == target].index

            if out is None:
                print(f"❌ {clinic_name}: Fetch failed — page not retrievable after 3 retries")
                if not idx.empty:
                    df.loc[idx[0], "Clinic"] = "uncertain"
                continue

            # Retry specialization if needed
            if needs_spec and out.get("Specialization_Reason") == "no_species_match":
                try:
                    retry_out = await asyncio.wait_for(
                        process_row(context, row, animal_keywords_all, clinic_name, needs_spec=needs_spec),
                        timeout=120
                    )
                    if retry_out and retry_out.get("Specialization") and retry_out.get("Specialization_Reason") != "no_species_match":
                        out = retry_out
                except Exception:
                    pass

            # Save results to DataFrame
            if not idx.empty:
                for col in ["Clinic", "Specialization", "Small Animals", "Large Animals", "Horses"]:
                    df.loc[idx[0], col] = out.get(col, pd.NA)
            else:
                print(f"⚠️ No match found in input for {clinic_name}")

        # Save entire DataFrame once per batch
        df.drop(columns="Name_norm", inplace=True, errors="ignore")
        df.sort_index(inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"💾 Batch {batch_idx} saved to {OUTPUT_PATH}")

        await context.close()
        await browser.close()

        if "Clinic_norm" in df.columns:
            df.drop(columns="Clinic_norm", inplace=True)


async def Spec_service():
    df = pd.read_csv(INPUT_PATH)
    df["Clinic"] = df["Clinic"].astype("string").str.strip().str.lower()    
    df.reset_index(drop=True, inplace=True)
    spec_before = df['Specialization'].notna().sum() if 'Specialization' in df.columns else 0
    df.replace(to_replace=[None, '', 'nan', 'null', 'none'], value=np.nan, inplace=True)
    # Mark Clinic as 'uncertain' for rows with missing or invalid websites
    invalid_url_mask = ~df["Website"].apply(is_valid_url)
    df.loc[invalid_url_mask, "Clinic"] = "uncertain"
    for col in ["Specialization", "Small Animals", "Large Animals", "Horses"]:
        if col not in df.columns:
            df[col] = np.nan
    df_to_process = df[df['Website'].notna()].copy()
    # Normalize Clinic column to lowercase strings
    df_to_process["Clinic_norm"] = df_to_process["Clinic"].astype(str).str.strip().str.lower()
    # Only process rows where Clinic is missing or invalid
    df_to_process = df_to_process[~df_to_process["Clinic_norm"].isin(["yes", "no"])].copy()
    df_to_process.reset_index(drop=True, inplace=True)
    # Determine if specialization detection is needed
    df_to_process["Needs_Spec"] = df_to_process["Specialization"].isna()
    # 🔢 Show number of rows to process
    print(f"📋 Total rows to process: {len(df_to_process)}")

    animal_keywords_all = load_animal_keywords(ANIMAL_KW_PATH)
    batch_size = 20
    batches = list(chunked(df_to_process.iterrows(), batch_size))

    from asyncio import Semaphore
    semaphore = Semaphore(2)

    async def limited_process_batch(*args):
        async with semaphore:
            await process_batch(*args)

    tasks = [limited_process_batch(i, batch, df, animal_keywords_all) for i, batch in enumerate(batches)]
    await asyncio.gather(*tasks)

    df_final = pd.read_csv(OUTPUT_PATH).sort_index()
    spec_after = df_final['Specialization'].notna().sum()
    print(f"📌 Before processing: {spec_before} rows with Specialization")
    print(f"📌 After processing: {spec_after} rows with Specialization")
    def set_flag(val, key):
        if pd.isna(val) or str(val).strip() == "":
            return np.nan
        val = str(val).lower()
        return key in val

    df_final["Small Animals"] = df_final["Specialization"].apply(lambda x: set_flag(x, "small animals"))
    df_final["Large Animals"] = df_final["Specialization"].apply(lambda x: set_flag(x, "large animals"))
    df_final["Horses"] = df_final["Specialization"].apply(lambda x: set_flag(x, "horses"))

    if "Clinic" in df_final.columns:
        summary = df_final["Clinic"].value_counts(dropna=False).reindex(["yes", "no", "uncertain"], fill_value=0)
        print("\n--- Clinic Classification Summary ---")
        print(f"Yes:       {summary['yes']}")
        print(f"No:        {summary['no']}")
        print(f"Uncertain: {summary['uncertain']}")

    df["Clinic"] = df["Clinic"].fillna("uncertain")
    df_final = df_final[~df_final["Clinic"].str.lower().eq("no")]
    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Done. Saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    asyncio.run(Spec_service())