import os
import pandas as pd
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
import unicodedata
import re
import time
from difflib import SequenceMatcher
from PIL import Image
from team_config import TEAM_CONFIG
import langid
import concurrent.futures
from rapidfuzz import fuzz
import multiprocessing
from multiprocessing import Process

# File DIR
#BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = "C:/Users/myuan/Desktop"
COUNTRY_DIR = "CHE"
INPUT_FILE = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_filtered.csv")
OUTPUT_FOLDER = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_image")
LARGE_FOLDER = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_large")
# Create the directory if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True) 
SIMILARITY_THRESHOLD = 85
non_html_extensions = (
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
        ".pdf", ".doc", ".docx", ".xls", ".xlsx",
        ".zip", ".rar", ".mp4", ".mp3", ".avi", ".mov"
    )

# Keywords and settings
BUTTON_SELECTORS = [
        "#blogloader",
        ".e-loop__load-more .elementor-button-link",
        ".e-loop__load-more.elementor-button-wrapper a.elementor-button-link",
        "a.elementor-button-link[role='button']",
        "button.load-more",
        "a.load-more",
        ".load-more",
        ".show-more",
        ".btn-more",
        ".more-btn",
        "button[class*='load']",
        "a[class*='load']",
        "button[class*='more']",
        "a[class*='more']"
    ]
# Utility functions
def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").rstrip()

def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode().lower()
    return text.replace("-", " ").strip()

def detect_language(text):
    lang, _ = langid.classify(text)
    return lang

def get_team_config(lang_code, default_lang="de"):
    config = TEAM_CONFIG.get(lang_code)
    if not config and default_lang:
        config = TEAM_CONFIG.get(default_lang)
    if not config:
        raise ValueError(f"No TEAM_CONFIG found for language code: {lang_code}")
    # Normalize all relevant fields once
    config["TEAM_KEYWORDS"] = [normalize_text(k) for k in config.get("TEAM_KEYWORDS", [])]
    config["PREFERRED_PATHS"] = [normalize_text(p) for p in config.get("PREFERRED_PATHS", [])]
    config["EXCLUDE_KEYWORDS"] = [normalize_text(e) for e in config.get("EXCLUDE_KEYWORDS", [])]
    config["COOKIE_BUTTON_KEYWORDS"] = [normalize_text(c) for c in config.get("COOKIE_BUTTON_KEYWORDS", [])]
    config["KEYWORD_WEIGHTS"] = {normalize_text(k): v for k, v in config.get("KEYWORD_WEIGHTS", {}).items()}
    return config

def dismiss_cookies(page, max_attempts=3, delay=2):
    MAX_ELEMENTS_TO_TRY = 30
    BLOCKED_THRESHOLD = 20
    dismissed = False

    def boxes_overlap(box1, box2):
        return not (
            box1["x"] + box1["width"] <= box2["x"] or
            box2["x"] + box2["width"] <= box1["x"] or
            box1["y"] + box1["height"] <= box2["y"] or
            box2["y"] + box2["height"] <= box1["y"]
        )

    def try_dismiss_overlay(frame):
        CLOSE_SELECTORS = [
            ".cc-close", ".ui-dialog-titlebar-close", ".modal-close", ".popup-close",
            ".close-button", "[aria-label='Close']", "[class*='close']"
        ]
        try:
            for selector in CLOSE_SELECTORS:
                for el in frame.query_selector_all(selector):
                    try:
                        if el.is_visible():
                            el.scroll_into_view_if_needed()
                            el.click(timeout=2000)
                            frame.page.wait_for_timeout(300)
                            return True
                    except:
                        continue
        except:
            pass
        return False

    def remove_overlays_via_js(page):
        js = """
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
        """
        try:
            page.evaluate(js)
        except:
            pass

    def try_dismiss_in_frame(frame):
        """
        Try dismissing cookie popup via accept buttons or close buttons.
        """
        if try_dismiss_overlay(frame):
            return True

        try:
            elements = frame.query_selector_all("button, a, div, [role='button']")
        except:
            return False

        blocked_count = 0

        for i, el in enumerate(elements):
            if i >= MAX_ELEMENTS_TO_TRY:
                break
            try:
                if not el.is_visible():
                    continue
                text = (el.inner_text() or "").strip().lower()
                if not text:
                    continue
                lang = detect_language(text)
                config = get_team_config(lang)
                keywords = config.get("COOKIE_BUTTON_KEYWORDS", [])
                match = any(k in text for k in keywords)
                fuzzy = max((fuzz.ratio(text, k) for k in keywords), default=0) >= 85
                box = el.bounding_box()
                if box:
                    overlay = frame.query_selector("div[class*='overlay'], div[class*='backdrop']")
                    if overlay:
                        overlay_box = overlay.bounding_box()
                        if overlay_box and boxes_overlap(box, overlay_box):
                            blocked_count += 1
                            if blocked_count >= BLOCKED_THRESHOLD:
                                break
                            continue
                if match or fuzzy:
                    el.scroll_into_view_if_needed()
                    el.click(timeout=3000)
                    frame.page.wait_for_timeout(500)
                    return True
            except:
                continue
        return False

    for _ in range(max_attempts):
        try:
            if try_dismiss_in_frame(page.main_frame):
                dismissed = True
                break
            for frame in page.frames:
                if frame == page.main_frame:
                    continue
                if try_dismiss_in_frame(frame):
                    dismissed = True
                    break
        except:
            pass
        if dismissed:
            break
        page.wait_for_timeout(delay * 1000)

    if dismissed:
        print("🍪 Cookie popup dismissed")
    else:
        remove_overlays_via_js(page)
        print("⚠️ No cookie popup dismissed")

def retry(func, retries=3, delay=2, backoff=2, exceptions=(Exception,), *args, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            if i < retries - 1:
                time.sleep(delay)
                delay *= backoff
            else:
                raise e

def is_page_visually_nonempty(page):
    try:
        if page.url == "about:blank":
            return False
        body_text = retry(lambda: page.evaluate("() => document.body.innerText || ''"))
        return len(body_text.strip()) >= 300
    except:
        return False
    
def scroll_to_bottom(page, pause_time=1500, max_scrolls=20):
    last_height = 0
    for _ in range(max_scrolls):
        retry(lambda: page.evaluate("window.scrollTo(0, document.body.scrollHeight)"))
        page.wait_for_timeout(pause_time)
        new_height = retry(lambda: page.evaluate("document.body.scrollHeight"))
        if new_height == last_height:
            break
        last_height = new_height

def click_all_load_more_buttons(page, max_attempts=10):
    for _ in range(max_attempts):
        button = None
        for selector in BUTTON_SELECTORS:
            btn = page.query_selector(selector)
            if btn and btn.is_visible():
                button = btn
                break
        if not button:
            break
        try:
            retry(lambda: page.evaluate("el => el.scrollIntoView()", button))
        except:
            continue
        page.wait_for_timeout(1000)
        retry(lambda: page.evaluate("el => el.click()", button))
        page.wait_for_load_state("networkidle")
        for _ in range(20):
            retry(lambda: page.evaluate("window.scrollTo(0, 0)"))
            page.wait_for_timeout(300)
            retry(lambda: page.evaluate("window.scrollTo(0, document.body.scrollHeight)"))
            page.wait_for_timeout(500)
        page.wait_for_timeout(2000)

def clean_main_content(html):
    soup = BeautifulSoup(html, "html.parser")
    # Try preferred main content containers first
    main_content = (
        soup.select_one("div#main") or
        soup.select_one("main") or
        soup.select_one("div#content") or
        soup.select_one("div.site-main") or
        soup.select_one("div.page-content") or
        soup.select_one("div#primary") or
        soup.body  # fallback
    )
    if not main_content:
        return ""
    # Safely remove only layout/technical tags
    for tag in main_content.find_all(["script", "style"]):
        tag.decompose()
    # Carefully remove headers/footers/nav if shallow or generic
    for tag in main_content.find_all(["header", "footer", "nav", "aside"]):
        if len(tag.get_text(strip=True)) < 50:  # skip if content is trivial
            tag.decompose()
    # Remove cookie-related divs or sections by class or id
    for tag in main_content.find_all(True):
        try:
            classes = tag.get("class") or []
            class_list = " ".join(classes).lower() if isinstance(classes, list) else ""
            id_attr = tag.get("id") or ""
            if any(kw in class_list or kw in id_attr.lower() for kw in ["cookie", "consent", "gdpr"]):
                tag.decompose()
        except Exception:
            continue
    return main_content.get_text(" ", strip=True)

def asymmetric_similarity(text1, text2):
    matcher = SequenceMatcher(None, text1, text2)
    match_ratio = sum(block.size for block in matcher.get_matching_blocks())
    return match_ratio / len(text1) if len(text1) > 0 else 0

def extract_team_profiles(team_url, page, base_domain, exclude_keywords=None, max_profiles=50):
    team_path = urlparse(team_url).path.rstrip("/") + "/"
    profile_links = set()
    visited = set()
    
    page.wait_for_timeout(2000)
    links = page.query_selector_all("a.elementor-post__thumbnail__link")
    if not links:
        links = page.query_selector_all("a")
    for link in links:
        try:
            href = link.get_attribute("href")
            if not href or href.strip().startswith("#"):
                continue  # Skip anchors and empty
            full_url = urljoin(team_url, href.split("#")[0])
            parsed = urlparse(full_url)
            # Exclude external links
            if parsed.netloc and parsed.netloc != base_domain:
                continue  
            # Skip non-http(s) schemes
            if parsed.scheme not in ("http", "https", ""):
                continue
            # Skip non-HTML resources
            if any(full_url.lower().endswith(ext) for ext in non_html_extensions):
                continue
            # Skip known trap patterns (e.g., download install)
            if any(x in parsed.path.lower() for x in ["download", "install"]) or "os=" in parsed.query:
                continue
            norm_path = parsed.path.rstrip("/").lower() + "/"
            if norm_path in visited:
                continue  # Already seen
            visited.add(norm_path)
            # Exclude the main team page itself
            if norm_path == team_path: 
                continue 
            # Must be a subpage of the team section
            if not norm_path.startswith(team_path):
                continue
            # Skip links to images, docs, videos, etc.
            if any(ext in full_url.lower() for ext in non_html_extensions):
                continue 
            # Skip paths containing excluded keywords 
            if any(fuzz.partial_ratio(bad, norm_path) >= SIMILARITY_THRESHOLD for bad in exclude_keywords):
                continue  
            profile_links.add(full_url)
            if len(profile_links) >= max_profiles:
                break
        except:
            continue
    return sorted(profile_links)

def take_screenshot_as_fallback(page, path):
    print("📸 Capturing screenshot as fallback...")
    scroll_to_bottom(page)
    height = page.evaluate("() => document.body.scrollHeight")
    page.set_viewport_size({"width": 1920, "height": height})
    page.screenshot(path=path, full_page=True)
    print(f"🖼️ Screenshot saved to {path}")

def html_to_text(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Remove only truly non-informative tags
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    # Cautiously remove layout wrappers if short/unstructured
    for tag in soup.find_all(["header", "footer", "nav", "aside"]):
        if len(tag.get_text(strip=True)) < 80:
            tag.decompose()
    # Remove cookie/consent banners based on exact class/id matches
    cookie_keywords = ["cookie-banner", "cookie-consent", "gdpr-consent", "eu-cookie", "cc-banner"]
    for tag in soup.find_all(True):
        try:
            cls = " ".join(tag.get("class", [])).lower()
            id_ = (tag.get("id") or "").lower()
            if any(kw in cls.split() or kw == id_ for kw in cookie_keywords):
                tag.decompose()
        except Exception:
            continue
    text = soup.get_text(separator='\n', strip=True)
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    cleaned = '\n'.join(chunk for chunk in chunks if chunk)
    cleaned = re.sub(r'\n\s*\n', '\n\n', cleaned)
    cleaned = re.sub(r' +', ' ', cleaned)
    return cleaned.strip()

def scrape_page_content(page, url):
    try:
        retry(lambda: page.goto(url, timeout=50000, wait_until="domcontentloaded"))
        page.wait_for_timeout(2000)
        scroll_to_bottom(page)
        click_all_load_more_buttons(page)
        html_content = page.content()
        return html_to_text(html_content)
    except Exception:
        return None
    
def scrape_and_merge_profile_content(profile_urls, page, output_path, team_page_url, team_content):
    all_content = []
    all_content.append(f"={'='*78}\nMAIN TEAM PAGE: {team_page_url}\n={'='*78}\n{team_content}\n")
    kept_profiles = 0 # Counter for retained profiles
    for i, url in enumerate(profile_urls):
        content = scrape_page_content(page, url)
        # Filter out profiles too similar to the team page
        similarity = asymmetric_similarity(content, team_content)
        if similarity >= 0.85:
            continue
        if content.strip():
            kept_profiles += 1
            all_content.append(f"={'='*78}\nPROFILE {i+1}: {url}\n={'='*78}\n{content}\n")
    if all_content:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(all_content))
        print(f"\U0001F4DD Merged profile content saved: {output_path}")
    else:
        print("⚠️ No profile content found to merge")

def score_page_content(content, url=None, config=None):
    if config is None:
        raise ValueError("Team config must be provided.")
    team_keywords = config.get("TEAM_KEYWORDS", [])
    keyword_weights = config.get("KEYWORD_WEIGHTS", {})
    cleaned = clean_main_content(content)
    text = normalize_text(cleaned)
    score = 0
     # Keyword-based content scoring
    for kw in team_keywords:
        weight = keyword_weights.get(kw, 3)  # Default fallback weight
        pattern = rf"\b{re.escape(kw)}\b"
        score += weight * len(re.findall(pattern, text))
    # URL path-based score boosting
    if url:
        url_path = normalize_text(urlparse(url).path)
    for strong_kw in team_keywords:
            if strong_kw in url_path:
                score += 25
    strong_words = [kw for kw, w in keyword_weights.items() if w >= 20]
    if url_path.strip() in ["", "/"] and not any(kw in text for kw in strong_words):
        score -= 15
    return score if len(text) > 200 else 0

def find_best_team_page(base_url, page, max_links=50):
    def collect_links(scope_url, base_domain, config):
        seen_paths, candidates, preferred = set(), [], []
        team_keywords = config.get("TEAM_KEYWORDS", [])
        preferred_paths = config.get("PREFERRED_PATHS", [])
        exclude_keywords = config.get("EXCLUDE_KEYWORDS", [])
        try:
            retry(lambda: page.goto(scope_url, timeout=50000, wait_until="domcontentloaded"))
        except:
            return [], []
        for link in page.query_selector_all("a"):
            try:
                href = link.get_attribute("href")
                text = normalize_text(link.inner_text() or "")
                if not href or href.strip().startswith("#"):
                    continue  # Skip anchors and empty links
                # Resolve relative paths safely
                full_url = urljoin(scope_url, href.split("#")[0])
                parsed = urlparse(full_url)
                # Skip non-http(s) schemes like mailto:, tel:, javascript:
                if parsed.scheme not in ("http", "https", ""):
                    continue
                # Skip external domains
                if parsed.netloc and parsed.netloc != base_domain:
                    continue
                # Skip non-HTML file types
                if any(full_url.lower().endswith(ext) for ext in non_html_extensions):
                    continue
                norm_path = normalize_text(parsed.path.rstrip("/"))
                if norm_path in seen_paths:
                    continue # Filter out duplicates and excluded paths
                seen_paths.add(norm_path)
                if any(fuzz.partial_ratio(bad, norm_path) >= SIMILARITY_THRESHOLD for bad in exclude_keywords):
                    continue
                if any(kw in norm_path for kw in team_keywords) or any(kw in text for kw in team_keywords):
                        candidates.append(full_url)
                        path_last = norm_path.split("/")[-1]
                        if any(path_last.startswith(normalize_text(pref)) for pref in preferred_paths):
                            preferred.append(full_url)
                if len(candidates) >= max_links:
                    break
            except:
                continue
        return preferred, candidates
    
    def override_language_rules(lang, preferred, candidates):
        override_keywords = {
            "fr": ["equipe"],
            "it": ["il-nostro-team"],
            "de": ["team"]
        }
        urls_to_check = preferred + candidates
        if lang in override_keywords:
            for url in urls_to_check:
                path_last = normalize_text(urlparse(url).path.rstrip("/").split("/")[-1])
                for kw in override_keywords[lang]:
                    if path_last.startswith(normalize_text(kw)):
                        print(f"ℹ️ Override: Forcing URL due to strict override keyword match → {url}")
                        return url
        return None
    
    def score_urls_by_content(urls, config, lang):
        best_score, best_url = -1, None
        seen_norm_paths = set()
        for url in urls:
            try:
                norm_path = normalize_text(urlparse(url).path)
                if norm_path in seen_norm_paths:
                    continue
                seen_norm_paths.add(norm_path)
                retry(lambda: page.goto(url, timeout=50000, wait_until="domcontentloaded"))
                html = page.content()
                text = BeautifulSoup(html, "html.parser").get_text(separator="\n")
                if detect_language(text) != lang:
                    continue
                score = score_page_content(html, url, config)
                if score > best_score:
                    best_score = score
                    best_url = url
            except:
                continue
        return best_url
    # Step 1: Setup
    base_parsed = urlparse(base_url)
    base_domain = base_parsed.netloc
    homepage_url = f"{base_parsed.scheme}://{base_domain}/"
    # Step 2: Detect language from homepage
    try:
        page.goto(homepage_url, timeout=50000, wait_until="domcontentloaded")
        raw_text = BeautifulSoup(page.content(), "html.parser").get_text(separator="\n")
        homepage_lang = detect_language(raw_text)
    except:
        homepage_lang = "en"
    try:
        config = get_team_config(homepage_lang)
    except ValueError:
        config = get_team_config("en")
    # Phase 1: Crawl under given base_url
    preferred1, candidates1 = collect_links(base_url, base_domain, config)
    if preferred1:
        if len(preferred1) == 1:
            return preferred1[0]
        override = override_language_rules(homepage_lang, preferred1, candidates1)
        if override:
            return override
        best = score_urls_by_content(preferred1, config, homepage_lang)
        if best:
            return best
    if candidates1:
        best = score_urls_by_content(candidates1, config, homepage_lang)
        if best:
            return best
    # Phase 2: Fallback to full homepage domain
    preferred2, candidates2 = collect_links(homepage_url, base_domain, config)
    if preferred2:
        if len(preferred2) == 1:
            return preferred2[0]
        override = override_language_rules(homepage_lang, preferred2, candidates2)
        if override:
            return override
        best = score_urls_by_content(preferred2, config, homepage_lang)
        if best:
            return best
    if candidates2:
        best = score_urls_by_content(candidates2, config, homepage_lang)
        if best:
            return best
    return base_url

def is_valid_content(text: str) -> bool:
    lower_text = text.strip().lower()
    if not lower_text:
        return False

    # General error patterns
    error_patterns = [
        "error scraping content",
        "no content saving",
        "no content",
        "<!doctype html>",
        "404 not found",
        "page not found",
        "document not found",
        "the resource requested could not be found",
        "diese seite wurde nicht gefunden",
        "seite nicht gefunden",
        "page introuvable"
    ]

    # Bot protection / interstitials
    bot_block_patterns = [
        "verifying you are human",
        "enable javascript and cookies",
        "cloudflare",
        "waiting for .* to respond",
        "performance & security by cloudflare",
        "ray id"
    ]

    combined_patterns = error_patterns + bot_block_patterns

    # Pattern-based exclusion
    for pattern in combined_patterns:
        if re.search(pattern, lower_text):
            return False

    # Minimum length cutoff
    if len(lower_text) < 200:
        return False

    return True

def scrape_team_content(website, practice_name, output_dir, max_attempts=2):
    # 🔒 Ensure output directory exists before anything else
    os.makedirs(output_dir, exist_ok=True)

    txt_path = os.path.join(output_dir, sanitize_filename(practice_name) + ".txt")
    png_path = os.path.join(output_dir, sanitize_filename(practice_name) + ".png")
    # ✅ Skip if already processed
    if os.path.exists(txt_path) or os.path.exists(png_path):
        print(f"⏩ Skipping {practice_name} — output already exists.")
        return
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"\n🌍 Visiting: {website} (Attempt {attempt}/{max_attempts})")

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(viewport={"width": 1920, "height": 1080})

                retry(lambda: page.goto(website, timeout=50000))
                page.wait_for_load_state("load")
                dismiss_cookies(page)

                team_page = find_best_team_page(website, page)
                print(f"🔗 Best team page: {team_page}")

                retry(lambda: page.goto(team_page, timeout=50000))
                page.wait_for_load_state("load")

                raw_text = BeautifulSoup(page.content(), "html.parser").get_text(separator="\n")
                homepage_lang = detect_language(raw_text)
                try:
                    config = get_team_config(homepage_lang)
                except ValueError:
                    config = get_team_config("en")

                exclude_keywords = config.get("EXCLUDE_KEYWORDS", [])

                team_content = scrape_page_content(page, team_page)
                profiles = extract_team_profiles(team_page, page, urlparse(website).netloc, exclude_keywords)

                if profiles:
                    print(f"🔍 Found {len(profiles)} team profile URLs")
                    scrape_and_merge_profile_content(profiles, page, txt_path, team_page, team_content)
                else:
                    if team_content and team_content.strip():
                        content_text = f"TEAM PAGE: {team_page}\n{'='*80}\n{team_content}"

                        if not is_valid_content(content_text):
                            return  # Skip short, invalid or junk content                     
                        else:
                            with open(txt_path, 'w', encoding='utf-8') as f:
                                f.write(content_text)
                    elif is_page_visually_nonempty(page):
                        take_screenshot_as_fallback(page, png_path)
                    else:
                        return
                browser.close()
                return  # Success: exit the retry loop

        except PlaywrightTimeout:
            print(f"⏳ Timeout on {practice_name} (Attempt {attempt}) — retrying...")
        except Exception:
            pass

        time.sleep(2)  # Wait briefly before retrying

    print(f"🛑 Failed all {max_attempts} attempts for: {practice_name}")

def run_scraping_task(website, name, output_dir):
    try:
        scrape_team_content(website, name, output_dir)
    except Exception:
        pass
def process_single_site(args, timeout=300):  # timeout in seconds (5 minutes)
    website, name, output_dir = args
    proc = Process(target=run_scraping_task, args=(website, name, output_dir))
    proc.start()
    proc.join(timeout)
    if proc.is_alive():
        print(f"⏱️ Timeout: Killing process for {name} after {timeout}s")
        proc.terminate()
        proc.join()

def run_batch(csv_path, output_dir, max_workers=4, chunksize=100, timeout=300):
    batch_start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)
    # ✅ Check required columns before reading in chunks
    try:
        header = pd.read_csv(csv_path, nrows=0)
        required_columns = {"Name", "Website"}
        missing = required_columns - set(header.columns)
        if missing:
            raise ValueError(f"❌ Input CSV is missing required column(s): {', '.join(missing)}")
    except Exception as e:
        print(f"❌ Error reading CSV header: {e}")
        return
    
    total_processed = 0
    chunk_index = 0
    print(f"🚀 Starting batch processing")

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk_index += 1
            tasks = [
                (str(row["Website"]).strip(), str(row["Name"]).strip(), output_dir)
                for _, row in chunk.iterrows()
                if pd.notna(row["Website"]) and str(row["Website"]).strip() != ""
            ]
            if not tasks:
                continue
            print(f"\n📦 Processing chunk {chunk_index} — {len(tasks)} valid sites")
            # Submit and run each with timeout protection
            futures = [executor.submit(process_single_site, task, timeout) for task in tasks]
            concurrent.futures.wait(futures)
            total_processed += len(tasks)

    elapsed = time.time() - batch_start_time
    print(f"\n✅ Finished {total_processed} sites in {elapsed:.2f}s → Avg: {elapsed / max(total_processed, 1):.2f}s/site (parallel)")

if __name__ == "__main__":
    run_batch(INPUT_FILE, OUTPUT_FOLDER, max_workers=4, chunksize=100)

    def count_output_files(folder_path):
        txt_count = 0
        png_count = 0

        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".txt"):
                txt_count += 1
            elif filename.lower().endswith(".png"):
                png_count += 1

        print(f"📄 Text files (.txt): {txt_count}")
        print(f"🖼️ Image files (.png): {png_count}")
        print(f"📁 Total files: {txt_count + png_count}")

    def move_large_txt_files(source_dir, target_dir, threshold=100_000):
        os.makedirs(target_dir, exist_ok=True)
        moved = 0
        for filename in os.listdir(source_dir):
            if filename.lower().endswith(".txt"):
                src_path = os.path.join(source_dir, filename)
                with open(src_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                if len(content) > threshold:
                    dst_path = os.path.join(target_dir, filename)
                    os.rename(src_path, dst_path)
                    moved += 1
        print(f"✅ Moved {moved} large files to: {target_dir}")

    count_output_files(OUTPUT_FOLDER)
    move_large_txt_files(OUTPUT_FOLDER, LARGE_FOLDER)