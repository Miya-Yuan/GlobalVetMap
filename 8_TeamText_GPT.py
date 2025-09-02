import os
import re
import json
import time
import tiktoken
import pandas as pd
import unicodedata
from dotenv import load_dotenv
from rapidfuzz import process, fuzz
from openai import OpenAI, APIStatusError, APITimeoutError, RateLimitError, APIConnectionError

# -------------------------------
# Config
# -------------------------------
BASE_DIR = "C:/Users/myuan/Desktop"
COUNTRY_DIR = "CHE"
TEXT_DIR = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_image")
INPUT_CSV_PATH  = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_filtered_team.csv")
CACHE_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "team_cache")
OUTPUT_CSV_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_team.csv")
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds
BATCH_SIZE = 20
os.makedirs(CACHE_PATH, exist_ok=True)

def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").rstrip()
# -------------------------------
# Load API key and Count Token use for each call
# -------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()

def count_tokens_for_chat(messages, model="gpt-4o"):
    """Counts total tokens in a list of OpenAI chat messages."""
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = 0
    for msg in messages:
        num_tokens += 4  # every message metadata structure
        for key, value in msg.items():
            num_tokens += len(encoding.encode(value))
    num_tokens += 2  # priming tokens
    return num_tokens

# -------------------------------
# Prompt
# -------------------------------
EXTRACTION_PROMPT = """
You are an information extraction assistant specialized in veterinary team pages. Given plain text from a clinic’s team page, extract a structured list of all named individuals with the following fields:

- Name (as written)
- Gender: Female or Male (inferred from first name and context)
- Role: "Doctor" or "Non-Doctor"
- Uncertain: true or false (true if gender or role was unclear and best judgment was applied)

Rules:
1. Include all explicitly named individuals (veterinarians, assistants, interns, students, receptionists, admin, finance, support).
2. Do not include unnamed people or hallucinate names.
3. Use typical gender association + any context; if unclear, infer best guess but set Uncertain: true.
4. Roles:
   - Doctor: veterinarians, specialists, interns/residents, emergency vets, behaviorists, physiotherapists, or clinical staff
   - Non-Doctor: assistants, apprentices, technicians, students, admin, reception, finance, or non-clinical staff
5. Do not duplicate individuals.
6. Respond with a raw JSON list only — no commentary or explanation.
"""

# -------------------------------
# OpenAI call with retry
# -------------------------------
def query_openai_with_retry(prompt, content, practice_name=None, max_retries=3):
     for attempt in range(1, max_retries + 1):
        try:
            messages = [
                {"role": "system", "content": prompt},
                {"role": "user", "content": content}
            ]

            token_count = count_tokens_for_chat(messages, model="gpt-4o")
            if practice_name:
                print(f"ℹ️ Token count for {practice_name}: {token_count}")
            else:
                print(f"ℹ️ Token count: {token_count}")

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=0
            )
            return response.choices[0].message.content

        except (APIStatusError, APITimeoutError, RateLimitError, APIConnectionError) as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"API failed after {max_retries} attempts: {e}")
            wait_time = RETRY_BACKOFF * attempt
            print(f"[Retry {attempt}] API error: {e}. Waiting {wait_time}s...")
            time.sleep(wait_time)

def parse_individuals(json_text, practice_name):
    if not json_text or "[" not in json_text or "]" not in json_text:
        print(f"⚠️ Skipped: {practice_name} — No JSON structure detected")
        # Flag failed JSON
        failed_path = os.path.join(CACHE_PATH, f"Failed_{practice_name}.json")
        with open(failed_path, "w", encoding="utf-8") as f_fail:
            f_fail.write(json_text or "")
        return None, None, None, None
    # Attempt to recover if model included extra text
    if not json_text.strip().startswith("["):
        start = json_text.find("[")
        end = json_text.rfind("]") + 1
        if start != -1 and end != -1 and end > start:
            print(f"⚠️ Recovered list from partial response for {practice_name}")
            json_text = json_text[start:end]
        else:
            print(f"⚠️ Skipped: {practice_name} — Could not recover JSON list")
            # Flag unrecoverable JSON
            failed_path = os.path.join(CACHE_PATH, f"Failed_{practice_name}.json")
            with open(failed_path, "w", encoding="utf-8") as f_fail:
                f_fail.write(json_text or "")
            return None, None, None, None

    try:
        people = json.loads(json_text)
        fd = md = fnd = mnd = 0

        for person in people:
            role = person.get("Role", "").strip().lower()
            gender = person.get("Gender", "").strip().lower()
            if role == "doctor":
                if gender == "female":
                    fd += 1
                elif gender == "male":
                    md += 1
            elif role == "non-doctor":
                if gender == "female":
                    fnd += 1
                elif gender == "male":
                    mnd += 1

        return fd, md, fnd, mnd

    except Exception as e:
        print(f"⚠️ Failed to parse JSON for {practice_name}: {e}")
        # Flag failed or unrecoverable JSON
        failed_path = os.path.join(CACHE_PATH, f"Failed_{practice_name}.json")
        with open(failed_path, "w", encoding="utf-8") as f_fail:
            f_fail.write(json_text or "")
        return None, None, None, None

# -------------------------------
# Load and prepare input CSV
# -------------------------------
if os.path.exists(INPUT_CSV_PATH):
    df_input = pd.read_csv(INPUT_CSV_PATH)
    for col in ["FD", "MD", "FND", "MND"]:
        if col not in df_input.columns:
            df_input[col] = pd.NA
    df_output = df_input.copy()
    df_output["__sanitized__"] = df_output["Name"].apply(sanitize_filename)
else:
    raise FileNotFoundError(f"Input file not found: {INPUT_CSV_PATH}")

# -------------------------------
# Main processing loop
# -------------------------------
def process_team_files():
    new_rows = []
    all_txt_files = sorted(f for f in os.listdir(TEXT_DIR) if f.endswith(".txt"))
    total = len(all_txt_files)
    batches = [all_txt_files[i:i+BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches, 1):
        print(f"\n📦 Starting batch {batch_idx}/{len(batches)}")

        for i, filename in enumerate(batch, 1):
            practice_name = os.path.splitext(filename)[0]
            cached_path = os.path.join(CACHE_PATH, f"{practice_name}.json")
            sanitized = sanitize_filename(practice_name)

            if os.path.exists(cached_path):
                print(f"⚠️ Skipping {practice_name} — cached.")
                continue

            # Skip if already filled
            match_idx = df_output[df_output["__sanitized__"] == sanitized].index
            if not match_idx.empty:
                row = df_output.loc[match_idx[0]]
                if row[["FD", "MD", "FND", "MND"]].notna().all():
                    print(f"⚠️ Skipping {practice_name} — already processed.")
                    continue

            print(f"🔍 [{i}/{len(batch)}] Processing: {practice_name}")
            with open(os.path.join(TEXT_DIR, filename), "r", encoding="utf-8", errors="replace") as f:
                text = f.read()

            try:
                reply = query_openai_with_retry(EXTRACTION_PROMPT, text, practice_name)
                with open(cached_path, "w", encoding="utf-8") as f_out:
                    f_out.write(reply)
                fd, md, fnd, mnd = parse_individuals(reply, practice_name)
                if None not in (fd, md, fnd, mnd):
                    new_rows.append({"Name": practice_name, "FD": fd, "MD": md, "FND": fnd, "MND": mnd})
                    print(f"✅ {practice_name} → FD: {fd}, MD: {md}, FND: {fnd}, MND: {mnd}")
                else:
                    print(f"❌ Skipped: {practice_name} (invalid data)")
            except Exception as e:
                print(f"❌ Failed: {practice_name}: {e}")
                with open(os.path.join(CACHE_PATH, "failures.log"), "a", encoding="utf-8") as flog:
                    flog.write(f"{practice_name} — {str(e)}\n")

        # Merge after batch
        if new_rows:
            df_new = pd.DataFrame(new_rows)
            df_new["__sanitized__"] = df_new["Name"].apply(sanitize_filename)

            for _, row_new in df_new.iterrows():
                name_sanitized = row_new["__sanitized__"]
                match_idx = df_output[df_output["__sanitized__"] == name_sanitized].index

                if not match_idx.empty:
                    idx = match_idx[0]
                    df_output.loc[idx, ["FD", "MD", "FND", "MND"]] = row_new[["FD", "MD", "FND", "MND"]].values
                else:
                    match, score, _ = process.extractOne(
                        name_sanitized,
                        df_output["__sanitized__"].tolist(),
                        scorer=fuzz.token_sort_ratio
                    )
                    if score >= 90:
                        idx = df_output[df_output["__sanitized__"] == match].index[0]
                        df_output.loc[idx, ["FD", "MD", "FND", "MND"]] = row_new[["FD", "MD", "FND", "MND"]].values
                    else:
                        print(f"⚠️ No reliable match for: {row_new['Name']} (score: {score})")

            df_output.drop(columns="__sanitized__", inplace=True)
            zero_mask = (df_output[["FD", "MD", "FND", "MND"]] == 0).all(axis=1)
            df_output.loc[zero_mask, ["FD", "MD", "FND", "MND"]] = pd.NA
            df_output.to_csv(OUTPUT_CSV_PATH, index=False)
            print(f"💾 Batch saved to: {OUTPUT_CSV_PATH}")
            df_output["__sanitized__"] = df_output["Name"].apply(sanitize_filename)
            new_rows.clear()
        else:
            print("⚠️ No valid updates in this batch.")

if __name__ == "__main__":
    process_team_files()