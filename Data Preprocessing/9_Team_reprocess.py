import os
import json
import shutil
import pandas as pd

# -------------------------------
# Config
# -------------------------------
BASE_DIR = "C:/Users/myuan/Desktop"
COUNTRY_DIR = "CHE"
CACHE_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "team_cache")
INPUT_CSV = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_team.csv")
TEXT_DIR = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_image")
FAILED_TEXT_DIR = os.path.join(BASE_DIR, COUNTRY_DIR, "Failed_text")
os.makedirs(FAILED_TEXT_DIR, exist_ok=True)

def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").rstrip()

def parse_cached_team(json_path, practice_name):
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            content = f.read()

        if not content.strip().startswith("["):
            start = content.find("[")
            end = content.rfind("]") + 1
            if start != -1 and end > start:
                content = content[start:end]
            else:
                print(f"⚠️ Unrecoverable JSON for {practice_name}")
                return None

        people = json.loads(content)
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
        print(f"❌ Failed to parse {practice_name}: {e}")
        return None

def reprocess_na_rows_and_store_failed():
    df = pd.read_csv(INPUT_CSV)
    for col in ["FD", "MD", "FND", "MND"]:
        if col not in df.columns:
            df[col] = pd.NA
    df["__sanitized__"] = df["Name"].apply(sanitize_filename)

    # Step 1: Filter rows with all NA
    na_rows = df[df[["FD", "MD", "FND", "MND"]].isna().all(axis=1)]

    updated = 0
    failed_names = []

    for _, row in na_rows.iterrows():
        practice_name = row["Name"]
        sanitized = sanitize_filename(practice_name)
        json_file = os.path.join(CACHE_PATH, f"{sanitized}.json")

        if not os.path.exists(json_file):
            print(f"⛔ Missing cache for: {practice_name}")
            failed_names.append(practice_name)
            continue

        result = parse_cached_team(json_file, practice_name)
        if result:
            fd, md, fnd, mnd = result
            if fd + md + fnd + mnd > 0:
                idx = row.name
                df.loc[idx, ["FD", "MD", "FND", "MND"]] = [fd, md, fnd, mnd]
                print(f"✅ Updated: {practice_name} → FD: {fd}, MD: {md}, FND: {fnd}, MND: {mnd}")
                updated += 1
            else:
                print(f"⚠️ Skipped (still 0/0/0/0): {practice_name}")
                failed_names.append(practice_name)
        else:
            print(f"⚠️ Failed to parse JSON: {practice_name}")
            failed_names.append(practice_name)

    # Step 2: Copy corresponding .txt files for all failed practices
    copied = 0
    for name in failed_names:
        base_name = sanitize_filename(name)
        txt_path = os.path.join(TEXT_DIR, f"{base_name}.txt")
        if os.path.exists(txt_path):
            shutil.copy(txt_path, os.path.join(FAILED_TEXT_DIR, f"{base_name}.txt"))
            copied += 1
        else:
            print(f"⚠️ No .txt file found for failed practice: {name}")

    # Finalize
    df.drop(columns="__sanitized__", inplace=True)
    # Convert count columns to nullable integer type
    for col in ["FD", "MD", "FND", "MND"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")
    df.to_csv(INPUT_CSV, index=False)
    total_na = len(na_rows)
    failed = len(failed_names)
    success = updated
    copied_txts = copied

    print(f"\n✅ Reprocessing complete.")
    print(f"🔢 Total NA rows attempted: {total_na}")
    print(f"✅ Successfully reprocessed (non-zero results): {success}")
    print(f"⚠️ Still failed (0/0/0/0, parse error, or missing JSON): {failed}")
    print(f"📁 Failed .txt files copied: {copied_txts}")
    print(f"💾 Input CSV updated in place: {INPUT_CSV}")

if __name__ == "__main__":
    reprocess_na_rows_and_store_failed()
