import os
import pandas as pd
import numpy as np
import unicodedata
import re
from rapidfuzz import fuzz
from rapidfuzz.process import extractOne
from langid import classify

# === Configuration ===
BASE_DIR = "C:/Users/myuan/Desktop"
KEYWORD_DIR = "C:/Users/myuan/Desktop/VetMap/Keyword"
COUNTRY_DIR = "CHE"
TEXT_DIR = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_image")
ANIMAL_KW_PATH = os.path.join(KEYWORD_DIR, "animal_keywords.csv")
CLINIC_KW_PATH = os.path.join(KEYWORD_DIR, "vet_keywords.csv")
NON_CLINIC_KW_PATH = os.path.join(KEYWORD_DIR, "nonclinic_keywords.csv")
INPUT_CSV = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_filtered.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_filtered_team.csv")

# === Helper Functions ===
def normalize_text(text):
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    text = re.sub(r"[\u00A0\u200B]+", " ", text)
    text = text.replace("-", " ")
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def has_loose_match(text, keyword_list, threshold=85):
    return any(extractOne(text, keyword_list, scorer=fuzz.partial_ratio, score_cutoff=threshold))

def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").rstrip()

def load_multilingual_keywords(csv_path):
    df = pd.read_csv(csv_path)
    df['Language'] = df['Language'].str.strip().str.lower()
    df['Keyword'] = df['Keyword'].dropna().apply(normalize_text)
    out = {}
    for lang in df['Language'].unique():
        out[lang] = set(df[df['Language'] == lang]['Keyword'])
    return out

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

# === Match logic per row ===
def has_fuzzy_match(text, keywords, threshold=85):
        return any(fuzz.partial_ratio(k, text) >= threshold for k in keywords)

def classify_clinic_status(text, clinic_kw_by_lang, non_clinic_kw_by_lang, lang_code):
    # Collect clinic and non-clinic keywords for detected language + English fallback
    clinic_kw = set(clinic_kw_by_lang.get(lang_code, set())) | set(clinic_kw_by_lang.get("en", set()))
    non_clinic_kw = set(non_clinic_kw_by_lang.get(lang_code, set())) | set(non_clinic_kw_by_lang.get("en", set()))

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
    
def match_species_from_text(norm_text, animal_kw_all, lang):
    keywords = animal_kw_all.get(lang, {})
    spec = []
    if "small animals" in keywords and has_fuzzy_match(norm_text, keywords["small animals"]):
        spec.append("Small Animals")
    if "large animals" in keywords and has_fuzzy_match(norm_text, keywords["large animals"]):
        spec.append("Large Animals")
    if "horses" in keywords and has_fuzzy_match(norm_text, keywords["horses"]):
        spec.append("Horses")
    return ", ".join(spec) if spec else np.nan

# === Main Execution ===
def classify_all(input_csv, text_dir, animal_kw_path, clinic_kw_path, non_clinic_kw_path, output_csv):
    df = pd.read_csv(input_csv)
    df.replace(to_replace=[None, '', 'nan', 'null', 'none'], value=np.nan, inplace=True)
    df['Clinic'] = df['Clinic'].fillna("").astype(str).str.strip().str.lower()
    before_count_S = df["Specialization"].notna().sum()
    before_count_C = df["Clinic"].notna().sum()
    print(f"🔎BEFORE processing: Clinic entries = {before_count_C}, Specialization entries = {before_count_S}")

    animal_kw_all = load_animal_keywords(animal_kw_path)
    clinic_kw_by_lang = load_multilingual_keywords(clinic_kw_path)
    non_clinic_kw_by_lang = load_multilingual_keywords(non_clinic_kw_path)

    def process_row(row):
        name = str(row['Name']).strip()
        filename = sanitize_filename(name).lower() + ".txt"
        filepath = os.path.join(text_dir, filename)
        if not os.path.isfile(filepath):
            return pd.Series([row.get("Specialization", np.nan), row.get("Clinic", "uncertain")])

        with open(filepath, encoding='utf-8') as f:
            text = f.read()
        if not text.strip():
            return pd.Series([row.get("Specialization", np.nan), row.get("Clinic", "uncertain")])

        norm_text = normalize_text(text)
        lang_code, _ = classify(norm_text)

        # Specialization classification
        spec = row.get("Specialization", "")
        if pd.isna(spec) or str(spec).strip().lower() in {"", "nan", "null", "none"}:
            spec = match_species_from_text(norm_text, animal_kw_all, lang_code)

        # Clinic classification
        clinic_val = row.get("Clinic", "")
        if clinic_val not in {"yes", "no", "uncertain"}:
            clinic_val = classify_clinic_status(norm_text, clinic_kw_by_lang, non_clinic_kw_by_lang, lang_code)

        return pd.Series([spec, clinic_val])

    df[["Specialization", "Clinic"]] = df.apply(process_row, axis=1)

    after_count_S = df["Specialization"].notna().sum()
    after_count_C = df["Clinic"].notna().sum()
    print(f"🔎AFTER processing: Clinic entries = {after_count_C}, Specialization entries = {after_count_S}")
    
    def set_flag(val, key):
        if pd.isna(val) or str(val).strip() == "":
            return np.nan
        return key.lower() in str(val).lower()

    df["Small Animals"] = df["Specialization"].apply(lambda x: set_flag(x, "Small Animals"))
    df["Large Animals"] = df["Specialization"].apply(lambda x: set_flag(x, "Large Animals"))
    df["Horses"] = df["Specialization"].apply(lambda x: set_flag(x, "Horses"))

    df.to_csv(output_csv, index=False)
    print(f"✅ Done. Output saved to: {output_csv}")

if __name__ == "__main__":
    classify_all(
        input_csv=INPUT_CSV,
        text_dir=TEXT_DIR,
        animal_kw_path=ANIMAL_KW_PATH,
        clinic_kw_path=CLINIC_KW_PATH,
        non_clinic_kw_path=NON_CLINIC_KW_PATH,
        output_csv=OUTPUT_CSV
    )
